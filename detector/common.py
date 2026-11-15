# -*- coding: utf-8 -*-
"""
共享基础模块：分数（约分正分母 p/q 串）、参数校验、HTTP 服务端与客户端。
仅依赖 Python 标准库。
"""
import json
import math
import sqlite3
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from http.server import HTTPServer


# ---------------------------------------------------------------- 分数
def gcd(a, b):
    return math.gcd(a, b)


def frac_str(num, den):
    """返回约分后的正分母 p/q 串；num 可为 0。"""
    if den <= 0:
        raise ValueError("denominator must be positive")
    if num == 0:
        return "0/1"
    g = math.gcd(abs(num), den)
    return "%d/%d" % (num // g, den // g)


def _parse_decimal(s, path):
    """解析十进制整数字符串（允许前导正负号）；非法抛 ValueError(path)。"""
    if not isinstance(s, str) or s == "":
        raise ValueError(path)
    body = s[1:] if s[0] in "+-" else s
    if not body.isdigit():
        raise ValueError(path)
    return int(s)


def parse_frac(s):
    """
    解析 p/q 串，返回约分后的 (p, q)，q 为正整数。
    分子非法抛 $.alpha，分母非法抛 $.alpha.denominator。
    """
    if not isinstance(s, str):
        raise ValueError("$.alpha")
    parts = s.split("/")
    if len(parts) != 2:
        raise ValueError("$.alpha")
    p = _parse_decimal(parts[0], "$.alpha")
    q = _parse_decimal(parts[1], "$.alpha.denominator")
    if q <= 0:
        raise ValueError("$.alpha.denominator")
    if p <= 0:
        raise ValueError("$.alpha")
    g = math.gcd(abs(p), q)
    return p // g, q // g


# ---------------------------------------------------------------- 校验
def is_valid_id(x):
    """标识：非空 ASCII 字母数字串。"""
    if not isinstance(x, str) or len(x) == 0:
        return False
    return all(("a" <= c <= "z") or ("A" <= c <= "Z") or ("0" <= c <= "9")
               for c in x)


def parse_int(x, path):
    """严格整数：布尔值一律拒绝。"""
    if isinstance(x, bool) or not isinstance(x, int):
        raise ValueError(path)
    return x


def require_id(obj):
    x = obj.get("id") if isinstance(obj, dict) else None
    if not is_valid_id(x):
        raise ValueError("$.id")
    return x


# ---------------------------------------------------------------- 持久化
def open_db(path):
    # check_same_thread=False：HTTP 多线程共用连接，由调用方加锁串行化
    conn = sqlite3.connect(path, timeout=30, isolation_level=None,
                           check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


# ---------------------------------------------------------------- HTTP 客户端
class HttpError(Exception):
    def __init__(self, message, payload=None):
        super().__init__(message)
        self.payload = payload


def http_post(url, payload, timeout=10):
    """POST JSON，返回解析后的 JSON。通信失败抛 HttpError（可安全重试）。"""
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"},
        method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:  # 语义错误体也带 JSON
        try:
            return json.loads(e.read().decode("utf-8"))
        except Exception:
            raise HttpError("http %d" % e.code)
    except (urllib.error.URLError, OSError, TimeoutError) as e:
        raise HttpError(str(e))


# ---------------------------------------------------------------- HTTP 服务端
def make_handler(dispatch):
    """dispatch(method, body) -> (可 JSON 序列化结果, 错误 path 或 None)。"""

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):
            pass  # 静默默认访问日志

        def _send(self, status, obj):
            body = json.dumps(obj).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            try:
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length) if length else b""
                body = json.loads(raw.decode("utf-8")) if raw else {}
            except Exception:
                self._send(400, {"error": "$.body"})
                return
            method = self.path.lstrip("/")
            try:
                result, err = dispatch(method, body)
            except ValueError as e:
                self._send(200, {"error": str(e)})
                return
            except Exception:
                self._send(500, {"error": "$.server"})
                raise
            if err is not None:
                self._send(200, {"error": err})
            else:
                self._send(200, result)

    return Handler


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def serve(host, port, dispatch):
    httpd = ThreadingHTTPServer((host, port), make_handler(dispatch))
    return httpd
