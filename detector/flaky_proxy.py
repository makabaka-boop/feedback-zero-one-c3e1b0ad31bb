# -*- coding: utf-8 -*-
"""
丢应答代理（仅验收用）：监听本地端口，把请求转发给上游仪器源；
按 --rate 概率直接丢弃应答（关闭连接、不返回任何内容），
其余请求正常透传。源端所有操作均幂等，worker 重试即可收敛。
"""
import argparse
import http.client
import random
import threading
from http.server import BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from http.server import HTTPServer


class ProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"  # 丢弃时直接关连接，避免 keep-alive 挂起

    def log_message(self, fmt, *args):
        pass

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else b""
        if random.random() < self.server.drop_rate:
            sys_stderr("proxy DROP %s" % self.path)
            self.close_connection = True
            self.wfile.flush()
            return  # 不回任何应答：模拟应答丢失
        try:
            conn = http.client.HTTPConnection(
                self.server.uphost, self.server.upport, timeout=10)
            conn.request("POST", self.path, body=body,
                         headers={"Content-Type": "application/json"})
            resp = conn.getresponse()
            data = resp.read()
            self.send_response(resp.status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            conn.close()
        except Exception as e:
            sys_stderr("proxy upstream error: %r" % e)
            self.close_connection = True


def sys_stderr(msg):
    import sys
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()


class ProxyServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--upstream-host", default="127.0.0.1")
    ap.add_argument("--upstream-port", type=int, required=True)
    ap.add_argument("--rate", type=float, default=0.5)
    args = ap.parse_args()

    httpd = ProxyServer((args.host, args.port), ProxyHandler)
    httpd.uphost = args.upstream_host
    httpd.upport = args.upstream_port
    httpd.drop_rate = args.rate
    print("proxy listening on %s:%d -> %s:%d drop=%.2f"
          % (args.host, args.port, args.upstream_host, args.upstream_port,
             args.rate), flush=True)
    httpd.serve_forever()


if __name__ == "__main__":
    main()
