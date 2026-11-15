# -*- coding: utf-8 -*-
"""
检测后端 HTTP API（独立进程，持久化于 SQLite）。

接口（POST JSON）：
  init(alpha)            可选：仅首次生效，重复须一致，冲突回 $.alpha
  start(id)              原子分配从 1 连续的 j 及 a=alpha/2^j，回 {"j","a"}；
                         同 id 回原值；标识限非空 ASCII 字母数字串
  query(id)              回 {"state","result"}；结清前 pending/null，
                         done 时回 {decision,n,e,cut,rows}
非法参数/未知 id/状态冲突回 {"error": JSONPath}，下标零基，整数禁布尔，
被拒绝的请求不改变任何状态。其余成功回 null。
"""
import argparse

from common import serve, require_id, parse_frac
from backend_store import BackendStore


def main():
    ap = argparse.ArgumentParser(description="detection backend API")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--db", required=True)
    ap.add_argument("--alpha", default=None,
                    help="约分 p/q 串，仅在库首次初始化时写入")
    args = ap.parse_args()

    store = BackendStore(args.db)
    if args.alpha is not None:
        num, den = parse_frac(args.alpha)
        if not (0 < num < den):
            raise SystemExit("alpha must satisfy 0 < alpha < 1")
        if not store.init_alpha(num, den):
            raise SystemExit("alpha conflict: database already initialized "
                             "with a different value")

    def dispatch(method, body):
        if not isinstance(body, dict):
            raise ValueError("$.body")
        if method == "init":
            num, den = parse_frac(body.get("alpha"))
            if not (0 < num < den):
                return None, "$.alpha"
            if store.init_alpha(num, den):
                return None, None
            return None, "$.alpha"
        if method == "start":
            ident = require_id(body)
            return store.start(ident)
        if method == "query":
            ident = require_id(body)
            return store.query(ident)
        return None, "$.path"

    httpd = serve(args.host, args.port, dispatch)
    print("backend listening on %s:%d db=%s" % (args.host, args.port, args.db),
          flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
