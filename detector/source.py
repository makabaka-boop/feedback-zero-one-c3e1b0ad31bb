# -*- coding: utf-8 -*-
"""
独立持久化 HTTP 仪器源（instrument source）。

接口（POST JSON，成功回业务结果或 null，失败回 {"error": JSONPath}）：
  open(id)                 幂等建空日志；不重开已关闭日志；未知 id 由此登记
  append(id, y)            仅对未关闭日志追加 0/1，回从 1 连续的正整数 seq
  close(id)                封口；重复回原末序号 cut；空日志 cut=0
  fetch(id, after)         回 {"rows": [[seq, y], ...], "cut": int|null}
除 open 外，未知 id 一律拒绝。
全部状态在 SQLite 中原子持久化，进程重启后状态不丢。
"""
import argparse
import threading
import sqlite3

from common import (open_db, serve, require_id, parse_int)


SCHEMA = """
CREATE TABLE IF NOT EXISTS logs(
    id      TEXT PRIMARY KEY,
    closed  INTEGER NOT NULL DEFAULT 0,
    cut     INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS samples(
    id   TEXT NOT NULL,
    seq  INTEGER NOT NULL,
    y    INTEGER NOT NULL CHECK(y IN (0, 1)),
    PRIMARY KEY (id, seq)
);
"""


class SourceStore:
    def __init__(self, db_path):
        self.lock = threading.Lock()
        self.conn = open_db(db_path)
        self.conn.executescript(SCHEMA)

    # ------------------------------------------------------------ open
    def open(self, ident):
        """幂等建空日志；已关闭则状态冲突。回 null。"""
        with self.lock, self.conn:
            row = self.conn.execute(
                "SELECT closed FROM logs WHERE id=?", (ident,)).fetchone()
            if row is None:
                self.conn.execute(
                    "INSERT INTO logs(id, closed, cut) VALUES(?, 0, 0)",
                    (ident,))
                return None
            if row[0] == 1:
                return "$.state"
            return None  # 已存在且未关闭：幂等

    # ------------------------------------------------------------ append
    def append(self, ident, y):
        if not (y == 0 or y == 1):  # 布尔已在解析阶段拒绝
            return None, "$.y"
        with self.lock, self.conn:
            row = self.conn.execute(
                "SELECT closed, cut FROM logs WHERE id=?",
                (ident,)).fetchone()
            if row is None:
                return None, "$.id"
            if row[0] == 1:
                return None, "$.state"
            seq = row[1] + 1
            self.conn.execute(
                "INSERT INTO samples(id, seq, y) VALUES(?, ?, ?)",
                (ident, seq, y))
            self.conn.execute(
                "UPDATE logs SET cut=? WHERE id=?", (seq, ident))
            return seq, None

    # ------------------------------------------------------------ close
    def close(self, ident):
        with self.lock, self.conn:
            row = self.conn.execute(
                "SELECT closed, cut FROM logs WHERE id=?",
                (ident,)).fetchone()
            if row is None:
                return None, "$.id"
            self.conn.execute(
                "UPDATE logs SET closed=1 WHERE id=?", (ident,))
            return row[1], None  # 重复关闭回原末序号 cut；空日志为 0

    # ------------------------------------------------------------ fetch
    def fetch(self, ident, after):
        with self.lock:
            row = self.conn.execute(
                "SELECT closed, cut FROM logs WHERE id=?",
                (ident,)).fetchone()
            if row is None:
                return None, "$.id"
            length = row[1]
            if after > length:  # after 上界为日志长度
                return None, "$.after"
            cur = self.conn.execute(
                "SELECT seq, y FROM samples WHERE id=? AND seq>? ORDER BY seq",
                (ident, after))
            rows = [[s, v] for s, v in cur.fetchall()]
            cut = row[1] if row[0] == 1 else None  # row=(closed, cut)
            return {"rows": rows, "cut": cut}, None


def main():
    ap = argparse.ArgumentParser(description="persistent HTTP instrument source")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--db", required=True)
    args = ap.parse_args()

    store = SourceStore(args.db)

    def dispatch(method, body):
        if not isinstance(body, dict):
            raise ValueError("$.body")
        if method == "open":
            ident = require_id(body)
            err = store.open(ident)
            return None, err
        if method == "append":
            ident = require_id(body)
            y = parse_int(body.get("y"), "$.y")  # 布尔拒绝
            return store.append(ident, y)
        if method == "close":
            ident = require_id(body)
            return store.close(ident)
        if method == "fetch":
            ident = require_id(body)
            after = parse_int(body.get("after"), "$.after")  # 布尔拒绝
            if after < 0:
                raise ValueError("$.after")
            # after 上界（日志长度）在加锁的 store.fetch 内校验
            return store.fetch(ident, after)
        return None, "$.path"

    httpd = serve(args.host, args.port, dispatch)
    print("source listening on %s:%d db=%s" % (args.host, args.port, args.db),
          flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
