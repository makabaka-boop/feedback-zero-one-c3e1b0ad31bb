# -*- coding: utf-8 -*-
"""
检测后端存储（SQLite，后端 HTTP API 与工作进程共用同一库文件）。

关键表：
  kv        全局键值：alpha 仅初始化一次（分子/分母）
  jseq      单行：下一个待分配的 j（从 1 连续）
  tasks     任务：start 原子分配 j、a；状态 pending/done
  progress  工作进程断点：证据 E、下一个待分析 seq、已归档至、cut（封口后）、
            frozen 及触发固定值 (trigger_n/trigger_enum/trigger_eden)、租约
  archive   已归档读数 (id, seq, y)，INSERT OR IGNORE 保证重放不重复

线程模型：HTTP 多线程与 worker 可能同进程访问本对象，统一 RLock 串行化；
跨进程（后端进程 vs worker 进程）由 SQLite WAL + busy timeout 保证，
worker 另以租约保证同一任务不会被两个 worker 同时处理。
"""
import math
import threading
import time
import sqlite3

from common import frac_str


SCHEMA = """
CREATE TABLE IF NOT EXISTS kv(
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS jseq(
    id INTEGER PRIMARY KEY CHECK(id=1),
    next_j INTEGER NOT NULL
);
INSERT OR IGNORE INTO jseq(id, next_j) VALUES(1, 1);
CREATE TABLE IF NOT EXISTS tasks(
    id      TEXT PRIMARY KEY,
    j       INTEGER NOT NULL UNIQUE,
    a_num   INTEGER NOT NULL,
    a_den   INTEGER NOT NULL,
    state   TEXT NOT NULL CHECK(state IN ('pending','done')),
    decision TEXT,
    n       INTEGER,
    e_num   INTEGER,
    e_den   INTEGER,
    cut     INTEGER
);
CREATE TABLE IF NOT EXISTS progress(
    id           TEXT PRIMARY KEY,
    e_num        INTEGER NOT NULL,
    e_den        INTEGER NOT NULL,
    next_seq     INTEGER NOT NULL,
    archived     INTEGER NOT NULL,
    cut          INTEGER,
    closed       INTEGER NOT NULL DEFAULT 0,
    frozen       INTEGER NOT NULL DEFAULT 0,
    trigger_n    INTEGER,
    trigger_enum INTEGER,
    trigger_eden INTEGER,
    owner        TEXT,
    lease        INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS archive(
    id   TEXT NOT NULL,
    seq  INTEGER NOT NULL,
    y    INTEGER NOT NULL CHECK(y IN (0, 1)),
    PRIMARY KEY (id, seq)
);
"""


class BackendStore:
    def __init__(self, db_path):
        # check_same_thread=False：连接跨线程共用，全部访问由 self.lock 串行化
        self.conn = sqlite3.connect(db_path, timeout=30, isolation_level=None,
                                    check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        with self.lock:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=FULL")
            self.conn.execute("PRAGMA foreign_keys=ON")
            self.conn.executescript(SCHEMA)
            # 旧库平滑升级：补齐新增列
            cols = {r["name"] for r in
                    self.conn.execute("PRAGMA table_info(progress)")}
            for extra in ("frozen", "trigger_n", "trigger_enum",
                          "trigger_eden"):
                if extra not in cols:
                    default = (" INTEGER NOT NULL DEFAULT 0"
                               if extra == "frozen" else " INTEGER")
                    self.conn.execute(
                        "ALTER TABLE progress ADD COLUMN %s%s"
                        % (extra, default))

    # ------------------------------------------------------------ alpha
    def init_alpha(self, num, den):
        """alpha 仅初始化一次；重复初始化必须与首次一致，否则冲突。"""
        with self.lock, self.conn:
            row = self.conn.execute(
                "SELECT v FROM kv WHERE k='alpha_num'").fetchone()
            if row is None:
                self.conn.execute(
                    "INSERT INTO kv(k, v) VALUES('alpha_num', ?)",
                    (str(num),))
                self.conn.execute(
                    "INSERT INTO kv(k, v) VALUES('alpha_den', ?)",
                    (str(den),))
                return True
            old_num = int(row["v"])
            old_den = int(self.conn.execute(
                "SELECT v FROM kv WHERE k='alpha_den'").fetchone()["v"])
            return (old_num, old_den) == (num, den)

    def get_alpha(self):
        with self.lock:
            row = self.conn.execute(
                "SELECT v FROM kv WHERE k='alpha_num'").fetchone()
            if row is None:
                return None
            den = int(self.conn.execute(
                "SELECT v FROM kv WHERE k='alpha_den'").fetchone()["v"])
            return int(row["v"]), den

    # ------------------------------------------------------------ start
    def start(self, ident):
        """原子分配从 1 连续的 j 与 a=alpha/2^j；同 id 回原值。"""
        alpha = self.get_alpha()
        if alpha is None:
            return None, "$.alpha"
        alpha_num, alpha_den = alpha
        with self.lock, self.conn:
            row = self.conn.execute(
                "SELECT j, a_num, a_den FROM tasks WHERE id=?",
                (ident,)).fetchone()
            if row is not None:
                return ({"j": row["j"],
                         "a": frac_str(row["a_num"], row["a_den"])}, None)
            jr = self.conn.execute(
                "SELECT next_j FROM jseq WHERE id=1").fetchone()
            j = jr["next_j"]
            a_num, a_den = alpha_num, alpha_den * (2 ** j)
            self.conn.execute(
                "UPDATE jseq SET next_j=? WHERE id=1", (j + 1,))
            self.conn.execute(
                "INSERT INTO tasks(id, j, a_num, a_den, state) "
                "VALUES(?, ?, ?, ?, 'pending')",
                (ident, j, a_num, a_den))
            self.conn.execute(
                "INSERT INTO progress(id, e_num, e_den, next_seq, archived, "
                "cut, closed, frozen, owner, lease) "
                "VALUES(?, 1, 1, 1, 0, NULL, 0, 0, NULL, 0)", (ident,))
            return {"j": j, "a": frac_str(a_num, a_den)}, None

    # ------------------------------------------------------------ query
    def query(self, ident):
        with self.lock:
            task = self.conn.execute(
                "SELECT * FROM tasks WHERE id=?", (ident,)).fetchone()
            if task is None:
                return None, "$.id"
            if task["state"] != "done":
                return {"state": "pending", "result": None}, None
            rows = [[r["seq"], r["y"]] for r in self.conn.execute(
                "SELECT seq, y FROM archive WHERE id=? ORDER BY seq",
                (ident,)).fetchall()]
            result = {
                "decision": task["decision"],
                "n": task["n"],
                "e": frac_str(task["e_num"], task["e_den"]),
                "cut": task["cut"],
                "rows": rows,
            }
            return {"state": "done", "result": result}, None

    # ------------------------------------------------------------ worker 租约
    def claim_pending(self, worker_id, ttl=30.0):
        now = time.time()
        with self.lock, self.conn:
            row = self.conn.execute(
                "SELECT p.id FROM progress p JOIN tasks t ON t.id=p.id "
                "WHERE t.state='pending' AND (p.owner IS NULL OR p.lease<?) "
                "LIMIT 1", (now,)).fetchone()
            if row is None:
                return None
            ident = row["id"]
            self.conn.execute(
                "UPDATE progress SET owner=?, lease=? WHERE id=?",
                (worker_id, now + ttl, ident))
            return ident

    def renew(self, ident, worker_id, ttl=30.0):
        with self.lock:
            self.conn.execute(
                "UPDATE progress SET lease=? WHERE id=? AND owner=?",
                (time.time() + ttl, ident, worker_id))

    def release(self, ident, worker_id):
        """主动放弃租约，让其他 worker（或自己下轮）重新领取。"""
        with self.lock:
            self.conn.execute(
                "UPDATE progress SET owner=NULL, lease=0 "
                "WHERE id=? AND owner=?", (ident, worker_id))

    def get_progress(self, ident):
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM progress WHERE id=?", (ident,)).fetchone()

    def get_task(self, ident):
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM tasks WHERE id=?", (ident,)).fetchone()

    # ------------------------------------------------------------ 应用一批读数
    def apply_fetch(self, ident, rows, closed, cut, a_num, a_den):
        """
        断点续算 + 归档，单事务原子提交。
        rows: [[seq, y], ...] 按 seq 升序（可能含已归档的重叠首项，被忽略）。
        frozen 后只归档、不再更新证据 E（首次触发位置已固定）。
        阈值：E >= 1/a  <=>  e_num/e_den >= a_den/a_num
                  <=>  e_num*a_num >= e_den*a_den
        返回 dict: trigger=(n, e_num, e_den) 或 None；e；archived；frozen。
        """
        with self.lock, self.conn:
            p = self.conn.execute(
                "SELECT * FROM progress WHERE id=?", (ident,)).fetchone()
            e_num, e_den = p["e_num"], p["e_den"]
            next_seq = p["next_seq"]
            archived = p["archived"]
            frozen = bool(p["frozen"])
            trigger = None

            for seq, y in rows:
                if seq <= archived:
                    continue  # 已归档：重放跳过，不重复分析
                if seq != next_seq:
                    raise RuntimeError(
                        "seq gap: expect %d got %d" % (next_seq, seq))
                # 先归档（INSERT OR IGNORE 幂等），再更新证据
                self.conn.execute(
                    "INSERT OR IGNORE INTO archive(id, seq, y) "
                    "VALUES(?, ?, ?)", (ident, seq, y))
                if not frozen:
                    if y == 1:
                        e_num *= 3
                        e_den *= 2
                    else:
                        e_den *= 2
                    g = math.gcd(e_num, e_den)
                    e_num //= g
                    e_den //= g
                    if e_num * a_num >= e_den * a_den:
                        frozen = True
                        trigger = (seq, e_num, e_den)
                next_seq += 1
                archived = seq
            # 触发固定值只在本事务首次触发时写入；重启重放保持原值
            if trigger is not None:
                self.conn.execute(
                    "UPDATE progress SET e_num=?, e_den=?, next_seq=?, "
                    "archived=?, cut=?, closed=?, frozen=1, trigger_n=?, "
                    "trigger_enum=?, trigger_eden=? WHERE id=?",
                    (e_num, e_den, next_seq, archived,
                     cut if closed else None, 1 if closed else 0,
                     trigger[0], trigger[1], trigger[2], ident))
            else:
                self.conn.execute(
                    "UPDATE progress SET e_num=?, e_den=?, next_seq=?, "
                    "archived=?, cut=?, closed=?, frozen=? WHERE id=?",
                    (e_num, e_den, next_seq, archived,
                     cut if closed else None, 1 if closed else 0,
                     1 if frozen else 0, ident))
            return {"trigger": trigger, "e": (e_num, e_den),
                    "archived": archived, "frozen": frozen}

    def find_trigger(self, ident, a_num, a_den):
        """
        依据已归档数据找首次 E >= 1/a 的固定位置 n 及该位置 e=E。
        E 从 1 起逐前缀重算（纯读，结果与断点推进一致）。
        返回 (n 或 None, e_num, e_den)；无触发时 e 为全日志最终证据。
        """
        with self.lock:
            e_num, e_den = 1, 1
            for r in self.conn.execute(
                    "SELECT seq, y FROM archive WHERE id=? ORDER BY seq",
                    (ident,)).fetchall():
                if r["y"] == 1:
                    e_num *= 3
                    e_den *= 2
                else:
                    e_den *= 2
                # E >= 1/a  <=>  e_num/e_den >= a_den/a_num
                if e_num * a_num >= e_den * a_den:
                    g = math.gcd(e_num, e_den)
                    return r["seq"], e_num // g, e_den // g
            g = math.gcd(e_num, e_den)
            return None, e_num // g, e_den // g

    def freeze_trigger(self, ident, n, e_num, e_den):
        """外部先封口且全前缀存在触发：固定 n/e 并冻结（一次性）。"""
        with self.lock, self.conn:
            p = self.conn.execute(
                "SELECT frozen FROM progress WHERE id=?",
                (ident,)).fetchone()
            if p is None:
                return False
            if p["frozen"]:
                return True
            self.conn.execute(
                "UPDATE progress SET frozen=1, trigger_n=?, trigger_enum=?, "
                "trigger_eden=? WHERE id=?", (n, e_num, e_den, ident))
            return True

    def finalize(self, ident, decision, n, e_num, e_den, cut):
        """终态一次性原子写入；封口且归档完整（archived==cut）才可结清。"""
        with self.lock, self.conn:
            p = self.conn.execute(
                "SELECT archived, cut, closed FROM progress WHERE id=?",
                (ident,)).fetchone()
            if p is None or not p["closed"] or p["cut"] is None:
                return False, "$.state"
            if p["archived"] != p["cut"]:
                return False, "$.state"
            cur = self.conn.execute(
                "SELECT state FROM tasks WHERE id=?", (ident,)).fetchone()
            if cur is None:
                return False, "$.id"
            if cur["state"] == "done":
                return True, None  # 已 done：幂等成功
            self.conn.execute(
                "UPDATE tasks SET state='done', decision=?, n=?, e_num=?, "
                "e_den=?, cut=? WHERE id=?",
                (decision, n, e_num, e_den, cut, ident))
            self.conn.execute(
                "UPDATE progress SET owner=NULL, lease=0 WHERE id=?",
                (ident,))
            return True, None
