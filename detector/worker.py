# -*- coding: utf-8 -*-
"""
工作进程（worker）：独立进程，持有后端 SQLite 库与仪器源 HTTP 地址。

职责（全部断点在 SQLite 中原子持久，重启补开、补取、补停，不重复分析）：
  1. 领取 pending 任务（带租约，崩溃后租约过期可被新进程接管）；
  2. 对源 open(id)（幂等，冲突可忽略）；
  3. 从断点 archived 之后 fetch 增量读数，单事务归档 + 推进证据 E；
     E 初值 1，遇 1 乘 3/2、遇 0 乘 1/2；首次 E>=1/a 即冻结，
     固定位置 n 与 e，随后只归档不再分析；
  4. 触发即向源发 close 停采（停令到达前已采读数仍会在后续 fetch 归档）；
     外部也可先 close：封口后检查全部前缀，有触发 alarm，否则 exhausted；
  5. 封口且 archived==cut（归档完整）后一次性原子写终态 done。

通信可丢应答：所有源调用均幂等，任何失败都回轮询重试，靠断点收敛。

故障注入（仅验收用，环境变量）：
  CRASH_BEFORE_COMMIT / CRASH_AFTER_COMMIT / CRASH_BEFORE_CLOSE /
  CRASH_BEFORE_FINALIZE，命中即 os._exit(9)（SIGKILL 级强退，无清理）。
"""
import argparse
import os
import sys
import time

from common import http_post, HttpError, frac_str
from backend_store import BackendStore


def crash_point(name):
    if os.environ.get(name) == "1":
        sys.stderr.write("[worker] crash point %s hit, os._exit(9)\n" % name)
        sys.stderr.flush()
        os._exit(9)


class Worker:
    def __init__(self, db_path, source_url, poll_interval, lease_ttl=30.0):
        self.store = BackendStore(db_path)
        self.source_url = source_url.rstrip("/")
        self.poll = poll_interval
        self.lease_ttl = lease_ttl
        self.worker_id = "worker-%d" % os.getpid()
        alpha = self.store.get_alpha()
        if alpha is None:
            raise SystemExit("backend alpha not initialized")
        self.a_cache = {}  # id -> (a_num, a_den)，任务参数不可变

    def _src(self, method, payload):
        return http_post("%s/%s" % (self.source_url, method), payload)

    def _a(self, task):
        ident = task["id"]
        if ident not in self.a_cache:
            self.a_cache[ident] = (task["a_num"], task["a_den"])
        return self.a_cache[ident]

    def _fetch(self, ident):
        """从断点之后拉增量；通信失败返回 None 由主循环下轮重试。"""
        prog = self.store.get_progress(ident)
        try:
            fetched = self._src("fetch",
                                {"id": ident, "after": prog["archived"]})
            if isinstance(fetched, dict) and "error" in fetched:
                raise HttpError("fetch error: %r" % fetched)
        except HttpError as e:
            sys.stderr.write("[worker] fetch retry: %s\n" % e)
            return None
        return fetched

    # ------------------------------------------------------------ 单任务一步
    def step(self, ident):
        """
        处理一轮。返回 True 表示本轮有实质进展（新增量/封口/结清/触发），
        False 表示无可推进数据或通信失败（调用方可释放租约稍后重试）。
        """
        store = self.store
        task = store.get_task(ident)
        if task is None or task["state"] == "done":
            return False
        a_num, a_den = self._a(task)
        prog = store.get_progress(ident)

        # 1) 补开：open 幂等；已封口冲突也可继续
        try:
            r = self._src("open", {"id": ident})
            if isinstance(r, dict) and "error" in r and r["error"] != "$.state":
                raise HttpError("open error: %r" % r)
        except HttpError as e:
            sys.stderr.write("[worker] open retry: %s\n" % e)
            return False

        # 2) 补取：从断点之后拉全部增量
        fetched = self._fetch(ident)
        if fetched is None:
            return False

        rows = fetched["rows"]
        cut = fetched["cut"]
        closed = cut is not None

        # 3) 归档 + 推进证据（单事务原子）。仅在确有新增量时注入强退，
        #    避免空轮询误触发，保证崩溃点精确落在"提交/停令/终态"动作上。
        has_new = any(s > prog["archived"] for s, _ in rows)
        if has_new:
            crash_point("CRASH_BEFORE_COMMIT")
        out = store.apply_fetch(ident, rows, closed, cut, a_num, a_den)
        if has_new:
            crash_point("CRASH_AFTER_COMMIT")

        trigger = out["trigger"]
        prog = store.get_progress(ident)

        # 4) 触发即停采：本批首次触发，或重启后发现已冻结但停令未补发。
        #    close 幂等，应答丢失可重试；停令到达前的读数由随后 fetch 归档。
        if trigger is not None:
            n, e_num, e_den = trigger
            sys.stderr.write(
                "[worker] %s trigger n=%d e=%s -> close\n"
                % (ident, n, frac_str(e_num, e_den)))
        if (trigger is not None or prog["frozen"] == 1) and prog["closed"] == 0:
            if trigger is not None:
                crash_point("CRASH_BEFORE_CLOSE")
            try:
                self._src("close", {"id": ident})
            except HttpError as e:
                sys.stderr.write("[worker] close retry: %s\n" % e)
                return False
            # close 已生效：立刻再拉一次，把封口 cut 与停令前增量落入断点
            fetched2 = self._fetch(ident)
            if fetched2 is None:
                return False
            store.apply_fetch(ident, fetched2["rows"],
                              fetched2["cut"] is not None, fetched2["cut"],
                              a_num, a_den)

        prog = store.get_progress(ident)

        # 5) 外部先封口：对全部前缀做一次触发检查
        if prog["closed"] == 1 and not prog["frozen"]:
            n, e_num, e_den = store.find_trigger(ident, a_num, a_den)
            if n is not None:
                store.freeze_trigger(ident, n, e_num, e_den)

        # 6) 封口且归档完整 -> 终态原子写入
        prog = store.get_progress(ident)
        if prog["closed"] == 1 and prog["cut"] is not None \
                and prog["archived"] == prog["cut"]:
            if prog["frozen"]:
                decision = "alarm"
                n = prog["trigger_n"]
                e_num, e_den = prog["trigger_enum"], prog["trigger_eden"]
            else:
                decision = "exhausted"
                n = prog["cut"]  # 无触发：n=cut
                e_num, e_den = prog["e_num"], prog["e_den"]  # 全日志证据
            crash_point("CRASH_BEFORE_FINALIZE")
            ok, err = store.finalize(
                ident, decision, n, e_num, e_den, prog["cut"])
            if err is not None:
                raise RuntimeError("finalize conflict: %s" % err)
            sys.stderr.write(
                "[worker] %s done decision=%s n=%d e=%s cut=%d rows=%d\n"
                % (ident, decision, n, frac_str(e_num, e_den),
                   prog["cut"], prog["cut"]))
            return True

        # 未封口且本批无新增量：无可推进，交还租约等待数据/外部封口
        return has_new or closed

    # ------------------------------------------------------------ 主循环
    def run(self):
        sys.stderr.write("[worker] started %s source=%s\n"
                         % (self.worker_id, self.source_url))
        sys.stderr.flush()
        current = None
        while True:
            try:
                if current is None:
                    current = self.store.claim_pending(
                        self.worker_id, ttl=self.lease_ttl)
                    if current is None:
                        time.sleep(self.poll)
                        continue
                else:
                    # 续做前确认仍持有租约，否则重新领取
                    prog = self.store.get_progress(current)
                    task = self.store.get_task(current)
                    if task is None or task["state"] == "done" \
                            or prog["owner"] != self.worker_id:
                        current = None
                        continue
                    self.store.renew(current, self.worker_id,
                                     ttl=self.lease_ttl)

                progressed = self.step(current)
                # 无新增量且未封口：释放租约回到领取队列，
                # 避免数据后到时空轮询占着任务（也让故障注入点可命中）
                if progressed is False:
                    self.store.release(current, self.worker_id)
                    current = None
                    time.sleep(self.poll)
            except Exception:
                sys.stderr.write("[worker] step %s failed:\n" % current)
                import traceback
                traceback.print_exc()
                sys.stderr.flush()
                time.sleep(self.poll)


def main():
    ap = argparse.ArgumentParser(description="detection worker")
    ap.add_argument("--db", required=True, help="与后端同一个 SQLite 库")
    ap.add_argument("--source", required=True,
                    help="仪器源基址，如 http://127.0.0.1:9001")
    ap.add_argument("--poll", type=float, default=0.2,
                    help="无任务/失败时轮询间隔秒")
    ap.add_argument("--lease", type=float, default=30.0,
                    help="任务租约秒数；持有者强退后租约过期即可被接管")
    args = ap.parse_args()
    Worker(args.db, args.source, args.poll,
           lease_ttl=args.lease).run()


if __name__ == "__main__":
    main()
