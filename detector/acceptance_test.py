# -*- coding: utf-8 -*-
"""
验收脚本（标准库实现，python3 -I acceptance_test.py）。

覆盖：
  A. 仪器源全部接口与非法参数矩阵（幂等、seq、cut、after 边界、布尔拒绝、状态冲突）
  B. 后端 alpha 一次初始化、start 原子连续 j、同 id 原值、query 状态机
  C. 题面端到端示例：alpha=1/2，四个 1 触发，停令前再采 0
     -> alarm, n=4, e=81/16, cut=5, 归档五行
  D. 外部先封口：alarm 与 exhausted（含空日志 n=0、e=1/1）
  E. 强退验收：提交前/提交后/停令前/终态前 os._exit(9)，重启收敛、不重复分析
  F. 丢应答代理（50% 丢应答）下最终一致
  G. 源进程与后端进程 kill 后同库重启，状态不丢
  H. 双 worker 抢同一库，任务恰好结清一次
"""
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
import urllib.error

HERE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable
PASS, FAIL = 0, 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("  [PASS] %s" % name)
    else:
        FAIL += 1
        print("  [FAIL] %s %s" % (name, detail))


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def call(url, payload, timeout=10):
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return json.loads(e.read().decode())


def wait_port(port, timeout=10):
    end = time.time() + timeout
    while time.time() < end:
        try:
            with socket.create_connection(("127.0.0.1", port), 0.5):
                return True
        except OSError:
            time.sleep(0.05)
    return False


_LOG_SEQ = 0


def start(proc_args, env=None, cwd=HERE):
    global _LOG_SEQ
    _LOG_SEQ += 1
    e = dict(os.environ)
    if env:
        e.update(env)
    log = open(os.path.join(e.get("LOGDIR", "/tmp"),
                            "proc_%d.log" % _LOG_SEQ), "w")
    p = subprocess.Popen(proc_args, cwd=cwd, env=e,
                         stdout=log, stderr=log)
    return p


def wait_done(base, ident, timeout=15):
    end = time.time() + timeout
    last = None
    while time.time() < end:
        last = call(base + "/query", {"id": ident})
        if last.get("state") == "done":
            return last
        time.sleep(0.1)
    raise AssertionError("not done in time: %r" % last)


def kill9(p):
    p.kill()
    p.wait(timeout=10)


class Cluster:
    def __init__(self, root, alpha="1/2", worker_env=None, use_proxy=False,
                 proxy_rate=0.5):
        self.root = root
        os.makedirs(root, exist_ok=True)
        self.src_port = free_port()
        self.back_port = free_port()
        self.proc_port = free_port() if use_proxy else None
        self.src_db = os.path.join(root, "source.db")
        self.back_db = os.path.join(root, "backend.db")
        self.procs = []
        self.source = start([PY, "source.py", "--port", str(self.src_port),
                             "--db", self.src_db],
                            env={"LOGDIR": root})
        self.procs.append(("source", self.source))
        wait_port(self.src_port)
        self.backend = start(
            [PY, "backend.py", "--port", str(self.back_port),
             "--db", self.back_db, "--alpha", alpha],
            env={"LOGDIR": root})
        self.procs.append(("backend", self.backend))
        wait_port(self.back_port)
        if use_proxy:
            self.proxy = start(
                [PY, "flaky_proxy.py", "--port", str(self.proc_port),
                 "--upstream-port", str(self.src_port),
                 "--rate", str(proxy_rate)],
                env={"LOGDIR": root})
            self.procs.append(("proxy", self.proxy))
            wait_port(self.proc_port)
            self.source_url = "http://127.0.0.1:%d" % self.proc_port
        else:
            self.source_url = "http://127.0.0.1:%d" % self.src_port
        self.src = "http://127.0.0.1:%d" % self.src_port
        self.back = "http://127.0.0.1:%d" % self.back_port
        env = {"LOGDIR": root}
        if worker_env:
            env.update(worker_env)
        self.worker = start(
            [PY, "worker.py", "--db", self.back_db,
             "--source", self.source_url, "--poll", "0.05",
             "--lease", "2"], env=env)
        self.procs.append(("worker", self.worker))

    def stop(self):
        for _, p in self.procs:
            if p.poll() is None:
                p.terminate()
        for _, p in self.procs:
            try:
                p.wait(timeout=5)
            except Exception:
                kill9(p)
        # 兜底：确保没有进程残留
        for _, p in self.procs:
            if p.poll() is None:
                kill9(p)

    def restart_worker(self, env=None):
        kill9(self.worker)
        e = {"LOGDIR": self.root}
        if env:
            e.update(env)
        self.worker = start(
            [PY, "worker.py", "--db", self.back_db,
             "--source", self.source_url, "--poll", "0.05",
             "--lease", "2"], env=e)
        self.procs.append(("worker", self.worker))

    def restart_source(self):
        kill9(self.source)
        self.source = start([PY, "source.py", "--port",
                             str(self.src_port), "--db", self.src_db],
                            env={"LOGDIR": self.root})
        self.procs.append(("source", self.source))
        wait_port(self.src_port)

    def restart_backend(self):
        kill9(self.backend)
        self.backend = start([PY, "backend.py", "--port",
                              str(self.back_port), "--db", self.back_db],
                             env={"LOGDIR": self.root})
        self.procs.append(("backend", self.backend))
        wait_port(self.back_port)

    def open_log(self, ident):
        """显式在仪器源登记日志（worker 的 open 仅作补开兜底）。"""
        return call(self.src + "/open", {"id": ident})


# ================================================================ A. 仪器源
def test_source(root):
    print("A. 仪器源接口与参数矩阵")
    port = free_port()
    db = os.path.join(root, "s.db")
    p = start([PY, "source.py", "--port", str(port), "--db", db],
              env={"LOGDIR": root})
    try:
        wait_port(port)
        u = "http://127.0.0.1:%d" % port
        check("未知 id append 拒绝", call(u + "/append",
              {"id": "nope", "y": 1}) == {"error": "$.id"})
        check("未知 id close 拒绝", call(u + "/close",
              {"id": "nope"}) == {"error": "$.id"})
        check("未知 id fetch 拒绝", call(u + "/fetch",
              {"id": "nope", "after": 0}) == {"error": "$.id"})
        for bad in ["", "a b", "a-b", "a.b", 123, True, None, ["x"]]:
            check("非法 id %r" % bad, call(u + "/open",
                  {"id": bad}) == {"error": "$.id"})
        check("open 成功回 null", call(u + "/open", {"id": "L1"}) is None)
        check("open 幂等回 null", call(u + "/open", {"id": "L1"}) is None)
        check("空日志 fetch", call(u + "/fetch",
              {"id": "L1", "after": 0}) == {"rows": [], "cut": None})
        check("空日志封口 cut=0", call(u + "/close", {"id": "L1"}) == 0)
        check("重复封口回原 cut=0", call(u + "/close", {"id": "L1"}) == 0)
        check("封口后不重开", call(u + "/open",
              {"id": "L1"}) == {"error": "$.state"})
        check("封口后 append 冲突", call(u + "/append",
              {"id": "L1", "y": 1}) == {"error": "$.state"})
        check("封口后 fetch cut=0", call(u + "/fetch",
              {"id": "L1", "after": 0}) == {"rows": [], "cut": 0})

        call(u + "/open", {"id": "L2"})
        seqs = [call(u + "/append", {"id": "L2", "y": y}) for y in (1, 0, 1)]
        check("seq 从 1 连续", seqs == [1, 2, 3], str(seqs))
        check("y=2 拒绝且不改状态",
              call(u + "/append", {"id": "L2", "y": 2}) == {"error": "$.y"})
        check("y 布尔拒绝",
              call(u + "/append", {"id": "L2", "y": True}) == {"error": "$.y"})
        check("id 布尔拒绝",
              call(u + "/append", {"id": True, "y": 1}) == {"error": "$.id"})
        check("after 负拒绝", call(u + "/fetch",
              {"id": "L2", "after": -1}) == {"error": "$.after"})
        check("after 超长度拒绝", call(u + "/fetch",
              {"id": "L2", "after": 4}) == {"error": "$.after"})
        check("after 布尔拒绝", call(u + "/fetch",
              {"id": "L2", "after": True}) == {"error": "$.after"})
        check("after=0 全量", call(u + "/fetch",
              {"id": "L2", "after": 0}) ==
              {"rows": [[1, 1], [2, 0], [3, 1]], "cut": None})
        check("after=2 增量", call(u + "/fetch",
              {"id": "L2", "after": 2}) ==
              {"rows": [[3, 1]], "cut": None})
        check("after=长度 空增量", call(u + "/fetch",
              {"id": "L2", "after": 3}) == {"rows": [], "cut": None})
        check("封口回末序号 3", call(u + "/close", {"id": "L2"}) == 3)
        check("封口 fetch 带 cut", call(u + "/fetch",
              {"id": "L2", "after": 1}) ==
              {"rows": [[2, 0], [3, 1]], "cut": 3})
        check("封口后 after 上界=cut", call(u + "/fetch",
              {"id": "L2", "after": 3}) == {"rows": [], "cut": 3})
    finally:
        kill9(p)


# ================================================================ B. 后端
def test_backend(root):
    print("B. alpha 一次初始化 / start 原子 j / query 状态机")
    port = free_port()
    db = os.path.join(root, "b.db")
    p = start([PY, "backend.py", "--port", str(port), "--db", db],
              env={"LOGDIR": root})
    try:
        wait_port(port)
        u = "http://127.0.0.1:%d" % port
        check("未初始化 alpha 拒绝 start",
              call(u + "/start", {"id": "t1"}) == {"error": "$.alpha"})
        check("alpha 边界 0 拒绝",
              call(u + "/init", {"alpha": "0/1"}) == {"error": "$.alpha"})
        check("alpha 边界 1 拒绝",
              call(u + "/init", {"alpha": "2/2"}) == {"error": "$.alpha"})
        check("alpha 负分母拒绝",
              call(u + "/init", {"alpha": "1/-2"}) == {"error":
              "$.alpha.denominator"})
        check("init 1/2 成功", call(u + "/init",
              {"alpha": "1/2"}) is None)
        check("init 冲突拒绝", call(u + "/init",
              {"alpha": "1/3"}) == {"error": "$.alpha"})
        check("init 同值幂等", call(u + "/init",
              {"alpha": "2/4"}) is None)  # 约分后同为 1/2
        check("start t1", call(u + "/start", {"id": "t1"}) ==
              {"j": 1, "a": "1/4"})
        check("start t2", call(u + "/start", {"id": "t2"}) ==
              {"j": 2, "a": "1/8"})
        check("start t3", call(u + "/start", {"id": "t3"}) ==
              {"j": 3, "a": "1/16"})
        check("同 id 回原值", call(u + "/start", {"id": "t2"}) ==
              {"j": 2, "a": "1/8"})
        check("并发/非法 id", call(u + "/start",
              {"id": "bad id"}) == {"error": "$.id"})
        check("query 未知 id", call(u + "/query",
              {"id": "zzz"}) == {"error": "$.id"})
        check("未结清 pending", call(u + "/query",
              {"id": "t1"}) == {"state": "pending", "result": None})
    finally:
        kill9(p)


# ================================================================ C. 题面示例
def test_example(root):
    print("C. 题面端到端示例（触发停采 + 停令前读数归档）")
    c = Cluster(os.path.join(root, "c"))
    try:
        r = call(c.back + "/start", {"id": "demo"})
        check("分配 j=1 a=1/4", r == {"j": 1, "a": "1/4"}, str(r))
        check("仪器源 open 回 null", c.open_log("demo") is None)
        for k, y in enumerate((1, 1, 1, 1), start=1):
            check("append %d -> seq %d" % (y, k),
                  call(c.src + "/append", {"id": "demo", "y": y}) == k)
        # 停令到达前再采一个 0（worker 尚未轮询触发，日志仍开）
        check("停令前仍可 append 0",
              call(c.src + "/append", {"id": "demo", "y": 0}) == 5)
        q = wait_done(c.back, "demo")
        res = q["result"]
        check("decision=alarm", res["decision"] == "alarm", str(res))
        check("n=4", res["n"] == 4, str(res))
        check("e=81/16", res["e"] == "81/16", str(res))
        check("cut=5", res["cut"] == 5, str(res))
        check("归档五行且升序",
              res["rows"] == [[1, 1], [2, 1], [3, 1], [4, 1], [5, 0]],
              str(res["rows"]))
        # 源端确实已封口，seq 5 也已落入
        f = call(c.src + "/fetch", {"id": "demo", "after": 0})
        check("源端封口 cut=5", f["cut"] == 5 and len(f["rows"]) == 5, str(f))
        check("done 后再 query 稳定", wait_done(c.back, "demo") == q)
    finally:
        c.stop()


# ================================================================ D. 外部封口
def test_external_close(root):
    print("D. 外部先封口：alarm / exhausted / 空日志")
    c = Cluster(os.path.join(root, "d"))
    try:
        # D1 外部先封口，前缀内有触发
        call(c.back + "/start", {"id": "ext1"})
        c.open_log("ext1")
        for y in (1, 1, 1, 1, 0):
            call(c.src + "/append", {"id": "ext1", "y": y})
        check("外部 close 回 5", call(c.src + "/close", {"id": "ext1"}) == 5)
        q = wait_done(c.back, "ext1")
        r = q["result"]
        check("ext1 alarm n=4 e=81/16 cut=5",
              (r["decision"], r["n"], r["e"], r["cut"]) ==
              ("alarm", 4, "81/16", 5), str(r))

        # D2 永不触发（全 0），外部封口 -> exhausted，n=cut，e 为全日志证据
        call(c.back + "/start", {"id": "ext2"})
        c.open_log("ext2")
        for y in (0, 0, 0):
            call(c.src + "/append", {"id": "ext2", "y": y})
        call(c.src + "/close", {"id": "ext2"})
        q = wait_done(c.back, "ext2")
        r = q["result"]
        check("ext2 exhausted n=cut=3 e=1/8",
              (r["decision"], r["n"], r["e"], r["cut"]) ==
              ("exhausted", 3, "1/8", 3), str(r))
        check("ext2 rows 三行 0",
              r["rows"] == [[1, 0], [2, 0], [3, 0]], str(r["rows"]))

        # D3 空日志外部封口
        call(c.back + "/start", {"id": "ext3"})
        c.open_log("ext3")
        call(c.src + "/close", {"id": "ext3"})
        q = wait_done(c.back, "ext3")
        r = q["result"]
        check("空日志 exhausted n=0 e=1/1 cut=0 rows=[]",
              (r["decision"], r["n"], r["e"], r["cut"], r["rows"]) ==
              ("exhausted", 0, "1/1", 0, []), str(r))
    finally:
        c.stop()


# ================================================================ E. 强退
def test_crashes(root):
    print("E. 强退验收（os._exit(9)，等同 SIGKILL，无缓冲清理）")
    scenarios = [
        ("CRASH_BEFORE_COMMIT", 4),
        ("CRASH_AFTER_COMMIT", 4),
        ("CRASH_BEFORE_CLOSE", 4),
        ("CRASH_BEFORE_FINALIZE", 4),
    ]
    for crash, ones in scenarios:
        sub = os.path.join(root, "e_" + crash.lower())
        c = Cluster(sub, worker_env={crash: "1"})
        try:
            # 标识仅 ASCII 字母数字（下划线非法），用纯字母串
            ident = "k" + crash.lower().replace("_", "")
            call(c.back + "/start", {"id": ident})
            c.open_log(ident)
            for _ in range(ones):
                call(c.src + "/append", {"id": ident, "y": 1})
            call(c.src + "/append", {"id": ident, "y": 0})  # 停令前读数
            # 等 worker 强退
            end = time.time() + 10
            while time.time() < end and c.worker.poll() is None:
                time.sleep(0.05)
            check("%s 进程确实强退" % crash, c.worker.poll() == 9,
                  "rc=%r" % c.worker.poll())
            if crash == "CRASH_BEFORE_CLOSE":
                f = call(c.src + "/fetch", {"id": ident, "after": 0})
                check("%s 停令未发出，日志仍开" % crash, f["cut"] is None, str(f))
            # 强退后日志若仍开，再补采一个 0，验证重启后照样归档
            if call(c.src + "/fetch",
                    {"id": ident, "after": 0})["cut"] is None:
                call(c.src + "/append", {"id": ident, "y": 0})
            c.restart_worker()  # 干净 worker 接管
            q = wait_done(c.back, ident, timeout=20)
            r = q["result"]
            check("%s 重启后 done=alarm n=4 e=81/16" % crash,
                  r["decision"] == "alarm" and r["n"] == 4 and
                  r["e"] == "81/16", str(r))
            check("%s cut 与归档行数一致且 seq 唯一连续" % crash,
                  r["cut"] == len(r["rows"]) and
                  [row[0] for row in r["rows"]] ==
                  list(range(1, r["cut"] + 1)), str(r))
            # 再重启一次 worker，done 任务不被重复处理
            c.restart_worker()
            time.sleep(0.5)
            q2 = call(c.back + "/query", {"id": ident})
            check("%s 二次重启结果不变（不重复分析）" % crash, q2 == q)
        finally:
            c.stop()


# ================================================================ F. 丢应答
def _expect(ys, j, alpha=(1, 2)):
    """独立重算预期结果：返回 (decision, n, e_str, cut)。"""
    from math import gcd
    an, ad = alpha
    a_num, a_den = an, ad * (2 ** j)  # a = alpha/2^j
    e_num, e_den = 1, 1
    n = None
    trig_e = None
    for idx, y in enumerate(ys, start=1):
        if y == 1:
            e_num *= 3
            e_den *= 2
        else:
            e_den *= 2
        if n is None and e_num * a_num >= e_den * a_den:
            g = gcd(e_num, e_den)
            n, trig_e = idx, (e_num // g, e_den // g)
    g = gcd(e_num, e_den)
    final_e = (e_num // g, e_den // g)
    cut = len(ys)
    if n is not None:
        return "alarm", n, "%d/%d" % trig_e, cut
    return "exhausted", cut, "%d/%d" % final_e, cut


def test_flaky(root):
    print("F. 50%% 丢应答代理下最终一致（全部外部封口，独立重算核对）")
    c = Cluster(os.path.join(root, "f"), use_proxy=True, proxy_rate=0.5)
    try:
        expected = {}
        for i in range(6):
            ident = "f%d" % i
            st = call(c.back + "/start", {"id": ident})
            j = st["j"]
            c.open_log(ident)
            # 混合 0/1 序列，制造不同触发位置与不触发情形
            ys = ([1, 1, 1, 1, 0], [0, 0, 0], [1, 0, 1, 1, 1, 0],
                  [0, 1, 0, 1], [1, 1, 0, 0, 0], [0, 1, 1, 1])[i]
            for y in ys:
                call(c.src + "/append", {"id": ident, "y": y})
            call(c.src + "/close", {"id": ident})  # 全部外部封口
            expected[ident] = (j, ys)
        for i in range(6):
            ident = "f%d" % i
            q = wait_done(c.back, ident, timeout=40)
            r = q["result"]
            j, ys = expected[ident]
            exp = _expect(ys, j)
            check("%s 归档完整 cut==行数且 seq 连续" % ident,
                  r["cut"] == len(r["rows"]) and
                  [row[0] for row in r["rows"]] ==
                  list(range(1, r["cut"] + 1)), str(r))
            check("%s 决策/n/e/cut 与独立重算一致 %s" % (ident, exp),
                  (r["decision"], r["n"], r["e"], r["cut"]) == exp, str(r))
            check("%s rows 与原始读数一致" % ident,
                  [row[1] for row in r["rows"]] == ys, str(r))
    finally:
        c.stop()


# ================================================================ G. 进程重启
def test_process_restart(root):
    print("G. 源/后端进程 kill -9 后同库重启")
    c = Cluster(os.path.join(root, "g"))
    try:
        call(c.back + "/start", {"id": "rst"})
        c.open_log("rst")
        call(c.src + "/append", {"id": "rst", "y": 1})
        # 同库拉起（重启后的进程也纳入统一回收）
        c.restart_source()
        c.restart_backend()
        check("后端重启后 start 同 id 回原值",
              call(c.back + "/start", {"id": "rst"}) ==
              {"j": 1, "a": "1/4"})
        check("源重启后读数仍在", call(c.src + "/fetch",
              {"id": "rst", "after": 0})["rows"] == [[1, 1]])
        for y in (1, 1, 1, 0):
            call(c.src + "/append", {"id": "rst", "y": y})
        q = wait_done(c.back, "rst")
        r = q["result"]
        check("重启续算 alarm n=4 e=81/16 cut=5",
              (r["decision"], r["n"], r["e"], r["cut"]) ==
              ("alarm", 4, "81/16", 5), str(r))
    finally:
        c.stop()


# ================================================================ H. 双 worker
def test_two_workers(root):
    print("H. 双 worker 同库抢任务，恰好结清一次")
    c = Cluster(os.path.join(root, "h"))
    w2 = start([PY, "worker.py", "--db", c.back_db,
                "--source", c.source_url, "--poll", "0.05",
                "--lease", "2"],
               env={"LOGDIR": root})
    try:
        ids = []
        meta = {}
        for i in range(8):
            ident = "w%d" % i
            ids.append(ident)
            st = call(c.back + "/start", {"id": ident})
            c.open_log(ident)
            ys = [1, 1, 1, 1] if i % 2 else [0, 0]
            for y in ys:
                call(c.src + "/append", {"id": ident, "y": y})
            call(c.src + "/close", {"id": ident})  # 全部外部封口
            meta[ident] = (st["j"], ys)
        results = {}
        for ident in ids:
            results[ident] = wait_done(c.back, ident, timeout=30)["result"]
        check("8 个任务全部 done", len(results) == 8)
        # 决策由各自 j 的阈值独立重算决定（j 递增，阈值随之指数提高）
        for ident in ids:
            j, ys = meta[ident]
            exp = _expect(ys, j)
            r = results[ident]
            check("%s 决策与独立重算一致 %s" % (ident, exp),
                  (r["decision"], r["n"], r["e"], r["cut"]) == exp, str(r))
        for ident, r in results.items():
            check("%s 归档无重复" % ident,
                  [row[0] for row in r["rows"]] ==
                  list(range(1, r["cut"] + 1)), str(r))
    finally:
        kill9(w2)
        c.stop()


def main():
    root = tempfile.mkdtemp(prefix="detector_accept_")
    print("workdir:", root)
    try:
        test_source(root)
        test_backend(root)
        test_example(root)
        test_external_close(root)
        test_crashes(root)
        test_flaky(root)
        test_process_restart(root)
        test_two_workers(root)
    finally:
        print("\n==== PASS=%d FAIL=%d ====" % (PASS, FAIL))
        if FAIL:
            sys.exit(1)
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
