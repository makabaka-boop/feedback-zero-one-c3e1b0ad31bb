# -*- coding: utf-8 -*-
"""并发 start 原子性专项：N 个线程同时 start 不同 id，j 必须恰为 1..N 的排列。"""
import json
import os
import shutil
import sys
import tempfile
import threading
import urllib.request
import urllib.error

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import acceptance_test as t


def call(url, payload):
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return json.loads(e.read())


def main():
    root = tempfile.mkdtemp(prefix="concurrent_")
    c = t.Cluster(os.path.join(root, "cc"))
    try:
        n = 30
        results = {}
        lock = threading.Lock()

        def worker(i):
            ident = "c%03d" % i
            r = call(c.back + "/start", {"id": ident})
            with lock:
                results[ident] = r

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        js = sorted(r["j"] for r in results.values())
        assert js == list(range(1, n + 1)), "j not a permutation: %r" % js
        # a = alpha/2^j, alpha=1/2
        for ident, r in results.items():
            j = r["j"]
            assert r["a"] == "1/%d" % (2 ** (j + 1)), (ident, r)
        # 同 id 并发/重复 start 回原值
        ident = "dup01"
        first = call(c.back + "/start", {"id": ident})

        def dup():
            r = call(c.back + "/start", {"id": ident})
            assert r == first, (r, first)

        dts = [threading.Thread(target=dup) for _ in range(10)]
        for th in dts:
            th.start()
        for th in dts:
            th.join()
        # 已有 30 个任务，dup01 应拿到 j=31
        assert first == {"j": 31, "a": "1/%d" % (2 ** 32)}, first
        print("concurrent start OK: j permutation 1..%d, duplicate idempotent" % n)
    finally:
        c.stop()
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
