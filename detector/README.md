# 连续监测触发停采系统（纯 Python 标准库 + SQLite）

三个**独立进程**、两份**独立 SQLite 库**，全部状态原子持久化，进程可被 `kill -9`
后同库重启，靠断点与幂等调用收敛，不重复分析。

## 组件

| 文件 | 角色 | 持久化 |
|---|---|---|
| `source.py` | 独立持久化 **HTTP 仪器源** | `source.db` |
| `backend.py` | 检测后端 HTTP API（alpha/start/query） | `backend.db` |
| `backend_store.py` | 后端共享存储（后端进程与 worker 共用同一库文件） | `backend.db` |
| `worker.py` | 工作进程：补开/补取/分析/停采/归档/结清 | `backend.db` |
| `common.py` | 分数（约分 p/q 串）、严格校验、HTTP 服务端/客户端 | — |
| `flaky_proxy.py` | 验收用丢应答代理 | — |
| `acceptance_test.py` | 全量验收（120 项断言，8 个场景） | 临时目录 |
| `concurrent_test.py` | 并发 start 原子性专项 | 临时目录 |

## 启动

```bash
# 1) 仪器源（独立进程、独立库）
python3 source.py --host 127.0.0.1 --port 9001 --db ./data/source.db

# 2) 后端（alpha 仅初始化一次；也可启动后用 init 接口设置）
python3 backend.py --host 127.0.0.1 --port 9002 --db ./data/backend.db --alpha 1/2

# 3) 工作进程（与后端同一个 backend.db，通过 HTTP 访问源）
python3 worker.py --db ./data/backend.db --source http://127.0.0.1:9001 \
                  --poll 0.2 --lease 30
```

所有接口均为 `POST` + JSON，成功回业务结果或 `null`，失败回
`{"error": "<JSONPath>"}`（下标零基，整数禁布尔，拒绝不改状态）。

## 仪器源接口 `source.py`

| 方法 | 语义 |
|---|---|
| `open(id)` | 幂等建空日志；不重开已关闭日志（回 `$.state`）；回 `null` |
| `append(id,y)` | 仅对未关闭日志追加 `0/1`，回从 1 连续的正整数 `seq` |
| `close(id)` | 封口；重复回原末序号 `cut`；空日志 `cut=0` |
| `fetch(id,after)` | 回 `{"rows":[[seq,y]...],"cut":int\|null}`；`after∈[0,日志长度]` |

除 `open` 外，未知 id 一律 `$.id` 拒绝。标识限**非空 ASCII 字母数字串**。

## 后端接口 `backend.py`

| 方法 | 语义 |
|---|---|
| `init(alpha)` | `p/q` 串，仅首次生效；重复须约分后相等，否则 `$.alpha` |
| `start(id)` | 原子分配**从 1 连续**的 `j` 与 `a=alpha/2^j`，回 `{"j","a"}`；同 id 回原值 |
| `query(id)` | 结清前 `{"state":"pending","result":null}`；done 回完整结果 |

`alpha` 仅初始化一次且必须 `0<alpha<1`，用整数分子、正整数分母的 `p/q` 串表示。

## 检测与终态语义（worker）

- 证据 `E` 初值 `1`；遇 `1` 乘 `3/2`，遇 `0` 乘 `1/2`，全程用整数分子分母精确计算并约分。
- 首次满足 `E >= 1/a` 时**固定**位置 `n` 与该位置证据 `e=E`，置 `frozen`，
  随即向源发 `close` 停采；**之后只归档、不再分析**。
- 停令到达前已采的读数在随后的 `fetch` 中照常归档。
- 外部也可先 `close`：封口后 worker 对**全部前缀**检查一次，有触发 `alarm`，
  否则 `exhausted`；无触发时 `n=cut`、`e` 为全日志最终证据；空日志 `n=0,e=1/1`。
- 封口且 `archived==cut`（归档完整）后，一次性原子写终态 `done`：
  `{decision,n,e,cut,rows}`，`rows` 按 `seq` 升序，分数为约分正分母 `p/q` 串。

## 持久化与容错

- SQLite `WAL + synchronous=FULL`，每个写动作在单事务内提交（预算、断点、证据、
  归档、终态均原子）。
- `progress` 断点：`e_num/e_den`（证据）、`next_seq`、`archived`、`cut/closed`、
  `frozen` 与触发固定值 `trigger_n/trigger_enum/trigger_eden`、租约 `owner/lease`。
- `archive` 用 `INSERT OR IGNORE`，重放重叠读数被跳过，**不重复分析**。
- 源端所有操作幂等；通信可丢应答，worker 失败即回轮询重试，做到
  **重启补开、补取、补停**。
- 租约：worker 领取任务带 TTL（`--lease`），持有者强退后租约过期即可被新进程接管；
  空轮询（无新增量且未封口）主动释放租约，避免占着任务空转。
- 后端 HTTP 多线程与 worker 对同一连接的访问由 `RLock` 串行化；
  跨进程由 SQLite WAL + busy timeout 保证。

## 故障注入与强退验收

worker 支持四个环境变量，命中即 `os._exit(9)`（等价 SIGKILL，无缓冲清理）：

| 变量 | 注入点 |
|---|---|
| `CRASH_BEFORE_COMMIT` | 新增量归档事务提交前 |
| `CRASH_AFTER_COMMIT` | 归档事务提交后、停令前 |
| `CRASH_BEFORE_CLOSE` | 发出停采令前 |
| `CRASH_BEFORE_FINALIZE` | 终态写入前 |

```bash
# 全量验收（含封口、四类强退重启、50% 丢应答、进程重启、双 worker）
python3 acceptance_test.py

# 并发 start 原子性
python3 concurrent_test.py
```

验收会断言：强退进程退出码为 9；重启后结果与无故障一致（如题面
`alarm, n=4, e=81/16, cut=5, 归档五行`）；二次重启结果不变（不重复分析）。

## 题面示例核对

`alpha=1/2` → 首任务 `j=1, a=1/4, 1/a=4`。读数 `1,1,1,1`：

| seq | y | E | 是否触发 |
|---|---|---|---|
| 1 | 1 | 3/2 = 1.5 | 否 |
| 2 | 1 | 9/4 = 2.25 | 否 |
| 3 | 1 | 27/8 = 3.375 | 否 |
| 4 | 1 | 81/16 = 5.0625 | **是，n=4, e=81/16** |

停令到达前再采一个 `0`（seq=5），封口后 `cut=5`、归档五行，
终态 `alarm, n=4, e=81/16, cut=5`。
