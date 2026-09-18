# taskflow

一个很小的、基于 SQLite 的并发 shell 命令任务队列。**运行时只依赖 Python 标准库**
（`asyncio` + `sqlite3` + `argparse` + `signal` …），测试用 `pytest`。

## 特性

- 命令行提交任务，worker 进程开 N 个并发槽一起跑（默认 4）。
- 任务状态：`pending → running → succeeded`；失败进入 `failed` 等待退避后自动重试，
  重试耗尽进入 `dead`。
- 失败自动重试：**最多重试 3 次（共执行 4 次）**，退避间隔 **1s / 2s / 4s** 指数增长。
- 全部状态持久化到 SQLite（WAL + `synchronous=FULL`，每条状态转移都是单条原子 SQL）。
- `kill -9` 强杀 worker 后重启，未完成的 `running` 任务自动恢复重跑，**同一个任务
  绝不会同时被执行两遍**（见下文“并发与崩溃安全”）。
- 任务 id 重复提交会报错 `task already exists` 并以退出码 1 结束，不会覆盖或重置原任务。
- 收到 `SIGTERM` / `SIGINT`（Ctrl+C；Windows 上为 `CTRL_BREAK`）时优雅关闭：停止领取
  新任务，等所有正在执行的任务跑完，然后以退出码 0 退出。

## 安装

无需安装第三方依赖，Python 3.10+ 即可。两种使用方式：

```bash
# 方式一：直接从源码目录用模块方式运行（推荐，零安装）
python -m taskflow --help

# 方式二：可编辑安装，得到 taskflow 命令
pip install -e .
taskflow --help
```

## 用法

数据库默认在当前目录的 `./taskflow.db`，也可以用 `--db` 或环境变量 `TASKFLOW_DB` 指定。

### 提交任务

```bash
taskflow submit <任务id> <要执行的 shell 命令>

# 例：
taskflow submit job-001 "echo hello && sleep 2"
taskflow submit job-002 -- "mycommand --some-flag"   # 命令以 - 开头时用 -- 隔开
```

- 每个任务 id 全局唯一；重复提交返回退出码 1 并打印 `task already exists: '...'`。
- 提交动作不需要有 worker 在运行；任务先落库，随时可以启动 worker 消费。

### 启动 worker

```bash
taskflow worker                 # 默认 4 个并发槽
taskflow worker --workers 3     # 3 个并发
```

worker 会：

1. 启动时检查上一个崩溃的 worker，把遗留的 `running` 任务原子地重置为 `pending`；
2. 多槽并发地原子领取任务并通过系统 shell 执行（命令的 stdout/stderr 直接继承 worker）；
3. 每 5 秒上报心跳；后台 reaper 周期性兜底回收其它已崩溃 worker 的任务；
4. 收到关闭信号后排干在执行的任务并退出。

可以同时起多个 worker 进程（甚至多台机器共享网络盘上的库），SQLite 的原子领取保证
任务不会被重复执行。

### 查询

```bash
taskflow list                 # 列出所有任务（顶部有各状态计数汇总）
taskflow status <任务id>       # 单个任务详情（状态、尝试次数、时间、最后错误）
taskflow runs <任务id>         # 每次尝试的时间/退出码（被崩溃打断的标记为 INTERRUPTED）
```

## 任务状态机

```
                领取(原子)
 pending ─────────────────► running ──退出码 0──► succeeded（终态）
   ▲                           │
   │ 崩溃恢复(recover)          ├──退出码非0 且还有重试名额──► failed
   │                           │                              │
   └───────────────────────────┘                              │ 退避到期
                                                                ▼
                                       退出码非0 且 4 次均失败 ──► dead（终态）
```

- `failed` 是“等待重试”的中间态，`next_attempt_at` 记录下次可领取时间（1/2/4 秒后）。
- 被强杀打断的那次执行**不计入重试次数**（`attempts` 回退），任务回到 `pending` 重跑。
- 最终只会停在 `succeeded` 或 `dead`，不会卡在 `pending/running`。

## 并发与崩溃安全（为什么不会跑两遍）

1. **原子领取**：领取是单条 `UPDATE ... WHERE status IN ('pending','failed')
   AND next_attempt_at <= now ... RETURNING`。SQLite 串行化写事务，两个进程
   不可能同时领到同一行；第二个人只会看到“没有可领的任务”。
2. **租约 + 心跳**：worker 每 5 秒写心跳；任务运行记录（`task_runs`）绑定所属 worker。
   worker 被判定死亡的条件是心跳变旧 **且** 其 pid 已不存在（并用进程创建时间防止
   pid 复用误判）。
3. **孤儿进程处理**：
   - Linux：任务子进程以新会话启动，并在 fork 后设置 `PR_SET_PDEATHSIG`，父 worker
     一死内核即发 `SIGKILL`；恢复时再用进程组 `SIGKILL` 兜底。
   - Windows：任务的 shell 及其子孙进程全部加入 `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`
     作业，worker 进程消失时操作系统自动终止整棵进程树。
4. **恢复顺序**：新 worker 启动时先杀遗留子进程树 → 再在一个事务里把这些任务重置为
   `pending`、关闭旧 run 记录（标记 `interrupted`、退出码 -1）→ 然后才开始领任务。
   因此“旧命令的最后一个字节”一定早于“新命令的第一个字节”，不会重叠。

## 优雅关闭语义

- 收到信号后 worker 立即停止领取新任务；
- 正在执行的子进程**不会被杀**，等它们自然结束并落库结果；
- 全部在执行任务结束后写 `workers.status='shutdown'`、关闭数据库、以 **退出码 0** 退出；
- 关闭期间/之后新提交的任务照常落库，下次启动 worker 时会被执行。

## 配置（环境变量，通常不用动）

| 变量 | 默认值 | 含义 |
| --- | --- | --- |
| `TASKFLOW_DB` | `./taskflow.db` | SQLite 数据库路径 |
| `TASKFLOW_MAX_ATTEMPTS` | `4` | 总尝试次数（初次 + 3 次重试） |
| `TASKFLOW_RETRY_BACKOFF_BASE` | `1` | 退避基数（秒） |
| `TASKFLOW_RETRY_BACKOFF_FACTOR` | `2` | 退避倍数 |
| `TASKFLOW_HEARTBEAT_INTERVAL` | `5` | 心跳间隔（秒） |
| `TASKFLOW_LEASE_TIMEOUT` | `30` | 租约超时（秒，跨机 pid 不可验时的保守阈值） |

## 跑测试

```bash
pytest          # 一条命令；Windows/Linux 均可运行
```

测试覆盖（`tests/`）：

- `test_db.py`：重复提交拒绝、原子领取、退避时长（1/2/4s）、重试耗尽进 dead、
  崩溃任务恢复且不消耗重试次数、存活 worker 不被误回收；
- `test_drain.py`：100 个随机 sleep/随机失败任务 + 3 并发槽全部落到终态；
  两个 worker 进程共享队列不重复执行；
- `test_crash_recovery.py`：任务执行中 `kill -9` 强杀 worker，重启后任务重新执行
  并完成，互斥标记 + 时间戳日志证明全程无并发重复执行；pending 任务也不丢；
- `test_graceful_shutdown.py`：运行中发 SIGTERM/CTRL_BREAK，等在跑任务完成后退出码 0；
- 关闭后立刻再提交 10 个任务，重启 worker 后全部执行。

## 数据库表

- `tasks`：任务定义与当前状态、尝试次数、退避时间、最后错误；
- `task_runs`：每次尝试的流水（worker、起止时间、退出码、子进程 pid、是否被崩溃打断）；
- `workers`：worker 实例心跳与存活信息（pid、进程创建时间、状态）。