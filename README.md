# taskflow

一个用 **Python 标准库**（asyncio + sqlite3 + argparse）实现的轻量并发任务队列：
批量提交耗时 shell 命令，多个 worker 一起跑；任务持久化在 SQLite 里，
进程被 `kill -9` 强杀后重启能自动恢复，同一个任务绝不会同时被两个
worker 执行。

无 celery / redis / 任何第三方运行时依赖；pytest 仅用于测试。

## 安装

要求 Python 3.8+。两种用法任选：

```bash
# 方式一：直接从源码目录用模块方式跑，零安装
python -m taskflow --help

# 方式二：pip 安装后得到 taskflow 命令
pip install -e .
taskflow --help
```

下文中的 `taskflow` 都可以替换成 `python -m taskflow`。

## 快速上手

```bash
# 1) 提交任务（任务 id 必须唯一）
taskflow submit job1 "sleep 1 && echo hello"
taskflow submit job2 "python do_something.py"

# 2) 启动 worker，默认 4 个并发槽位
taskflow worker --workers 4

# 3) 另开终端查看状态
taskflow status          # 各状态计数
taskflow status job1     # 单个任务详情
taskflow list            # 全部任务一览
```

数据库默认放在当前目录的 `taskflow.db`。可以用 `--db` 指定，
也可以用环境变量 `TASKFLOW_DB`：

```bash
taskflow --db /data/jobs.db submit job1 "echo hi"   # 全局放在子命令前也行
taskflow submit --db /data/jobs.db job1 "echo hi"   # 放在子命令后也行
```

可以同时启动多个 worker 进程（同一台机器或共享存储的多台机器），
它们会自动分摊任务：

```bash
taskflow worker --workers 4 &
taskflow worker --workers 4 &
```

## 任务状态与重试

```
pending ──▶ running ──▶ succeeded
               │
               ├─▶ failed ──▶（指数退避后重新被领取）──▶ running
               │
               └─▶ dead      （3 次重试都失败，彻底放弃）
```

* `pending`：排队等待；`failed`：上一次执行失败、正在退避等待
  （`pending` / `failed` 都属于“可被领取”）。
* 失败自动重试 **3 次**，即一个任务最多执行 **4 次**（首次 + 3 次重试）。
* 重试间隔指数退避：**1s、2s、4s**（可用 `--backoff-base` 调整基数）。
* 4 次全部失败后进入 `dead`，不再被领取。
* 命令以 shell 方式执行（POSIX 用 `/bin/sh -c`，Windows 用 cmd），
  退出码非 0 即视为失败；stdout/stderr 会收进数据库的 result 字段
  （失败时保留末尾 2000 字符）。

## 持久化与“不丢任务 / 不双跑”的保证

所有状态都实时落盘在 SQLite（WAL 模式），任务的领取是**单条原子
UPDATE**：

```sql
UPDATE tasks SET status='running', lease_token=?, lease_expires=?, ...
WHERE id=? AND status IN ('pending','failed') AND run_after <= ?
```

多个 worker 进程同时抢同一个任务时，数据库层面只有一个 UPDATE 能命中，
天然互斥。

每次领取还会生成一个随机的 **租约（lease）**：

* 任务被领取时写入 `lease_token` 和 `lease_expires`；
* 执行期间有独立心跳协程周期性续租（默认每 3s，租约 10s）；
* 写结果时必须带上匹配的 `lease_token`（fencing），即使租约意外易主，
  旧持有者的结果也不会覆盖新执行；
* worker 被 `kill -9` 后心跳停止，租约到期，任务会被存活 worker 的
  reaper（或重启 worker 的启动回收）退回 `pending` 重新执行。

因此语义是 **at-least-once**：正常情况下任务恰好执行一次；只有在
worker 被强杀这种极端时刻，正在跑的那个任务可能再跑一遍。请让命令
尽量幂等。相关参数可按场景调小（比如更快检测崩溃）：

```bash
taskflow worker --workers 4 --lease-seconds 10 \
                --heartbeat-interval 3 --reap-interval 2
# 也可用环境变量：TASKFLOW_LEASE_SECONDS / TASKFLOW_HEARTBEAT_INTERVAL /
# TASKFLOW_REAP_INTERVAL / TASKFLOW_POLL_INTERVAL / TASKFLOW_BACKOFF_BASE
```

## 优雅关闭

worker 收到 **Ctrl+C（SIGINT）** 或 **SIGTERM** 后：

1. 立刻停止领取新任务；
2. 正在执行的子进程**等它跑完**，结果照常写回数据库；
3. 全部收尾后进程以**退出码 0** 退出。

关闭期间（或进程已退出时）新 `submit` 的任务安全地留在 `pending`，
下次启动 worker 就会继续执行，不会丢。

> Windows 说明：Windows 上 SIGTERM 无法被进程捕获，Ctrl+C / Ctrl+Break
> （SIGINT）可以优雅关闭；测试里的 SIGTERM 用例只在 POSIX 上运行。

## 命令参考

```text
taskflow submit <任务id> <shell命令>   提交任务；id 重复时报错（退出码 2），不覆盖
taskflow worker [--workers N]         启动 worker 持续消费，默认 N=4
taskflow status [任务id]              不带 id 看计数，带 id 看详情
taskflow list                         列出所有任务
```

worker 的主要选项：

| 选项 | 默认 | 说明 |
| --- | --- | --- |
| `--workers` | 4 | 并发槽位（本进程内同时跑几个命令） |
| `--lease-seconds` | 10 | 任务租约时长，超时视为持有者已死 |
| `--heartbeat-interval` | 3 | 执行中续租的间隔秒数 |
| `--reap-interval` | 2 | 扫描过期 running 任务的间隔 |
| `--poll-interval` | 0.2 | 空队列时的轮询间隔 |
| `--backoff-base` | 1 | 重试退避基数（1/2/4 秒） |

## 运行测试

```bash
python -m pytest
```

测试覆盖：

* 重复提交拒绝、原子领取互斥、fencing token、指数退避与 dead 流转；
* **100 个任务 + 3 worker** 的随机 sleep/随机失败批量验收；
* 子进程 worker 运行中被 `kill`（模拟 `kill -9`），重启后原 running
  任务被重新执行，且通过任务内时间戳日志验证**同一任务不存在两段重叠
  执行区间**；
* 进程内/（POSIX）SIGTERM 优雅关闭：退出码 0、在跑任务跑完、关闭后
  新提交的任务重启后全部执行。

## 项目结构

```text
taskflow/
  __init__.py
  __main__.py     # python -m taskflow 入口
  cli.py          # argparse 命令行
  db.py           # SQLite 存储：提交/原子领取/心跳/结果/租约回收
  worker.py       # asyncio 多槽位执行器 + 心跳 + reaper + 信号处理
tests/            # pytest 测试（不参与运行时）
pyproject.toml
```
