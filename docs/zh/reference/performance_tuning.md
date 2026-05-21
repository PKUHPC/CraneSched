# 性能调优指南

本文档介绍 CraneSched 的关键性能配置参数，适用于高吞吐短时作业场景的调优。

所有参数均在 `/etc/crane/config.yaml` 中配置。

## 参数说明

### CraneCtld

| 参数 | 默认值 | 说明 |
|---|---|---|
| `CraneCtld.ThreadPoolSize` | 0 (auto = hardware_concurrency) | 控制节点通用线程池。处理 gRPC 请求、数据库操作和内部任务分发。当控制节点 CPU 核数较少但管理大量计算节点时，建议调大。 |
| `CraneCtld.SchedulerRpcThreadPoolSize` | 0 (auto) | 调度器向 Craned 发送 RPC 的专用线程池（AllocSteps、ExecuteSteps、FreeJobs 等）。大规模集群下建议调大以避免调度 RPC 积压。 |
| `CraneCtld.StatusChangeFlushTimeoutMs` | 100 | 状态变更队列的批处理定时器间隔（毫秒）。值越小，单任务延迟越低，但 CPU 开销越高。高吞吐短时作业场景建议设为 10。 |
| `CraneCtld.StatusChangeBatchNum` | 1000 | 状态变更队列达到此数量时立即触发处理，无需等待定时器。值越小在突发负载下响应越快。 |

### Craned

| 参数 | 默认值 | 说明 |
|---|---|---|
| `Craned.ThreadPoolSize` | 0 (auto = hardware_concurrency) | 计算节点通用线程池。用于 Supervisor 进程的启动、关闭、gRPC 处理和 cgroup 清理。**这是对尾延迟影响最大的参数。**线程池饱和时，Supervisor 启动和关闭操作会排队，导致显著延迟。高吞吐场景建议设为 CPU 核数的 2-4 倍。 |

### Supervisor

| 参数 | 默认值 | 说明 |
|---|---|---|
| `Supervisor.ThreadPoolSize` | 0 (auto = hardware_concurrency) | 每个 Supervisor 进程内部的线程池。对短时作业影响较小。除非在单个 Supervisor 内运行复杂的多步骤作业，否则保持默认即可。 |

## 推荐配置

### 高吞吐短时作业

适用于大量短时批量作业（AI 推理、参数扫描、CI 流水线）：

```yaml
CraneCtld:
  ThreadPoolSize: 32
  SchedulerRpcThreadPoolSize: 8
  StatusChangeFlushTimeoutMs: 10
  StatusChangeBatchNum: 100

Craned:
  ThreadPoolSize: 32  # 或 CPU 核数 × 4

Supervisor:
  ThreadPoolSize: 0
```

### 通用 HPC

适用于以长时作业为主的典型 HPC 负载，使用默认值即可：

```yaml
CraneCtld:
  ThreadPoolSize: 0
  SchedulerRpcThreadPoolSize: 0
  StatusChangeFlushTimeoutMs: 100
  StatusChangeBatchNum: 1000

Craned:
  ThreadPoolSize: 0

Supervisor:
  ThreadPoolSize: 0
```
