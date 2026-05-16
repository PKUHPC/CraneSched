# Performance Tuning Guide

This guide covers the key configuration parameters for tuning CraneSched performance, especially for high-throughput short-lived job workloads.

All parameters below are configured in `/etc/crane/config.yaml`.

## Parameters

### CraneCtld

| Parameter | Default | Description |
|---|---|---|
| `CraneCtld.ThreadPoolSize` | 0 (auto = hardware_concurrency) | General-purpose thread pool for the control daemon. Handles gRPC requests, database operations, and internal task dispatch. Increase if the control node CPU has few cores but manages many compute nodes. |
| `CraneCtld.SchedulerRpcThreadPoolSize` | 0 (auto) | Dedicated thread pool for scheduler-to-craned RPC dispatch (AllocSteps, ExecuteSteps, FreeJobs, etc.). Increase for large clusters to avoid scheduling RPC backlog. |
| `CraneCtld.StatusChangeFlushTimeoutMs` | 100 | Interval (ms) at which queued status changes are batch-processed. Lower values reduce per-job latency but increase CPU usage. For high-throughput short jobs, set to 10. |
| `CraneCtld.StatusChangeBatchNum` | 1000 | When the status change queue reaches this size, processing is triggered immediately without waiting for the timer. Lower values improve responsiveness under burst load. |

### Craned

| Parameter | Default | Description |
|---|---|---|
| `Craned.ThreadPoolSize` | 0 (auto = hardware_concurrency) | General-purpose thread pool on each compute node. Used for supervisor process spawning, shutdown, gRPC handling, and cgroup cleanup. **This is the most impactful parameter for tail latency.** When saturated, supervisor spawn and shutdown operations queue up, causing significant delays. For high-throughput workloads, set to 2-4x the CPU core count. |

### Supervisor

| Parameter | Default | Description |
|---|---|---|
| `Supervisor.ThreadPoolSize` | 0 (auto = hardware_concurrency) | Thread pool inside each supervisor process. Has minimal impact on short-lived jobs. Keep at default unless running complex multi-step jobs within a single supervisor. |

## Recommended Profiles

### High-Throughput Short Jobs

For workloads with many short-lived batch jobs (AI inference, parameter sweeps, CI pipelines):

```yaml
CraneCtld:
  ThreadPoolSize: 32
  SchedulerRpcThreadPoolSize: 8
  StatusChangeFlushTimeoutMs: 10
  StatusChangeBatchNum: 100

Craned:
  ThreadPoolSize: 32  # or CPU cores × 4

Supervisor:
  ThreadPoolSize: 0
```

### General HPC

For typical HPC workloads with longer-running jobs, the defaults work well:

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
