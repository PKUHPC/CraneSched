# Distributed Tracing

CraneSched includes OpenTelemetry-based distributed tracing for scheduler and job lifecycle diagnosis. It can trace submission, scheduling, execution, status changes, and cleanup across CraneCtld, Craned, and Supervisor.

## Overview

Tracing has three common analysis views:

| View | Typical spans | Use case |
|------|---------------|----------|
| Submission | `submit/*` | Diagnose validation and enqueue latency |
| Scheduling | `scheduling/*`, `status_change/*` | Find scheduler, DB, and RPC fanout bottlenecks |
| Execution | `job/*`, `step/*`, `task/*` | Follow a job from allocation to finish and cleanup |

Runtime level controls which spans are created:

| Level | Behavior |
|-------|----------|
| `basic` | Core lifecycle spans only |
| `detailed` | Core spans plus scheduler/status/cgroup/task detail spans |
| `debug` | All spans compiled into the binary |

The effective level is `min(runtime_level, CRANE_TRACE_COMPILED_MAX_LEVEL)`. Error routing applies only to spans that were created and exported; a debug-only span is not created when the effective level is `basic`.

## Requirements

- Build CraneSched with `CRANE_ENABLE_TRACING=ON`.
- Use `CRANE_TRACE_COMPILED_MAX_LEVEL=basic|detailed|debug` to set the compiled maximum level.
- Run cplugind with the independent `trace.so` plugin.
- Provide InfluxDB 2.x for trace storage.

The current implementation uses `SimpleSpanProcessor`, so spans are exported synchronously by the OpenTelemetry SDK into CraneSched's plugin client queue. The plugin client then sends `TraceHook` requests to cplugind.

## Configuration

Enable tracing in `/etc/crane/config.yaml`:

```yaml
Tracing:
  Enabled: true
  Level: debug
```

Register both monitor and trace plugins in `/etc/crane/plugin.yaml` when both resource metrics and trace collection are needed:

```yaml
Enabled: true
PlugindSockPath: "cplugind/cplugind.sock"
TraceHookMaxRequestBytes: 3670016
Plugins:
  - Name: "monitor"
    Path: "/usr/lib/crane/plugin/monitor.so"
    Config: "/etc/crane/monitor.yaml"
  - Name: "trace"
    Path: "/usr/lib/crane/plugin/trace.so"
    Config: "/etc/crane/trace.yaml"
```

`monitor.so` and `trace.so` are intentionally separate. Resource monitoring uses `monitor.yaml`; trace collection uses `trace.yaml`. Trace bucket fields in old `monitor.yaml` files are no longer used by the trace plugin.

Example `/etc/crane/trace.yaml`:

```yaml
Tracing:
  LogPath: "/var/log/crane/trace.log"

Database:
  Type: "influxdb"
  Influxdb:
    Url: "http://localhost:8086"
    Token: "<token>"
    Org: "crane"
    TraceBucket: "crane_trace"
    TraceCoreBucket: "crane_trace_core"
    TraceDetailBucket: "crane_trace_detail"
    TraceErrorBucket: "crane_trace_error"
    TraceShardBuckets: []
  TraceWriter:
    Shards: 4
    BatchSpans: 1024
    QueueBatches: 8192
    FlushIntervalMs: 50
    RetryBackoffMs: 200
    MaxRetryBackoffMs: 5000
```

## Runtime Control

Admins can inspect and update the runtime trace switch without restarting CraneCtld:

```bash
ccontrol show trace
ccontrol update trace enabled=true level=detailed
ccontrol update trace enabled=false
```

The change applies to CraneCtld immediately and is propagated to online Craned nodes by default. New supervisors inherit the current Craned trace config.

## Querying

Use AutoTest's pressure-test tool with the trace plugin config:

```bash
crane_press trace coverage --trace-config /etc/crane/trace.yaml --run-json output/run.json --json output/coverage.json
crane_press trace system --trace-config /etc/crane/trace.yaml --run-json output/run.json --json output/trace_system.json --html output/trace_system.html
```

For large runs, prefer `--run-json` so the query window is restricted to the current pressure test and old spans do not pollute the report.
