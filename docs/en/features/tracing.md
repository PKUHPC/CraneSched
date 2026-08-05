# Distributed Tracing

CraneSched includes OpenTelemetry-based distributed tracing. It records fine-grained spans for the full job lifecycle, from submission to completion, so administrators can diagnose scheduler bottlenecks and job execution failures.

## Overview

The tracing system provides three independent tracing dimensions:

| Dimension | Description | Typical use case |
|-----------|-------------|------------------|
| **Submission path** (`submit/*`) | Covers the path from gRPC arrival through authentication, validation, enqueue, and scheduler waiting | Diagnose submission failures and validation latency |
| **Scheduling cycle** (`scheduling/*`) | Covers each scheduling loop, including node selection, resource validation, DB persistence, and RPC fanout | Analyze scheduler algorithm and persistence bottlenecks |
| **Execution lifecycle** (`job/*`, `step/*`) | Crosses CraneCtld, Craned, and Supervisor, covering allocation, prolog, spawn, execution, finish, epilog, and resource release | End-to-end failure tracing and slow-node diagnosis |

Two analysis modes are supported:

| Mode | Description |
|------|-------------|
| **Single-job analysis** (`--job-id`) | Shows the complete timeline of one job and pinpoints which stage was slow |
| **System analysis** (`--system`) | Aggregates performance metrics across all jobs, including P50/P95/P99, to locate system-wide bottlenecks |

Runtime trace level controls which spans are created:

| Level | Behavior |
|-------|----------|
| `basic` | Creates core lifecycle spans only |
| `detailed` | Creates core spans plus scheduler, status-change, cgroup, and task detail spans |
| `debug` | Creates all spans compiled into the binary |

The effective level is `min(runtime_level, CRANE_TRACE_COMPILED_MAX_LEVEL)`. Error bucket routing only applies to spans that were already created and exported. For example, a debug-only span is not created when the effective level is `basic`, even if that code path later records an error status.

## Requirements

- CraneSched must be built with `CRANE_ENABLE_TRACING=ON` (the debug preset enables it by default).
- `CRANE_TRACE_COMPILED_MAX_LEVEL=basic|detailed|debug` controls the maximum trace level compiled into the binary.
- InfluxDB 2.x is required as the time-series storage backend.
- cplugind and the independent `trace.so` plugin are required as the span collection pipeline.

The current implementation uses OpenTelemetry `SimpleSpanProcessor`. Spans are exported synchronously by the OpenTelemetry SDK into CraneSched's plugin client queue, and the plugin client sends `TraceHook` requests to cplugind.

## Deployment

### 1. Install InfluxDB 2.x

```bash
# RHEL / Rocky Linux 9
cat > /etc/yum.repos.d/influxdb.repo << 'EOF'
[influxdb]
name = InfluxDB Repository - RHEL
baseurl = https://repos.influxdata.com/rhel/9/x86_64/stable/
enabled = 1
gpgcheck = 1
gpgkey = https://repos.influxdata.com/influxdata-archive_compat.key
EOF

yum install -y influxdb2 influxdb2-cli
```

### 2. Initialize InfluxDB

```bash
systemctl enable --now influxdb

influx setup --force \
  --username crane \
  --password <your_password> \
  --org crane \
  --bucket crane_trace \
  --retention 168h \
  --token <your_token>
```

Parameter description:

| Parameter | Description |
|-----------|-------------|
| `--org` | InfluxDB organization name. It must match the CraneSched configuration. |
| `--bucket` | Bucket used to store trace data. |
| `--retention` | Data retention period. `168h` means seven days. |
| `--token` | API token. It must match the CraneSched configuration. |

### 3. Build cplugind and trace plugin

In the CraneSched-FrontEnd repository:

```bash
make build   # Build cplugind and CLI tools
make plugin  # Build monitor.so, trace.so, and other plugins
```

The generated artifacts are `build/bin/cplugind`, `build/plugin/monitor.so`, and `build/plugin/trace.so`.

### 4. Configuration

#### config.yaml

Enable tracing in the cluster configuration:

```yaml
Tracing:
  Enabled: true
  Level: debug
  ExecutionFlow:
    Enabled: false
    HeartbeatIntervalSeconds: 5
```

`Level` controls span creation. `basic` creates only core lifecycle spans, `detailed` additionally creates scheduler, status-change, cgroup, and task detail spans, and `debug` creates all spans compiled into the binary. The effective level is the lower value of runtime `Level` and the compiled maximum `CRANE_TRACE_COMPILED_MAX_LEVEL`.

Error bucket routing only applies to spans that have already been created and exported. If a debug-only span is not created at `basic` level, it will not be created later just because that code path records an error status.

### Execution-flow lifecycle points

Execution-flow instrumentation is an independent, test-oriented layer on top
of tracing. Build with both `CRANE_ENABLE_TRACING=ON` and
`CRANE_ENABLE_EXECUTION_FLOW=ON`; the `ci-debug` preset enables both. All other
presets leave execution-flow instrumentation disabled unless explicitly
requested. CMake rejects execution flow without tracing.

At runtime, `Tracing.ExecutionFlow.Enabled` controls `flow/v1/*` instant spans
and service pipeline heartbeats. A normal Batch job is instrumented only when
its submitted environment contains `CRANE_EXECUTION_FLOW_ID` with exactly 32
strictly lowercase hexadecimal characters. Uppercase values are rejected, not
normalized. The validated value is propagated to Craned and Supervisor. Array
and container jobs are excluded from the first contract version. The job must
be submitted with `--no-requeue`; requeue-enabled submissions, later requeue
attempts, and additional common/`crun` steps emit only an enumerated
`unsupported` diagnostic. Unsupported submissions do not propagate the flow
ID to Craned or Supervisor.

Flow spans are classified as core even at the `basic` trace level. They record
stable identifiers, enumerated state/outcome values, logical service identity,
process-unique service identity and a process-local event sequence; they never
record commands, full environment contents, credentials or arbitrary error
text. This facility observes the state machine only and never advances or
repairs job state.

The trace plugin stores ordinary distributed-tracing spans in the `spans`
measurement and execution-flow points in the schema-defined
`execution_flow_points` measurement. Both measurements use the configured
core shard buckets; execution-flow failures copied to the error bucket keep
the same measurement. Keeping the measurements separate prevents attributes
that intentionally have different wire types, such as `job_id`, from causing
InfluxDB field-type conflicts. Consumers must select the measurement for the
data model they query rather than treating the bucket as a single schema.

The plugin list is configured in the independent `plugin.yaml` file.

#### plugin.yaml

cplugind uses an independent plugin configuration file. The default path is `/etc/crane/plugin.yaml`:

```yaml
Enabled: true
PlugindSockPath: "cplugind/cplugind.sock"
PlugindDebugLevel: "trace"
TraceHookMaxRequestBytes: 3670016
Plugins:
  - Name: "monitor"
    Path: "/usr/local/lib/crane/plugin/monitor.so"
    Config: "/etc/crane/monitor.yaml"
  - Name: "trace"
    Path: "/usr/local/lib/crane/plugin/trace.so"
    Config: "/etc/crane/trace.yaml"
```

`PlugindSockPath` is relative to `CraneBaseDir`.

`monitor.so` and `trace.so` are intentionally separate. Resource monitoring uses `monitor.yaml`; trace collection uses `trace.yaml`. Old deployments that placed `TraceBucket`, `TraceShardBuckets`, or `TraceWriter` in `monitor.yaml` must move those fields to `trace.yaml`.

#### trace.yaml

The trace plugin reads InfluxDB connection and writer settings from `trace.yaml`:

```yaml
Tracing:
  LogPath: "/var/log/crane/trace.log"

Database:
  Type: "influxdb"
  Influxdb:
    Url: "http://<influxdb_host>:8086"
    Token: "<your_token>"
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

For multi-node deployments, worker nodes must use an `Url` that points to the host where InfluxDB is running.

### 5. Deploy to the cluster

Distribute the following files to all compute nodes:

- `/usr/local/bin/craned`, `/usr/libexec/csupervisor`, `/usr/local/bin/cfored`
- `/usr/local/bin/cplugind`
- `/usr/local/lib/crane/plugin/monitor.so`, `/usr/local/lib/crane/plugin/trace.so`
- `/etc/crane/config.yaml`, `/etc/crane/database.yaml`, `/etc/crane/plugin.yaml`, `/etc/crane/monitor.yaml`, `/etc/crane/trace.yaml`

### 6. Startup order

```bash
# 1. Start cplugind on all nodes. It must start before cranectld/craned.
systemctl start cplugind

# 2. Start cranectld on the control node.
systemctl start cranectld

# 3. Start craned on compute nodes.
systemctl start craned

# 4. Start cfored on compute nodes.
systemctl start cfored
```

cplugind must start before cranectld and craned. Otherwise, spans may be dropped while PluginClient is reconnecting.

## Runtime Control

Administrators can inspect and update the runtime trace switch without restarting CraneCtld:

```bash
ccontrol show trace
ccontrol update trace enabled=true level=detailed
ccontrol update trace enabled=false
```

The change applies to CraneCtld immediately and is propagated to online Craned nodes by default. New supervisors inherit the current Craned trace configuration.

## Querying Traces

Use `test/Trace/query_trace.py` to query spans:

```bash
export INFLUX_URL=http://localhost:8086
export INFLUX_TOKEN=<your_token>
export INFLUX_ORG=crane
export TRACE_BUCKET=crane_trace

# Query by Job ID
python3 query_trace.py --job-id 12345 -v

# Query the full path by trace_id
python3 query_trace.py --trace-id <hex_trace_id> -v

# Show all spans from the last 30 minutes
python3 query_trace.py --minutes 30
```

Install the Python dependency with `pip install influxdb-client`.

AutoTest can also generate run-scoped reports from `trace.yaml`:

```bash
crane_press trace coverage --trace-config /etc/crane/trace.yaml --run-json output/run.json --json output/coverage.json
crane_press trace system --trace-config /etc/crane/trace.yaml --run-json output/run.json --json output/trace_system.json --html output/trace_system.html
```

For large runs, prefer `--run-json` so the query window is restricted to the current pressure test and old spans do not pollute the report.

## Visualization

### Text Flame Graph

Use `--tree` to show a hierarchical trace tree. This makes stage latency and parent-child relationships easier to inspect:

```bash
python3 query_trace.py --job-id 12345 --tree -v
```

Example output:

```text
Trace c6630cb7b9f3b2d8  (Job #12345)
==========================================================================================
[CraneCtld] job/lifecycle                ####################       10.36s
    |-- [Craned@wrl04] job/alloc                 #                            65us
    |-- [Craned@wrl04] step/supervisor_spawn     #                        262.70ms
    |     step_type: 1
    |   |-- [Supervisor@wrl04] step/execute          #################           9.09s
    |   |     step_type: 2
    |   |     task_count: 1
    |   `-- [Supervisor@wrl04] step/finish           #                           130us
    |         exit_code: 0
    |-- [CraneCtld] job/rpc_execute              #                          4.85ms
    |-- [Craned@wrl04] step/rpc_receive          #                            35us
    `-- [CraneCtld] job/end                      #                           271us
```

Each row is a span. Indentation shows parent-child relationships. The `-v` option prints span attributes. `step_type` values are `1=daemon`, `2=primary`, and `3=common`.

### Chrome Trace / Perfetto UI

Export traces as [Chrome Trace Event Format](https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/) JSON, then open them in [Perfetto UI](https://ui.perfetto.dev) or `chrome://tracing`:

```bash
# Export a single job trace
python3 query_trace.py --job-id 12345 --chrome /tmp/trace.json

# Visualization options:
# 1. Open https://ui.perfetto.dev in a browser and drag in the JSON file.
# 2. Open chrome://tracing in Chrome and click Load.
```

Perfetto view features:

- **Rows by service**: each service, such as CraneCtld, Craned@wrl02, or Supervisor@wrl02, appears as a separate process row.
- **Lanes by step**: spans from the same step are placed on the same lane, such as `Step 0 (daemon)`, `Step 1 (primary)`, or `Step 2 (common)`.
- **Color by step type**: daemon, primary, and common steps use different color families.
- **Causal arrows**: `step/supervisor_spawn` to `step/execute` can be shown as a flow arrow.
- **Detail panel**: clicking a span shows attributes such as `job_id`, `step_id`, and `exit_code`.

## Bottleneck Diagnosis

Tracing tools support system-level aggregate analysis for locating scheduling bottlenecks under high load.

### System Analysis

Use `--system` to aggregate all spans in a time window:

```bash
# Analyze system performance from the last 10 minutes
python3 query_trace.py --system --minutes 10 --limit 10000
```

Example output:

```text
======================================================================
 CraneSched System Performance (1060 spans)
======================================================================

Throughput:
  Jobs seen:       30
  Completed:       30 (100.0%)
  Failed:          0 (0.0%)

Scheduling Cycle (scheduling/cycle):  n=1
  avg=61.37ms  P50=61.37ms  P95=61.37ms  P99=61.37ms  max=61.37ms
  |-- node_select               avg=    1.71ms  P99=    1.71ms  (n=1)
  |-- resource_validate         avg=    6.22ms  P99=    6.22ms  (n=1)
  |-- db_persist                avg=   25.81ms  P99=   25.81ms  (n=1)  <--
  |-- rpc_alloc_jobs            avg=    6.76ms  P99=    6.76ms  (n=1)
  |-- rpc_alloc_steps           avg=   19.04ms  P99=   19.04ms  (n=1)

Submit-to-Running (job/pending):
  avg=934ms  P50=935ms  P95=936ms  P99=936ms

RPC Latency by Node (job/rpc_execute):
  wrl02  avg=3.21ms  P99=3.21ms  (n=1)
  wrl03  avg=4.17ms  P99=4.17ms  (n=1)
  wrl04  avg=3.64ms  P99=3.64ms  (n=1)

Step Execute by Type:
  primary  avg=2.17s  P99=2.19s  (n=30)
```

The report includes:

| Metric | Description |
|--------|-------------|
| **Scheduling Cycle** | Scheduling-cycle latency breakdown, such as `node_select`, `resource_validate`, `db_persist`, and `rpc_alloc`. The `<--` marker highlights a possible bottleneck. |
| **Submit-to-Running** | Distribution of waiting time from submission to execution start. |
| **RPC Latency by Node** | RPC latency per compute node, useful for finding slow nodes. |
| **Step Execute by Type** | Execution time grouped by step type, including daemon, primary, and common. |

### System Chrome Trace

Export all jobs into one timeline to inspect system-wide behavior:

```bash
python3 query_trace.py --system --minutes 10 --limit 10000 --chrome /tmp/system.json
```

Unlike single-job mode, the system view organizes lanes by **functional module** instead of by step:

```text
CraneCtld:
  scheduling/cycle:    [====cycle====][====cycle====]   <- Is the scheduler saturated?
  job/rpc_execute:     [rpc][rpc][rpc][rpc]...          <- Is RPC fanout parallel enough?
  job/status_change:   [sc][sc][sc]...                  <- Is status-change handling slow?

Craned@wrl02:
  step/supervisor_spawn: [spawn][spawn][spawn]...       <- Is supervisor startup parallel enough?
  step/execute:          [=====execute=====][=====]...  <- Is execution capacity saturated?

Craned@wrl03:
  ...                                                   <- Is load balanced across nodes?
```

Typical analysis scenarios:

- **Scheduler saturation**: `scheduling/cycle` spans have little or no gap between them, meaning the scheduler is continuously busy.
- **RPC bottleneck**: `job/rpc_execute` spans are heavily queued or mostly serialized.
- **Node imbalance**: one node has a much denser `step/execute` timeline than other nodes.
- **Status-change blocking**: `job/status_change` spans occupy a large amount of time and delay later step scheduling.

### Pressure Testing

Use the built-in trace stress script to generate load and run analysis automatically:

```bash
# Submit 30 short single-node jobs
bash test/Trace/stress_test.sh 30 1

# Submit 20 two-node jobs
bash test/Trace/stress_test.sh 20 2
```

The script performs parallel submission, waits for completion, waits for span flush, runs `--system` analysis, and exports Chrome Trace output.

### Example Diagnosis Workflow

1. **Observe the symptom**: users report that jobs spend more time in queue.
2. **Run system analysis**: use `--system --minutes 30` and check whether `job/pending` P99 increased.
3. **Locate the bottleneck**: if `scheduling/cycle` shows high `db_persist` P99, the embedded DB is likely the bottleneck.
4. **Inspect nodes**: if `RPC Latency by Node` shows one node with high P99, check that node.
5. **Drill down into one job**: use `--job-id <slow_job_id> --tree -v` to inspect the exact abnormal stage.
6. **Confirm visually**: export `--chrome` and inspect the timeline in Perfetto.

## Data Pipeline

```text
CraneSched (CraneCtld / Craned / Supervisor)
  |  OpenTelemetry SDK (SimpleSpanProcessor)
  v
CraneSpanExporter  ->  PluginClient (gRPC, Unix socket)
  v
cplugind  ->  trace.so (TraceHook handler)
  v
InfluxDB 2.x
  | ordinary traces: measurement "spans"
  | execution flow:  measurement "execution_flow_points"
  v
query_trace.py / Grafana / custom queries
```
