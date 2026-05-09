# 分布式 Tracing

CraneSched 内置了基于 OpenTelemetry 的分布式链路追踪能力，能够对任务从提交到完成的全生命周期进行细粒度打点，帮助管理员定位调度瓶颈、诊断任务执行故障。

## 功能概览

Tracing 系统提供三个独立的追踪维度：

| 维度 | 说明 | 典型场景 |
|------|------|----------|
| **提交链路** (`submit/*`) | 覆盖任务从 gRPC 到达 → 认证 → 校验 → 入队 → 调度等待的全过程 | 排查提交失败原因、评估校验环节耗时 |
| **调度周期** (`scheduling/*`) | 覆盖每次调度循环的 NodeSelect、资源验证、DB 持久化、RPC 扇出 | 分析调度算法性能瓶颈 |
| **执行生命周期** (`job/*`, `step/*`) | 跨 CraneCtld → Craned → Supervisor 三个服务，覆盖 alloc → prolog → spawn → 执行 → finish → epilog → 释放的全链路 | 端到端故障回溯、定位慢节点 |

支持两种分析模式：

| 模式 | 说明 |
|------|------|
| **单任务分析** (`--job-id`) | 查看单个 job 的完整时间线，精确定位某个任务在哪个阶段慢 |
| **系统级分析** (`--system`) | 聚合统计所有任务的性能指标（P50/P95/P99），定位系统瓶颈 |

## 前置条件

- CraneSched 编译时需开启 `CRANE_ENABLE_TRACING`（debug preset 默认开启）
- 需要 InfluxDB 2.x 作为时序存储后端
- 需要 cplugind + monitor 插件作为 span 收集管道

## 部署步骤

### 1. 安装 InfluxDB 2.x

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

### 2. 初始化 InfluxDB

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

参数说明：

| 参数 | 说明 |
|------|------|
| `--org` | InfluxDB 组织名，后续配置需一致 |
| `--bucket` | 存储 trace 数据的 bucket |
| `--retention` | 数据保留时间（`168h` = 7 天） |
| `--token` | API 认证 token，后续配置需一致 |

### 3. 构建 cplugind 和 monitor 插件

在 CraneSched-FrontEnd 仓库中：

```bash
make build   # 构建 cplugind 及 CLI 工具
make plugin  # 构建 monitor.so 等插件
```

构建产物位于 `build/bin/cplugind` 和 `build/plugin/monitor.so`。

### 4. 配置

#### config.yaml

在集群配置文件中开启 Tracing 和 Plugin：

```yaml
Tracing:
  Enabled: true

Plugin:
  Enabled: true
  PlugindSockPath: "cplugind/cplugind.sock"
  PlugindDebugLevel: "trace"
  Plugins:
    - Name: "monitor"
      Path: "/usr/local/lib/crane/plugin/monitor.so"
      Config: "/etc/crane/monitor.yaml"
```

> `PlugindSockPath` 是相对于 `CraneBaseDir` 的路径。

#### plugin.yaml

cplugind 需要独立的插件配置文件（默认路径 `/etc/crane/plugin.yaml`）：

```yaml
Enabled: true
PlugindSockPath: "cplugind/cplugind.sock"
PlugindDebugLevel: "trace"
Plugins:
  - Name: "monitor"
    Path: "/usr/local/lib/crane/plugin/monitor.so"
    Config: "/etc/crane/monitor.yaml"
```

#### monitor.yaml

monitor 插件的 InfluxDB 连接配置：

```yaml
Monitor:
  SamplePeriod: 5s
  Enabled:
    Job: false
    Ipmi: false
    Gpu: false
    Rapl: false
    System: false
    Event: false

Database:
  Type: "influxdb"
  Influxdb:
    Url: "http://<influxdb_host>:8086"
    Token: "<your_token>"
    Org: "crane"
    NodeBucket: "crane_trace"
    JobBucket: "crane_trace"
    ClusterBucket: "crane_trace"
    TraceBucket: "crane_trace"
```

> 如果仅需 trace 功能，可将 Monitor.Enabled 下的各项设为 `false`。
> 多节点部署时，worker 节点的 `Url` 需指向 InfluxDB 所在主机的地址。

### 5. 部署到集群

将以下文件分发到所有计算节点：

- `/usr/local/bin/craned`、`/usr/libexec/csupervisor`、`/usr/local/bin/cfored`
- `/usr/local/bin/cplugind`
- `/usr/local/lib/crane/plugin/monitor.so`
- `/etc/crane/config.yaml`、`/etc/crane/database.yaml`、`/etc/crane/plugin.yaml`、`/etc/crane/monitor.yaml`

### 6. 启动顺序

```bash
# 1. 所有节点启动 cplugind（必须先于 cranectld/craned）
systemctl start cplugind

# 2. 控制节点启动 cranectld
systemctl start cranectld

# 3. 计算节点启动 craned
systemctl start craned

# 4. 计算节点启动 cfored
systemctl start cfored
```

> cplugind 必须先于 cranectld/craned 启动，否则 span 导出会在 PluginClient 重连期间丢失。

## 查询 Trace

使用 `test/Trace/query_trace.py` 脚本查询：

```bash
export INFLUX_URL=http://localhost:8086
export INFLUX_TOKEN=<your_token>
export INFLUX_ORG=crane
export TRACE_BUCKET=crane_trace

# 按 Job ID 查询
python3 query_trace.py --job-id 12345 -v

# 按 trace_id 查询完整链路
python3 query_trace.py --trace-id <hex_trace_id> -v

# 查看最近 30 分钟的所有 span
python3 query_trace.py --minutes 30
```

依赖安装：`pip install influxdb-client`

## 可视化

### 文本火焰图

使用 `--tree` 选项以树形层级展示 trace，直观查看各阶段耗时和 parent-child 关系：

```bash
python3 query_trace.py --job-id 12345 --tree -v
```

输出示例：

```
Trace c6630cb7b9f3b2d8  (Job #12345)
==========================================================================================
[CraneCtld] job/lifecycle                ████████████████████       10.36s
    ├── [Craned@wrl04] job/alloc                 █                            65us
    ├── [Craned@wrl04] step/supervisor_spawn     █                        262.70ms
    │     step_type: 1
    │   ├── [Supervisor@wrl04] step/execute          █████████████████           9.09s
    │   │     step_type: 2
    │   │     task_count: 1
    │   └── [Supervisor@wrl04] step/finish           █                           130us
    │         exit_code: 0
    ├── [CraneCtld] job/rpc_execute              █                          4.85ms
    ├── [Craned@wrl04] step/rpc_receive          █                            35us
    └── [CraneCtld] job/end                      █                           271us
```

树中每一行表示一个 span，缩进表示 parent-child 关系。`-v` 选项显示 span 属性。`step_type` 取值：1=daemon, 2=primary, 3=common。

### Chrome Trace / Perfetto UI

导出为 [Chrome Trace Event Format](https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/) JSON 文件，可在 [Perfetto UI](https://ui.perfetto.dev) 或 `chrome://tracing` 中查看交互式火焰图：

```bash
# 导出单任务 trace
python3 query_trace.py --job-id 12345 --chrome /tmp/trace.json

# 打开可视化
# 方式1: 浏览器访问 https://ui.perfetto.dev，拖入 JSON 文件
# 方式2: Chrome 浏览器访问 chrome://tracing，点击 Load 按钮
```

Perfetto 视图特性：

- **按服务分行**：每个服务（CraneCtld, Craned@wrl02, Supervisor@wrl02）显示为独立的 Process 行
- **按 Step 分 Lane**：同一个 Step 的所有 span 在同一行，标注 "Step 0 (daemon)"、"Step 1 (primary)"、"Step 2 (common)"
- **颜色区分**：不同 step_type 使用不同色系（daemon / primary / common）
- **因果箭头**：`step/supervisor_spawn` → `step/execute` 之间有 flow arrow 标注因果关系
- **点击查看详情**：点击任意 span 可查看 job_id、step_id、exit_code 等属性

## 性能瓶颈定位

Tracing 工具支持系统级聚合分析，用于在高负载场景下定位调度瓶颈。

### 系统级分析

使用 `--system` 模式对时间窗口内的所有 span 进行统计分析：

```bash
# 分析过去 10 分钟的系统性能
python3 query_trace.py --system --minutes 10 --limit 10000
```

输出示例：

```
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

报告包含：

| 指标 | 说明 |
|------|------|
| **Scheduling Cycle** | 调度周期耗时分解（node_select / resource_validate / db_persist / rpc_alloc），`<--` 标记潜在瓶颈 |
| **Submit-to-Running** | 从提交到开始执行的等待时间分布 |
| **RPC Latency by Node** | 各计算节点的 RPC 延迟，可发现慢节点 |
| **Step Execute by Type** | 按 step 类型（daemon/primary/common）分组的执行时间 |

### 系统级 Chrome Trace

将所有任务叠加在同一时间轴上，查看系统整体行为：

```bash
python3 query_trace.py --system --minutes 10 --limit 10000 --chrome /tmp/system.json
```

与单任务模式不同，系统级视图按**功能模块**组织 Lane（而非按 step）：

```
CraneCtld:
  scheduling/cycle:    [====cycle====][====cycle====]   ← 调度器是否饱和？
  job/rpc_execute:     [rpc][rpc][rpc][rpc]...          ← RPC 扇出并行度？
  job/status_change:   [sc][sc][sc]...                  ← 状态变更处理耗时？

Craned@wrl02:
  step/supervisor_spawn: [spawn][spawn][spawn]...       ← Supervisor 启动并行度？
  step/execute:          [=====execute=====][=====]...  ← 执行阶段利用率？

Craned@wrl03:
  ...                                                    ← 节点间负载均衡？
```

典型分析场景：

- **调度器饱和**：`scheduling/cycle` 之间无间隙，说明调度器满负载运行
- **RPC 瓶颈**：`job/rpc_execute` 排队严重（串行而非并行）
- **节点不均衡**：某节点的 `step/execute` 密度明显高于其他节点
- **状态变更阻塞**：`job/status_change` 占据大量时间，阻塞后续 step 调度

### 压力测试

使用内置压测脚本生成高负载数据并自动分析：

```bash
# 提交 30 个单节点短任务
bash test/Trace/stress_test.sh 30 1

# 提交 20 个双节点任务
bash test/Trace/stress_test.sh 20 2
```

脚本自动完成：并行提交 → 等待完成 → 等待 span flush → `--system` 统计分析 → 导出 Chrome Trace。

### 排查流程示例

1. **发现问题**：用户反馈"任务排队时间变长"
2. **系统级分析**：`--system --minutes 30` 查看 `job/pending` P99 是否升高
3. **定位瓶颈**：如果 `scheduling/cycle` 的 `db_persist` P99 明显偏高 → DB 是瓶颈
4. **节点排查**：`RPC Latency by Node` 显示某节点 P99 偏高 → 检查该节点
5. **单任务深入**：`--job-id <慢任务ID> --tree -v` 精确查看哪个阶段异常
6. **可视化确认**：`--chrome` 导出到 Perfetto，在时间轴上直观确认

## 数据管道架构

```
CraneSched (CraneCtld / Craned / Supervisor)
  │  OpenTelemetry SDK (BatchSpanProcessor, 5s / 512 batch)
  ▼
CraneSpanExporter  →  PluginClient (gRPC, Unix socket)
  ▼
cplugind  →  monitor.so (TraceHook handler)
  ▼
InfluxDB 2.x  (measurement: "spans")
  ▼
query_trace.py / Grafana / 自定义查询
```
