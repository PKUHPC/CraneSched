# 分布式 Tracing

CraneSched 内置了基于 OpenTelemetry 的分布式链路追踪能力，能够对任务从提交到完成的全生命周期进行细粒度打点，帮助管理员定位调度瓶颈、诊断任务执行故障。

## 功能概览

Tracing 系统提供三个独立的追踪维度：

| 维度 | 说明 | 典型场景 |
|------|------|----------|
| **提交链路** (`submit/*`) | 覆盖任务从 gRPC 到达 → 认证 → 校验 → 入队 → ID 分配的全过程 | 排查提交失败原因、评估校验环节耗时 |
| **调度周期** (`scheduling/*`) | 覆盖每次调度循环的 NodeSelect、资源验证、DB 持久化、RPC 扇出 | 分析调度算法性能瓶颈 |
| **执行生命周期** (`job/*`, `step/*`) | 跨 CraneCtld → Craned → Supervisor 三个服务，覆盖 alloc → prolog → 执行 → epilog → 释放 → 提交的全链路 | 端到端故障回溯、定位慢节点 |

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
