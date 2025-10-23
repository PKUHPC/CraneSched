# 集群配置

本指南说明如何通过 `/etc/crane/config.yaml` 文件配置 CraneSched，以设置集群拓扑、分区和调度策略。

!!! info
    配置文件在所有节点（控制节点和计算节点）上必须**完全相同**。任何更改都需要重启受影响的服务。

## 快速开始示例

一个 4 节点集群的最小配置：

```yaml
# 集群标识
ControlMachine: crane01
ClusterName: my_cluster

# 数据库配置
DbConfigPath: /etc/crane/database.yaml

# 节点定义
Nodes:
  - name: "crane[01-04]"
    cpu: 4
    memory: 8G

# 分区定义
Partitions:
  - name: compute
    nodes: "crane[01-04]"
    priority: 5

DefaultPartition: compute
```

## 基本配置

### 集群设置

定义基本集群信息：

```yaml
# 运行 cranectld 的节点的主机名（控制节点）
ControlMachine: crane01

# 此集群的名称
ClusterName: my_cluster

# 数据库配置文件路径
DbConfigPath: /etc/crane/database.yaml

# CraneSched 数据和日志的基础目录
CraneBaseDir: /var/crane/
```

- **ControlMachine**：必须是控制节点的实际主机名
- **ClusterName**：在多集群环境中用于标识
- **CraneBaseDir**：所有相对路径基于此目录

### 节点定义

指定计算节点资源：

```yaml
Nodes:
  # 节点范围表示法
  - name: "crane[01-04]"
    cpu: 4
    memory: 8G

  # 单个节点
  - name: "crane05"
    cpu: 8
    memory: 16G

  # 带有 GPU 的节点
  - name: "crane[06-07]"
    cpu: 8
    memory: 32G
    gres:
      - name: gpu
        type: a100
        DeviceFileRegex: /dev/nvidia[0-3]
```

**节点参数：**

- **name**：主机名或范围（例如，`node[01-10]`）
- **cpu**：CPU 核心数
- **memory**：总内存（支持 K、M、G、T 后缀）
- **gres**：通用资源，如 GPU（可选）

**节点范围表示法：**

- `crane[01-04]` 展开为：crane01、crane02、crane03、crane04
- `cn[1-3,5]` 展开为：cn1、cn2、cn3、cn5

### 分区配置

将节点组织到分区中：

```yaml
Partitions:
  # CPU 分区
  - name: CPU
    nodes: "crane[01-04]"
    priority: 5

  # GPU 分区  
  - name: GPU
    nodes: "crane[05-08]"
    priority: 3
    DefaultMemPerCpu: 4096  # 每个 CPU 4GB（以 MB 为单位）
    MaxMemPerCpu: 8192      # 每个 CPU 最大 8GB

# 作业提交的默认分区
DefaultPartition: CPU
```

**分区参数：**

- **name**：分区标识符
- **nodes**：属于此分区的节点范围
- **priority**：值越高优先级越高（影响调度）
- **DefaultMemPerCpu**：每个 CPU 的默认内存（MB）（0 = 让调度器决定）
- **MaxMemPerCpu**：每个 CPU 的最大内存（MB）（0 = 无限制）

### 调度策略

配置作业调度行为：

```yaml
# 调度算法
# 选项：priority/basic, priority/multifactor
PriorityType: priority/multifactor

# 在调度中优先考虑较小的作业
PriorityFavorSmall: true

# 优先级计算的最大时限（天-小时格式）
PriorityMaxAge: 14-0

# 优先级因子权重
PriorityWeightAge: 500           # 作业等待时间权重
PriorityWeightFairShare: 10000   # 公平份额权重
PriorityWeightJobSize: 0         # 作业大小权重（0=禁用）
PriorityWeightPartition: 1000    # 分区优先级权重
PriorityWeightQoS: 1000000       # QoS 优先级权重
```

## 网络设置

### 控制节点（cranectld）

```yaml
# cranectld 的监听地址和端口
CraneCtldListenAddr: 0.0.0.0
CraneCtldListenPort: 10011
CraneCtldForInternalListenPort: 10013
```

### 计算节点（craned）

```yaml
# craned 的监听地址和端口
CranedListenAddr: 0.0.0.0
CranedListenPort: 10010

# 健康检查设置
Craned:
  PingInterval: 15        # 每 15 秒 ping cranectld
  CraneCtldTimeout: 5     # cranectld 连接超时时间
```

## 高级选项

### TLS 加密

启用节点之间的加密通信：

```yaml
TLS:
  Enabled: true
  InternalCertFilePath: /etc/crane/tls/internal.pem
  InternalKeyFilePath: /etc/crane/tls/internal.key
  ExternalCertFilePath: /etc/crane/tls/external.pem
  ExternalKeyFilePath: /etc/crane/tls/external.key
  CaFilePath: /etc/crane/tls/ca.pem
  AllowedNodes: "crane[01-10]"
  DomainSuffix: crane.local
```

### GPU 配置

使用设备控制定义 GPU 资源：

```yaml
Nodes:
  - name: "gpu[01-02]"
    cpu: 16
    memory: 64G
    gres:
      - name: gpu
        type: a100
        # 匹配设备文件的正则表达式
        DeviceFileRegex: /dev/nvidia[0-3]
        # 每个 GPU 的附加设备文件
        DeviceFileList:
          - /dev/dri/renderer[0-3]
        # 运行时的环境注入器
        EnvInjector: nvidia
```

### 队列限制

控制作业队列大小和调度行为：

```yaml
# 队列中的最大挂起作业数（最大值：900000）
PendingQueueMaxSize: 900000

# 每个周期调度的作业数（最大值：200000）
ScheduledBatchSize: 100000

# 当队列已满时拒绝作业
RejectJobsBeyondCapacity: false
```

### 分区访问控制

按账户限制分区访问：

```yaml
Partitions:
  - name: restricted
    nodes: "special[01-04]"
    priority: 10
    # 只有这些账户可以使用此分区
    AllowedAccounts: project1,project2
    
  - name: public
    nodes: "compute[01-20]"
    priority: 5
    # 除这些账户外所有账户都可以使用此分区
    DeniedAccounts: banned_account
```

!!! warning
    `AllowedAccounts` 和 `DeniedAccounts` 互斥。如果设置了 `AllowedAccounts`，则忽略 `DeniedAccounts`。

### 日志和调试

配置日志级别和位置：

```yaml
# 日志级别：trace、debug、info、warn、error
CraneCtldDebugLevel: info
CranedDebugLevel: info

# 日志文件路径（相对于 CraneBaseDir）
CraneCtldLogFile: cranectld/cranectld.log
CranedLogFile: craned/craned.log

# 在前台运行（对调试有用）
CraneCtldForeground: false
CranedForeground: false
```

## 应用更改

修改配置后：

1. **分发到所有节点**：
   ```bash
   # 使用 pdsh
   pdcp -w crane[01-04] /etc/crane/config.yaml /etc/crane/
   ```

2. **重启服务**：
   ```bash
   # 控制节点
   systemctl restart cranectld
   
   # 计算节点
   pdsh -w crane[01-04] systemctl restart craned
   ```

3. **验证更改**：
   ```bash
   cinfo  # 检查节点和分区状态
   ```

## 故障排除

**节点未显示**：检查 ControlMachine 主机名是否与实际控制节点主机名匹配。

**配置不匹配警告**：确保所有节点上的 `/etc/crane/config.yaml` 完全相同。

**作业未调度**：验证分区配置和节点成员关系。

**资源限制**：检查请求的资源是否超出节点定义。
