# 集群配置

本指南说明如何通过 `/etc/crane/config.yaml` 文件配置鹤思，以设置集群拓扑、分区和调度策略。

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

# 鹤思数据和日志的基础目录
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

### Gres配置

> 设备资源相关配置

定义 GPU、NPU 和其他加速器等通用资源：

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

**Gres 参数：**

- **name**：一般是资源类型如：`GPU`，`NPU`等
- **type**：一般是资源型号如：`A100`，`3090`等
- **DeviceFileRegex**: 资源对应的 /dev 目录下的设备文件，适用于一个物理设备对应一个设备文件的资源，**每个文件对应系统内的一个 Gres 资源**，支持 Regex 格式。常见设备对应设备文件。如 Nvidia、AMD、海光 DCU、昇腾等。
- **DeviceFileList**：适用于**一个物理设备对应多个 /dev 目录下设备文件的 Gres 资源**，每一组文件对应系统内的一个 Gres 资源，支持 Regex 格式。

DeviceFileRegex与DeviceFileList二选一，以上设备文件必须存在，**否则 Craned 启动时将报错退出**

- **EnvInjector**: 设备需要注入的环境变量

  -  可选值：对应环境变量

  - `nvidia`：`CUDA_VISIABLE_DEVICES`
  - `hip`：`HIP_VISIABLE_DEVICES`
  - `ascend`：`ASCEND_RT_VISIBLE_DEVICES`

- 常见厂商设备文件路径及相关配置

  - | 厂商        | 设备文件路径            | EnvInjector |
    | :---------- | :---------------------- | :---------- |
    | Nvidia      | /dev/nvidia0 ...        | nvidia      |
    | AMD/海光DCU | /dev/dri/renderer128... | hip         |
    | 昇腾        | /dev/davinci0 ...       | ascend      |
    | 天数智芯    | /dev/iluvatar0 ...      | nvidia      |

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


### Supervisor 配置

Supervisor 是鹤思的作业执行管理组件,负责在计算节点上监控和控制作业步骤（step）。

```yaml
Supervisor:
  # Supervisor 可执行文件路径
  Path: /usr/libexec/csupervisor
  
  # Supervisor 日志级别: trace, debug, info, warn, error
  DebugLevel: trace
  
  # 日志目录(相对于 CraneBaseDir)
  LogDir: supervisor
```

**Supervisor 参数:**

- **Path**: Supervisor 可执行文件的完整路径。默认路径为 `/usr/libexec/csupervisor`,通常在安装时已正确设置。
- **DebugLevel**: 控制 Supervisor 日志的详细程度。可选值包括 `trace`(最详细)、`debug`、`info`、`warn`、`error`(最简略)。生产环境建议使用 `info` 或 `warn`。
- **LogDir**: Supervisor 日志文件的存储目录,相对于 `CraneBaseDir` 配置项。日志文件对于诊断作业执行问题很有帮助。

!!! tip
    在排查作业执行问题时,可以临时将 `DebugLevel` 设置为 `debug` 或 `trace` 以获取更详细的日志信息。

## 容器支持

鹤思支持通过 CRI（容器运行时接口）在容器中运行作业：

```yaml
Container:
  # 启用容器支持（实验性）
  Enabled: false
  
  # 容器数据临时目录（相对于 CraneBaseDir）
  TempDir: supervisor/containers/
  
  # 容器运行时套接字路径
  RuntimeEndpoint: /run/containerd/containerd.sock
  
  # 镜像服务套接字路径（通常与 RuntimeEndpoint 相同）
  ImageEndpoint: /run/containerd/containerd.sock
```

!!! info "实验性功能"
    容器支持目前处于实验阶段，可能存在限制和问题。


**要求：**

- 在计算节点上安装兼容 CRI 的运行时（containerd 或 CRI-O）
- 具有适当权限可访问运行时套接字
- 容器镜像可用或可从 Registry 访问


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