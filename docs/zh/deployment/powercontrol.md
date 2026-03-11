# 节能模块部署

集群节点在大多数时候未被充分利用，空闲节点会造成能耗资源的浪费。通过采集一段时间内的任务与节点能耗数据，预估节点活跃趋势，自适应地执行开机 / 关机、睡眠 / 唤醒操作，从而达到节省能耗的目的。

本指南说明如何在鹤思集群中启用和配置节能模块。

!!! warning "注意事项"

    修改 `powerControl.yaml` 后，需要先通过 IPMI 命令将所有处于 sleep / off 状态的节点全部开启，然后再启动 CraneCtld 和 powerControl 插件。否则，这些未注册到 CraneCtld 的节点将无法被 powerControl 插件识别，后续也无法参与调度。主控节点的 cplugind 在启动时需要获取到所有 Craned 的注册信息。

## 环境准备

节能模块的主要应用场景包括：

1. 自适应开机 / 关机
2. 自适应睡眠 / 唤醒，再进入自适应开机 / 关机

不同功能依赖不同的软硬件条件，可根据实际需求选择性部署。

### 硬件要求

powerControl 插件通过 IPMI 远程关机和开机计算节点，通过 SSH 连接到计算节点执行睡眠操作，并基于 Wake-on-LAN 远程唤醒计算节点。

通常服务器级别的硬件才具备 IPMI 功能，需要配备 BMC（Baseboard Management Controller）。可通过以下命令检查节点是否具备 BMC。**所有 CraneCtld 和 Craned 节点都需要支持 IPMI。**

```bash
ipmitool lan print       # 查询 BMC 网口信息
lsmod | grep ipmi        # 检查系统是否加载 IPMI 相关模块（如 ipmi_si）
ls /dev/ipmi*             # 检查 IPMI 接口设备是否存在
```

Wake-on-LAN 用于唤醒指定网卡，需确认网卡已支持并开启该功能。**所有 Craned 节点都需要支持。**

```bash
ethtool eth0 | grep -i wake-on
```

### 安装依赖

#### InfluxDB

用于收集任务和节点能耗数据。可在独立节点部署，也可在 CraneCtld 节点部署。

##### 安装 InfluxDB 服务端

```bash
# 添加仓库
sudo tee /etc/yum.repos.d/influxdb.repo > /dev/null <<EOF
[influxdb]
name = InfluxDB Repository - RHEL
baseurl = https://repos.influxdata.com/rhel/9/\$basearch/stable
enabled = 1
gpgcheck = 1
gpgkey = https://repos.influxdata.com/influxdata-archive_compat.key
EOF

# 安装
sudo dnf install influxdb2 -y

# 设置开机自启动（可选）
systemctl enable --now influxdb
```

##### 安装客户端

```bash
wget https://dl.influxdata.com/influxdb/releases/influxdb2-client-2.7.5-linux-amd64.tar.gz

tar xvf influxdb2-client-2.7.5-linux-amd64.tar.gz
sudo cp influx /usr/local/bin/
which influx
influx version
```

##### 初始化并记录配置信息

初始化时需要设置并记录用户名、密码、**组织名（Org）** 和 **存储桶名（Bucket）** 等信息。

```bash
# 初始化
influx setup

# 验证安装
influx version
influx ping
```

获取 **Token**，用于后续在 `.yaml` 配置文件中填写：

```bash
influx auth list
```

#### IPMI

用于远程关机 / 开机节点，仅需在 CraneCtld 节点部署。

```bash
sudo dnf install ipmitool -y   # 安装
ipmitool -V                     # 验证
```

#### 节能数据模型

节能模块包含一个机器学习模型，用于预估未来一段时间内的节点活跃趋势。模型推理依赖 Python 环境和相关库，需在 CraneCtld 节点部署。

此部分暂未开源，请联系维护团队。

## 服务配置

CraneCtld 和 Craned 节点使用不同的插件，因此配置文件有所不同。

### CraneCtld 节点

#### plugin.yaml

编辑 `/etc/crane/plugin.yaml`，添加以下节能相关配置：

```yaml
Plugin:
  Enabled: true
  PlugindSockPath: "cplugind/cplugind.sock"
  PlugindDebugLevel: "trace"
  Plugins:
    - Name: "powerControl"
      Path: "/root/CraneSched-FrontEnd/build/plugin/powerControl.so"
      Config: "/etc/crane/powerControl.yaml"
```

#### powerControl.yaml

新增 `/etc/crane/powerControl.yaml`，参考内容如下：

??? example "powerControl.yaml 完整示例"

    ```yaml
    PowerControl:
      # 活跃节点数量预测器脚本路径
      PredictorScript: "/root/CraneSched-FrontEnd/plugin/powerControl/predictor/predictor.py"
      # 是否允许睡眠
      EnableSleep: false
      # 节点睡眠超出此阈值后执行关机（单位：秒）
      SleepTimeThresholdSeconds: 36000
      # 保留的空闲节点比例（相对于集群总节点数）
      IdleReserveRatio: 0.2
      # 执行电源控制操作的时间间隔（单位：秒）
      CheckIntervalSeconds: 1800
      # 节点状态检查间隔（单位：秒），用于检测节点是否已从临时状态转为稳定状态
      # 例如：craned01 被关机后状态变为 powering_off，程序每隔 30 秒检测该节点
      # 是否已完全关机，若已关机则将状态设为 powered_off
      NodeStateCheckIntervalSeconds: 30
      # 电源控制插件日志文件路径
      PowerControlLogFile: "/root/powerControlTest/log/powerControl.log"
      # 节点状态变更记录文件路径
      NodeStateChangeFile: "/root/powerControlTest/log/node_state_changes.csv"
      # 集群状态信息记录文件路径
      ClusterStateFile: "/root/powerControlTest/log/cluster_state.csv"

    Predictor:
      # 生产环境建议关闭，测试环境可开启
      Debug: false
      # 活跃节点数量预测器部署地址
      URL: "http://localhost:5000"
      # 预测器日志文件路径
      PredictorLogFile: "/root/powerControlTest/log/predictor.log"
      # 预测未来 30 分钟的活跃节点数量（模型固定值，无需修改）
      ForecastMinutes: 30
      # 获取过去 240 分钟的数据（模型固定值，无需修改）
      LookbackMinutes: 240
      # 模型检查点文件路径（可放置在 /etc/crane/model/ 目录下）
      CheckpointFile: "/root/powerControlTest/model/checkpoint.pth"
      # 数据缩放器文件路径
      ScalersFile: "/root/powerControlTest/model/dataset_scalers.pkl"

    InfluxDB:
      URL: "http://localhost:8086"
      Token: "<TOKEN>"
      Org: "<ORG>"
      # InfluxDB 中存储节点能耗数据的 Bucket 名称
      Bucket: "<BUCKET>"

    IPMI:
      # 节点 BMC 的用户名和密码（所有节点应保持一致）
      User: "energytest"
      Password: "fhA&%sfiFae7q4"
      # 每批次可操作的最大节点数
      PowerOffMaxNodesPerBatch: 10
      MaxNodesPerBatch: 10
      # 批次间隔时间（单位：秒）
      PowerOffBatchIntervalSeconds: 60
      BatchIntervalSeconds: 60
      # 排除在电源控制之外的节点
      ExcludeNodes:
        - "cninfer00"
      # Craned 节点与其 BMC IP 的映射
      NodeBMCMapping:
        cninfer05: "192.168.10.55"
        cninfer06: "192.168.10.56"
        cninfer07: "192.168.10.57"
        cninfer08: "192.168.10.58"
        cninfer09: "192.168.10.59"

    # 用于挂起（睡眠）Craned 节点（所有 Craned 节点的配置应保持一致）
    SSH:
      User: "root"
      Password: "hyq"
    ```

!!! note "配置说明"

    1. 节点下线后若空闲节点不足，提交的作业会直接失败（Failed），不会进入排队（Pending）状态。因此 `IdleReserveRatio` 不宜设置过低，建议不低于 `0.2`。
    2. `PowerControlLogFile` 和 `PredictorLogFile` 为调试日志；`NodeStateChangeFile` 和 `ClusterStateFile` 为 CSV 格式的记录文件，可用于数据分析。实际路径请根据运维需求调整。
    3. `SSH` 相关配置仅在开启睡眠功能时使用（CraneCtld 通过 SSH 连接到 Craned 执行睡眠命令）。当 `EnableSleep: false` 或已配置免密 SSH 时，无需填写此项。

### Craned 节点

#### plugin.yaml

编辑 `/etc/crane/plugin.yaml`，添加以下节能相关配置：

```yaml
Plugin:
  Enabled: true
  PlugindSockPath: "cplugind/cplugind.sock"
  PlugindDebugLevel: "trace"
  Plugins:
    - Name: "energy"
      Path: "/root/CraneSched-FrontEnd/build/plugin/energy.so"
      Config: "/etc/crane/energy.yaml"
```

#### energy.yaml

新增 `/etc/crane/energy.yaml`，参考内容如下：

??? example "energy.yaml 完整示例"

    ```yaml
    Monitor:
      # 采样时间间隔（跟随模型需求，目前固定为 60 秒，无需修改）
      SamplePeriod: 60s
      # 日志输出文件路径
      LogPath: "/var/crane/craned/energy.log"
      # GPU 类型（如节点配有 GPU，需配置此项以采集 GPU 能耗数据）
      GPUType: "nvidia"
      Enabled:
        # 开启作业能耗与负载监控
        Job: true
        # 开启 IPMI 能耗监控
        Ipmi: true
        # 开启 GPU 能耗与负载监控
        Gpu: true
        # 开启 RAPL 能耗监控（适用于 Intel CPU，也可仅开启 IPMI）
        Rapl: true
        # 开启节点系统负载监控（CPU、内存等）
        System: true

    Database:
      Type: "influxdb"
      Influxdb:
        Url: "http://<INFLUXDB_HOST>:8086"
        Token: "<TOKEN>"
        Org: "<ORG>"
        # InfluxDB 中存储节点能耗数据的 Bucket 名称
        NodeBucket: "<NODE_BUCKET>"
        # InfluxDB 中存储作业能耗数据的 Bucket 名称
        JobBucket: "<JOB_BUCKET>"
    ```

## 服务启动

### CraneCtld 节点

```bash
systemctl enable --now cranectld
```

#### CraneCtld 相关插件服务

详细参考 [插件指南](./frontend/plugins.md)。

!!! tip "环境变量配置"

    如果 cplugind 启动时 predictor 报错，需要在 `cplugind.service` 中补充环境变量：

    ```ini
    Environment=PYTHONPATH=/root/repo/CraneSched-powersaving-trainning:$PYTHONPATH
    ```

```bash
systemctl enable --now cplugind
```

### Craned 节点

!!! warning "自启动要求"

    Craned 节点的 `craned` 和 `cplugind` 服务都必须开启自启动（`enable`），否则宿主机重启后计算节点无法自动恢复组网和插件功能。

```bash
systemctl enable --now craned
```

#### Craned 相关插件服务

详细参考 [插件指南](./frontend/plugins.md)。

```bash
systemctl enable --now cplugind
```

#### 验证能耗数据写入

在 Craned 节点上验证 InfluxDB 的读写是否正常：

??? example "写入与查询 energy_node 数据"

    ```bash
    # 写入测试数据到 InfluxDB 的 energy_node Bucket
    curl -i -XPOST "http://<INFLUXDB_HOST>:8086/api/v2/write?org=<ORG>&bucket=energy_node&precision=s" \
      -H "Authorization: Token <TOKEN>" \
      --data-binary "test_node,node=cn01 value=123 $(date +%s)"

    # 从 energy_node Bucket 查询测试数据
    curl -s -XPOST "http://<INFLUXDB_HOST>:8086/api/v2/query?org=<ORG>" \
      -H "Authorization: Token <TOKEN>" \
      -H "Content-type: application/vnd.flux" \
      -d 'from(bucket:"energy_node")
          |> range(start: -5m)
          |> filter(fn: (r) => r._measurement == "test_node")'
    ```

??? example "写入与查询 energy_task 数据"

    ```bash
    # 写入测试数据到 InfluxDB 的 energy_task Bucket
    curl -i -XPOST "http://<INFLUXDB_HOST>:8086/api/v2/write?org=<ORG>&bucket=energy_task&precision=s" \
      -H "Authorization: Token <TOKEN>" \
      --data-binary "test_task,job=testjob value=456 $(date +%s)"

    # 从 energy_task Bucket 查询测试数据
    curl -s -XPOST "http://<INFLUXDB_HOST>:8086/api/v2/query?org=<ORG>" \
      -H "Authorization: Token <TOKEN>" \
      -H "Content-type: application/vnd.flux" \
      -d 'from(bucket:"energy_task")
          |> range(start: -5m)
          |> filter(fn: (r) => r._measurement == "test_task")'
    ```

## 使用说明

### 常用鹤思命令

节点状态支持自适应调整，也支持管理员通过命令行手动操作。

```bash
# 关机
ccontrol update nodeName=cninfer09 state=off
ccontrol update nodeName=cninfer05,cninfer06 state=off

# 开机
ccontrol update nodeName cninfer09 state on reason "power on"
ccontrol update nodeName cninfer05,cninfer06 state on reason "power on"

# 睡眠
ccontrol update nodeName=craned1 state sleep
ccontrol update nodeName=craned1,craned2 state sleep

# 唤醒
ccontrol update nodeName=craned1 state wake
ccontrol update nodeName=craned1,craned2 state wake
```

!!! note

    执行开机 / 关机操作后，需要等待 2–3 分钟生效。

通过以下命令查询节点状态：

```bash
cinfo
ccontrol show node
```

详细用法参考 [cinfo](../command/cinfo.md)。

### 常用 IPMI 命令

当节点出现异常且无法通过鹤思命令操作时，可直接使用 IPMI 命令执行开机或重启：

```bash
# 通过 IPMI 命令开机
ipmitool -I lanplus -H 10.129.227.189 -U ADMIN -P 'Admin@123' power on

# 通过 IPMI 命令重启
ipmitool -I lanplus -H 10.129.227.189 -U ADMIN -P 'Admin@123' power reset
```

!!! info "BMC 配置说明"

    上述命令中的 IP、用户名和密码对应节点 BMC 的连接信息。所有集群节点应保持相同的 BMC 用户名和密码，并与 `powerControl.yaml` 中 `IPMI` 段的配置一致。

    由于每个节点的 BMC IP 地址不同，需要在 `powerControl.yaml` 中手动配置每个节点主机名与 BMC IP 的映射关系（`NodeBMCMapping`），以便在执行开机 / 关机操作时定位到正确的 Craned 节点。
