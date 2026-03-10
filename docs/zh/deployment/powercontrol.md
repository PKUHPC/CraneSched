# 节能模块部署
集群节点大多时候跑不满，有时会有不少空闲节点，造成能耗资源的浪费。通过收集一段时间内任务和节点能耗情况，预估节点活跃情况，自适应的开机/关机、睡眠/唤醒，达到节省能耗的目的。  

本指南说明如何在鹤思集群中启用和配置节能模块。

## 注意事项
改动powerControl.yaml后，需要先基于ipmi命令让所有sleep/off节点全部开启。否则直接启动ctld和powerControl插件，会由于sleep/off节点没有向ctld注册，导致powerControl插件不识别，后续也无法进行相关调度操作。需要让主控节点的cplugind在启动时，获取到所有craned的注册信息。

## 环境准备
节能模块的主要应用场景包括：

1. 自适应开机/关机  
2. 自适应先进入睡眠/唤醒，再进入自适应开机/关机  

不同功能依赖不同的软硬件条件，可根据实际需求安装部署。

### 硬件判断
powerControl插件通过IPMI远程关机和开机计算节点，通过ssh连接到计算节点睡眠该节点，基于wakeonlan远程唤醒计算节点

通常服务器级别才有IPMI，支持IPMI需要有BMC，所以查看机器是否有BMC即可。**所有ctld，craned节点都需要支持。**
```bash
ipmitool lan print    ##查询BMC网口信息
lsmod | grep ipmi  ## 查看系统是否支持IPMI相关协议，例如ipmi_si
ls /dev/ipmi*    ##查看IPMI 接口设备是否存在
```

wakeonlan针对的是某个网卡的唤醒，查看网卡是否支持和开启wakeonlan。**所有craned节点都需要支持。**
```bash
ethtool eth0 | grep -i wake-on
```

### 安装依赖

#### influxDB
用于收集任务和节点能耗数据。可在独立节点部署，或在ctld节点部署。

##### influxDB安装
```bash
## 添加仓库
sudo tee /etc/yum.repos.d/influxdb.repo > /dev/null <<EOF
[influxdb]
name = InfluxDB Repository - RHEL
baseurl = https://repos.influxdata.com/rhel/9/\$basearch/stable
enabled = 1
gpgcheck = 1
gpgkey = https://repos.influxdata.com/influxdata-archive_compat.key
EOF

## 安装
sudo dnf install influxdb2 -y

## 开机自启动，可选
systemctl enable --now influxdb 
```

##### 客户端安装
```bash
wget https://dl.influxdata.com/influxdb/releases/influxdb2-client-2.7.5-linux-amd64.tar.gz

tar xvf influxdb2-client-2.7.5-linux-amd64.tar.gz
sudo cp influx /usr/local/bin/
which influx
influx version
```

##### 初始化并记录配置信息
初始化，设置并记录用户名，密码，**组织EXAMPLE_ORG，存储桶EXAMPLE_BUCKET**等信息
```bash
## 初始化
influx setup

## 验证安装版本
influx version
influx ping
```

获取**EXAMPLE_TOKEN**，用于后续配置.yaml
```bash
influx auth list
```

#### IPMI
用于远程关机/开机节点。仅需在ctld节点部署

```bash
sudo dnf install ipmitool -y  ##安装

ipmitool -V  ## 验证
```

#### 节能数据训练模型
部署在ctld节点。

拉取仓库: [CraneSched-powersaving-trainning](https://github.com/PKUHPC/CraneSched-powersaving-trainning) 仓库中trainer包括数据获取与处理、训练器、评估器三个部分。数据是从数据库中读取的计算中心集群数据，经过数据处理器处理成标签和特征形式的训练数据和验证数据，训练数据喂给训练器训练模型，验证数据喂给评估器评估模型效果。  

预估器predictor安装并启动，部署时会用到仓库中的model和predictor两个文件夹中的内容，model有模型，predictor那有启动预测服务的脚本，这两个相关路径直接配置在下述PowerControl.yaml中，预测服务predictor会在PowerControl插件加载时自动启动。  

predictor的启动需要安装torch环境和一些python依赖包，参考[README.md](https://github.com/PKUHPC/CraneSched-powersaving-trainning/blob/main/README.md) 

## 服务配置
ctld和craned节点使用不同插件，因此配置文件会有不同

### cranectld节点

#### config.yaml
编辑 `/etc/crane/config.yaml`，添加以下节能配置：
```yaml
Plugin:
  # Toggle the plugin module in CraneSched
  Enabled: true
  # Relative to CraneBaseDir
  PlugindSockPath: "cplugind/cplugind.sock"
  # Debug level of Plugind
  PlugindDebugLevel: "trace"
  # Plugins to be loaded in Plugind
  Plugins:
    - Name: "powerControl"
      Path: "/root/CraneSched-FrontEnd/build/plugin/powerControl.so"
      Config: "/etc/crane/powerControl.yaml"
```

#### powerControl.yaml
新增`/etc/crane/powerControl.yaml`，内容参考如下：

```yaml
PowerControl:
  # 活跃节点数量预测器的脚本的位置
  PredictorScript: "/root/CraneSched-FrontEnd/plugin/powerControl/predictor/predictor.py"
  # 是否允许睡眠
  EnableSleep: false
  # 节点睡眠超出这个阈值后就进行关机
  SleepTimeThresholdSeconds: 36000
  # 保留的空闲节点比例（相较于集群总节点个数） 
  IdleReserveRatio: 0.2
  # 每次执行电源控制的时间间隔 
  CheckIntervalSeconds: 1800
  # 检点状态检查的间隔时间，主要用于节点由稳定态转成临时态后，检测其是否变成了稳定态
  # 如craned01被关机了，则节点状态变为正在关机，程序会每隔30s检测下这个节点是否已经彻底关机
  # 如果关机则将craned01状态设为关机状态
  NodeStateCheckIntervalSeconds: 30
  # 电源控制插件的日志记录位置
  PowerControlLogFile: "/root/powerControlTest/log/powerControl.log"
  # 记录节点的每次状态变更文件的位置
  NodeStateChangeFile: "/root/powerControlTest/log/node_state_changes.csv"
  # 记录集群状态信息的的文件的位置
  ClusterStateFile: "/root/powerControlTest/log/cluster_state.csv"

Predictor:
  # 生产环境默认关，测试环境可以开
  Debug: false
  # 活跃节点数量预测器的部署地址
  URL: "http://localhost:5000"
  # 预测器的日志输出位置
  PredictorLogFile: "/root/powerControlTest/log/predictor.log"
  # 用于指定预测未来30分钟的活跃节点数量 （模型指定数值，不需要改动）
  ForecastMinutes: 30
  # 用于获取过去240分钟的数据（模型指定数值，不需要改动） 
  LookbackMinutes: 240
  # 模型仓库 model 文件夹下的两个模型文件，实际部署指定模型存储的下载位置就可，可以放到/etc/crane/model文件夹下
  CheckpointFile: "/root/powerControlTest/model/checkpoint.pth"
  # 数据scaler的位置
  ScalersFile: "/root/powerControlTest/model/dataset_scalers.pkl"

# 用于获取craned端能耗收集插件搜集的数据
InfluxDB:
  URL: "http://localhost:8086"
  Token: "EXAMPLE_TOKEN"
  Org: "EXAMPLE_ORG"
  # influxDB的表名，存储节点的能耗数据表
  # bucket相等于表，这里就是对应的influxdb中的表名，例如energy_node，org指组织，例如pku
  Bucket: "EXAMPLE_BUCKET"

IPMI:
  # 节点的对应的BMC的用户名和密码，所有节点的配置应该相同
  User: "energytest"
  # 每批次可以操作的最大节点个数
  Password: "fhA&%sfiFae7q4"
  # 每批次可以操作的最大节点个数
  PowerOffMaxNodesPerBatch: 10
  MaxNodesPerBatch: 10
  # 每次操作的间隔时间 
  PowerOffBatchIntervalSeconds: 60
  BatchIntervalSeconds: 60
  # 不进行电源控制的节点 
  ExcludeNodes:
    - "cninfer00"
  # craned与其BMC IP映射
  NodeBMCMapping:
    cninfer05: "192.168.10.55"
    cninfer06: "192.168.10.56"
    cninfer07: "192.168.10.57"
    cninfer08: "192.168.10.58"
    cninfer09: "192.168.10.59"
# 用于挂起（睡眠）craned，所有craned这里的配置应该一样
SSH:
  User: "root"
  Password: "hyq"
```
**注意**：  
1. 节点下线后，idle节点不足，提交的作业会直接failed，不会进入pending。因此powerControl.yaml中IdleReserveRatio值不要配置太低，建议0.2以上  
2. PowerControlLogFile，PredictorLogFile两个log文件是日志文件，调试时使用。NodeStateChangeFile和ClusterStateFile两个csv文件是记录文件，可以用于数据分析。实际路径根据运维需求调整。  
3. SSH相关配置用于开启睡眠功能时，ctld ssh连接到craned执行睡眠命令。如果不需要睡眠，即EnableSleep=false时，或者宿主节点已配置免密SSH，则不需要配置。

### craned节点

#### config.yaml
编辑 `/etc/crane/config.yaml`，添加以下节能配置：
```yaml
Plugin:
  # Toggle the plugin module in CraneSched
  Enabled: true
  # Relative to CraneBaseDir
  PlugindSockPath: "cplugind/cplugind.sock"
  # Debug level of Plugind
  PlugindDebugLevel: "trace"
  # Plugins to be loaded in Plugind
  Plugins:
    - Name: "energy"
      Path: "/root/CraneSched-FrontEnd/build/plugin/energy.so"
      Config: "/etc/crane/energy.yaml"
```

#### energy.yaml
新增`/etc/crane/energy.yaml`，内容参考如下：

```yaml
Monitor:
  # 采样的时间间隔，其跟随模型需求，不需要改动，目前固定为60s
  SamplePeriod: 60s 
  # 日志输出文件位置
  LogPath: "/var/crane/craned/energy.log"
  # 如果节点有GPU，则搜集GPU相关能耗时需要配置GPU类型
  GPUType: "nvidia"
  Enabled:
    # 开启任务的能耗监控和负载监控
    Job: true
    # 开启使用IPMI来监控能耗
    Ipmi: true
    # 开启GPU的能耗监控和负载监控
    Gpu: true
    # 开启使用RAPL来监控能耗，用于intel的cpu，也可以只开启IPMI
    Rapl: true
    # 开启节点的系统负载相关的监控，cpu、内存等
    System: true

Database:
  Type: "influxdb"            
  Influxdb:
    Url: "http://192.168.11.109:8086"
    Token: "EXAMPLE_TOKEN"
    Org: "EXAMPLE_ORG"
    # influxDB的表名，存储节点的能耗数据表energy_node
    NodeBucket: "EXAMPLE_BUCKET1"
    # influxDB的表名，存储job能耗数据的表energy_task
    JobBucket: "EXAMPLE_BUCKET2"
```

## 服务启动

### cranectld节点
```bash
## 启动ctld服务
systemctl start cranectld
```

#### cranectld相关插件服务
详细参考**[插件指南](./frontend/plugins.md)**  
如果cplugind启动时，predictor启动报错，则需要在cplugind.service中补充环境变量，参考
```service
Environment=PYTHONPATH=/root/repo/CraneSched-powersaving-trainning:$PYTHONPATH
```

```bash
## 启动插件服务
systemctl enable cplugind
```

### craned节点
```bash
## 启动ctld服务
systemctl start craned
## 必须开启自启动，否则调度宿主机开机后，计算节点无法自启动并恢复组网
systemctl enable craned
```
#### craned相关插件服务
详细参考**[插件指南](./frontend/plugins.md)**

```bash
## 启动插件服务
systemctl start cplugind
## 必须开启自启动，否则调度宿主机开机后，计算节点的插件无法自启动
systemctl enable cplugind
```

#### 验证能耗数据写入
需要在craned节点，验证往influxDB读写
```bash
## 尝试写入数据123到influxDB的energy_node表
curl -i -XPOST "http://192.168.234.1:8086/api/v2/write?org=pku&bucket=energy_node&precision=s" \
  -H "Authorization: Token EXAMPLE_TOKEN" \
  --data-binary "test_node,node=cn01 value=123 $(date +%s)"
  
## 查询数据123从influxDB的energy_node表
curl -s -XPOST "http://192.168.234.1:8086/api/v2/query?org=pku" \
  -H "Authorization: Token EXAMPLE_TOKEN" \
  -H "Content-type: application/vnd.flux" \
  -d 'from(bucket:"energy_node")
      |> range(start: -5m)
      |> filter(fn: (r) => r._measurement == "test_node")'

```

```bash
## 尝试写入数据456到influxDB的energy_task表
curl -i -XPOST "http://192.168.234.1:8086/api/v2/write?org=pku&bucket=energy_task&precision=s" \
  -H "Authorization: Token EXAMPLE_TOKEN" \
  --data-binary "test_task,job=testjob value=456 $(date +%s)"

## 查询数据456从influxDB的energy_task表
curl -s -XPOST "http://192.168.234.1:8086/api/v2/query?org=pku" \
  -H "Authorization: Token EXAMPLE_TOKEN" \
  -H "Content-type: application/vnd.flux" \
  -d 'from(bucket:"energy_task")
      |> range(start: -5m)
      |> filter(fn: (r) => r._measurement == "test_task")'
```

## 使用说明

### 常用鹤思命令
节点状态支持自适应，也支持管理员手动命令行操作

```bash
ccontrol update nodeName=cninfer09 state=off    ## 节点关机
ccontrol update nodeName=cninfer05,cninfer06 state=off     ## 关机多节点

ccontrol update nodeName cninfer09 state on reason "power on"    ## 节点开机
ccontrol update nodeName cninfer05,cninfer06 state on reason "power on"    ## 开机多节点

ccontrol update nodeName=craned1 state sleep  ## 节点睡眠
ccontrol update nodeName=craned1,craned2 state sleep  ## 多节点睡眠

ccontrol update nodeName=craned1 state wake  ## 节点唤醒
ccontrol update nodeName=craned1,craned2 state wake  ## 多节点唤醒
```
**注意**：on/off执行开关机时，需要等待2-3分钟生效

可以通过以下命令查询节点状态
```bash
cinfo
ccontrol show node
```
详细可参考[cinfo](../command/cinfo.md)

### 常用ipmi命令
当出现节点异常，且无法被操作时，可直接通过ipmi命令对节点开机或关机。
```bash
ipmitool -I lanplus -H 10.129.227.189 -U ADMIN -P 'Admin@123' power on ## 通过ipmi命令直接开机

ipmitool -I lanplus -H 10.129.227.189 -U ADMIN -P 'Admin@123' power reset  ## 通过ipmi命令直接重启节点
```
这里的ip，user和password就是节点对应BMC的IP，用户名和密码，只能运维配置，集群机器保持相同的用户名和密码，和powerControl.yaml中IPMI中的用户名密码一致。  

因为每个节点的BMC IP（例如10.129.227.189）地址不同，所以powerControl.yaml中需要手动配置，每个节点hostname和BMC IP的映射，这样主动开关机时才能操作到对应的craned节点