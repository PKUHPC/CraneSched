# 作业抢占

CraneSched 支持基于 QoS 的作业抢占机制。当高优先级 QoS 的作业无法获得资源时，调度器可以终止低优先级 QoS 的作业以释放资源。

## 配置

在 `config.yaml` 中启用抢占：

```yaml
Preempt:
  PreemptType: qos       # none | qos | partition
  PreemptMode: CANCEL    # OFF | CANCEL
```

| 字段 | 说明 |
| :--- | :--- |
| `PreemptType` | 抢占触发方式。`none` 禁用；`qos` 基于 QoS 抢占列表；`partition` 基于分区优先级（暂未实现） |
| `PreemptMode` | 抢占执行动作。`OFF` 禁用；`CANCEL` 直接取消被抢占作业 |

## QoS 抢占列表

每个 QoS 可配置一个抢占列表（`preempt`），列出该 QoS 能够抢占的其他 QoS 名称。

### 创建 QoS 并配置抢占

```bash
# 创建低优先级 QoS
cacctmgr add qos LowQos Priority=500

# 创建高优先级 QoS，声明可抢占 LowQos
cacctmgr add qos HighQos Priority=2000 Preempt=LowQos PreemptMode=CANCEL
```

### 修改已有 QoS 的抢占配置

```bash
# 添加抢占目标
cacctmgr modify qos where Name=HighQos set Preempt=LowQos,MediumQos

# 清空抢占列表
cacctmgr modify qos where Name=HighQos set Preempt=""

# 修改抢占模式
cacctmgr modify qos where Name=HighQos set PreemptMode=CANCEL
```

### 查看 QoS 抢占信息

```bash
cacctmgr show qos format=Name,Priority,Preempt,PreemptMode
```

## 抢占行为

### 触发条件

当满足以下全部条件时触发抢占：

1. 集群配置 `PreemptType` 不为 `none`
2. Pending 作业的 QoS 的 `preempt` 列表非空
3. 目标节点上存在属于被抢占 QoS 的 Running 作业
4. 抢占这些作业后资源足以满足新作业需求

### CANCEL 模式

被抢占作业立即收到取消信号，作业状态变为 `Cancelled`，释放的资源可供新作业使用。

## 示例

```bash
# 1. 创建两个 QoS
cacctmgr add qos Background Priority=100
cacctmgr add qos Interactive Priority=5000 Preempt=Background PreemptMode=CANCEL

# 2. 授权账户使用这两个 QoS
cacctmgr modify account where Name=MyAccount set AllowedQos+=Background,Interactive

# 3. 提交占满节点的低优先级作业
cbatch -p CPU --exclusive -q Background long_job.sh

# 4. 提交高优先级作业（触发抢占）
cbatch -p CPU -q Interactive short_job.sh
# → Background 作业被取消，Interactive 作业获得资源并启动
```
