# 资源限制配置指南

CraneSched 支持在两个层面为账户和用户配置资源使用上限：

- **QoS 层面**：通过 QoS 策略对使用该 QoS 的作业生效，限制范围绑定到 QoS 本身，适用于按服务质量分级管理资源配额
- **分区层面**：针对特定分区生效，可分别为账户和用户设置，适用于按分区隔离资源配额的场景

两套限制机制在同一实体（用户/账号）下作为**并列维度**独立检查：对每个用户和账号，系统先检查其 QoS 维度的限制，再检查其分区维度的限制，两者均通过才允许作业提交或调度。此外，分区限制仅在 QoS 未覆盖对应维度时才额外生效（例如，若 QoS 已设置 `MaxJobs`，则分区的 `MaxJobs` 不再重复检查）。

## 前置条件

- CraneSched 集群已正常运行
- 操作用户具有 Admin 或 Operator 权限
- 目标账户/用户已通过 `cacctmgr` 创建

---

## 一、QoS 资源限制

QoS（服务质量）资源限制全局生效，不区分分区。通过 `cacctmgr add qos` 或 `cacctmgr modify qos` 进行配置。

### 1.1 QoS 限制项说明

| 限制项 | 说明 | 作用维度 |
|--------|------|----------|
| `MaxJobsPerUser` | 每个用户最大并发运行作业数 | 用户 |
| `MaxSubmitJobsPerUser` | 每个用户最大提交（含排队）作业数 | 用户 |
| `MaxCpusPerUser` | 每个用户最大 CPU 使用量 | 用户 |
| `MaxTresPerUser` | 每个用户最大 TRES 总用量 | 用户 |
| `MaxJobsPerAccount` | 每个账号最大并发运行作业数 | 账号 |
| `MaxSubmitJobsPerAccount` | 每个账号最大提交（含排队）作业数 | 账号 |
| `MaxTresPerAccount` | 每个账号最大 TRES 总用量 | 账号 |
| `MaxJobs` | 该 QoS 全局最大并发运行作业数 | QoS 全局 |
| `MaxSubmitJobs` | 该 QoS 全局最大提交作业数 | QoS 全局 |
| `MaxTres` | 该 QoS 全局最大 TRES 总用量 | QoS 全局 |
| `MaxWall` | 该 QoS 全局累计墙钟时间上限（秒） | QoS 全局 |
| `MaxTimeLimitPerJob` | 单个作业最大运行时间 | 单作业 |
| `Priority` | QoS 优先级（值越高优先级越高） | — |
| `Flags` | QoS 标志（`DenyOnLimit` 或 `None`） | — |

### 1.2 创建 QoS

**语法：**

```bash
cacctmgr add qos <名称> [选项]
```

**示例：**

创建普通 QoS：

```bash
cacctmgr add qos normal Description="普通QoS" Priority=1000 \
  MaxJobsPerUser=10 MaxCpusPerUser=100
```

创建高优先级 QoS，并限制单作业最大运行时间为 24 小时：

```bash
cacctmgr add qos high Description="高优先级" Priority=5000 \
  MaxJobsPerUser=20 MaxCpusPerUser=200 MaxTimeLimitPerJob=86400
```

创建带 TRES 限制的 QoS：

```bash
cacctmgr add qos gpu_qos Description="GPU队列" Priority=2000 \
  MaxTresPerUser=cpu:64,mem:128G \
  MaxTresPerAccount=cpu:256,mem:512G \
  MaxTimeLimitPerJob=172800
```

### 1.3 修改 QoS

**语法：**

```bash
cacctmgr modify qos where Name=<qos名称> set <属性>=<值>
```

**可修改属性：**

| 参数 | 类型 | 说明 |
|------|------|------|
| `Description=<描述>` | string | 设置描述 |
| `Priority=<优先级>` | uint32 | 设置优先级 |
| `MaxCpusPerUser=<num>` | uint64 | 设置每用户最大 CPU 数 |
| `MaxJobsPerUser=<num>` | uint32 | 设置每用户最大作业数 |
| `MaxSubmitJobsPerUser=<num>` | uint32 | 设置每用户最大提交作业数 |
| `MaxTresPerUser=<tres>` | TRES 字符串 | 设置每用户最大 TRES 用量 |
| `MaxJobsPerAccount=<num>` | uint32 | 设置每账号最大作业数 |
| `MaxSubmitJobsPerAccount=<num>` | uint32 | 设置每账号最大提交作业数 |
| `MaxTresPerAccount=<tres>` | TRES 字符串 | 设置每账号最大 TRES 用量 |
| `MaxJobs=<num>` | uint32 | 设置 QoS 全局最大作业数 |
| `MaxSubmitJobs=<num>` | uint32 | 设置 QoS 全局最大提交作业数 |
| `MaxTres=<tres>` | TRES 字符串 | 设置 QoS 全局最大 TRES 用量 |
| `MaxWall=<sec>` | uint64（秒） | 设置 QoS 全局累计墙钟时间上限 |
| `MaxTimeLimitPerJob=<duration\|sec>` | 时长或秒数 | 设置单作业最大运行时间 |
| `Flags=<DenyOnLimit\|None>` | 枚举 | 设置 QoS 标志 |

**示例：**

修改 QoS 优先级：

```bash
cacctmgr modify qos where Name=normal set Priority=2000
```

更新每用户资源限制：

```bash
cacctmgr modify qos where Name=high set MaxJobsPerUser=50 MaxCpusPerUser=500
```

设置 TRES 限制：

```bash
cacctmgr modify qos where Name=gpu_qos set MaxTresPerUser=cpu:128,mem:256G
```

设置单作业最大运行时间（支持时长格式 `天-时:分:秒` 或秒数）：

```bash
# 使用秒数
cacctmgr modify qos where Name=normal set MaxTimeLimitPerJob=3600

# 使用时长格式（1天2小时30分）
cacctmgr modify qos where Name=high set MaxTimeLimitPerJob=1-2:30:0
```

### 1.4 查看 QoS

```bash
# 显示所有 QoS
cacctmgr show qos

# 显示特定 QoS
cacctmgr show qos normal

# 按格式显示
cacctmgr show qos format=name,MaxJobsPerUser,MaxCpusPerUser
```

### 1.5 删除 QoS

```bash
cacctmgr delete qos low
```

### 1.6 QoS 限制生效规则

QoS 限制在**提交阶段**和**调度阶段**均会检查：

**提交阶段（立即返回错误）：**

以下限制在作业提交时即刻检查，超限则直接拒绝提交：

| 错误码 | 说明 | 对应限制 |
|--------|------|---------|
| `ERR_MAX_JOB_COUNT_PER_USER` | 用户提交作业数超限 | `MaxSubmitJobsPerUser` 超限 |
| `ERR_MAX_JOB_COUNT_PER_ACCOUNT` | 账号提交作业数超限 | `MaxSubmitJobsPerAccount` 超限 |
| `ERR_QOS_JOB_COUNT_EXCEEDED` | QoS 全局提交作业数超限 | `MaxSubmitJobs` 超限 |
| `ERR_CPUS_PER_TASK_BEYOND` | 单作业 CPU 超过 QoS 限制 | `MaxCpusPerUser` 超限 |
| `ERR_TRES_PER_JOB_BEYOND` | 单作业 TRES 超过 QoS 限制 | `MaxTresPerUser`/`MaxTresPerAccount`/`MaxTres` 超限 |
| `ERR_TIME_TIMIT_BEYOND` | 作业时间限制超过 QoS 限制 | `MaxTimeLimitPerJob` 超限 |

**调度阶段（作业排队等待）：**

以下限制在调度时检查，超限则作业保持排队状态，并显示对应的排队原因：

| 排队原因 | 说明 | 对应限制 |
|----------|------|---------|
| `QosCpuResourceLimit` | CPU 使用量超过 QoS 限制 | `MaxCpusPerUser` 或 `MaxTresPerUser/Account` 中 CPU 超限 |
| `QosMemResourceLimit` | 内存使用量超过 QoS 限制 | `MaxTresPerUser` 或 `MaxTresPerAccount` 中 Mem 超限 |
| `QosGresResourceLimit` | GRES 使用量超过 QoS 限制 | `MaxTresPerUser` 或 `MaxTresPerAccount` 中 GRES 超限 |
| `QosJobsResourceLimit` | 运行作业数超过 QoS 限制 | `MaxJobsPerUser` 或 `MaxJobsPerAccount` 超限 |
| `QosWallTimeLimit` | 累计墙钟时间超过 QoS 限制 | `MaxWall` 超限 |

---

## 二、分区资源限制

分区资源限制（Partition Resource Limit）针对特定分区生效，可分别为账户和用户设置。通过 `cacctmgr modify account/user` 并在 `where` 子句中指定 `Partition=<分区名>` 进行配置。

### 2.1 分区限制项说明

| 限制项 | 说明 |
|--------|------|
| `MaxJobs` | 该账户/用户在此分区内最大并发运行作业数 |
| `MaxSubmitJobs` | 该账户/用户在此分区内最大提交（含排队）作业数 |
| `MaxTres` | 该账户/用户在此分区内最大 TRES 总用量（如 CPU、内存） |
| `MaxTresPerJob` | 单个作业在此分区内最大 TRES 用量 |
| `MaxWall` | 该账户/用户在此分区内累计墙钟时间上限（秒） |
| `MaxWallPerJob` | 单个作业在此分区内最大墙钟时间（秒） |

### 2.2 为账户设置分区资源限制

**语法：**

```bash
cacctmgr modify account where Name=<账户名> Partition=<分区名> set <限制项>=<值>
```

> **注意**：所有分区资源限制参数均**必须**在 `where` 子句中指定 `Partition=<分区名>`，否则命令将报错。

**参数说明：**

| 参数 | 类型 | 说明 | 示例值 |
|------|------|------|--------|
| `MaxJobs=<num>` | uint32 | 最大并发运行作业数 | `10` |
| `MaxSubmitJobs=<num>` | uint32 | 最大提交作业数（含排队） | `50` |
| `MaxTres=<tres>` | TRES 字符串 | 最大 TRES 总用量 | `cpu:100,mem:200G` |
| `MaxTresPerJob=<tres>` | TRES 字符串 | 单作业最大 TRES 用量 | `cpu:32,mem:64G` |
| `MaxWall=<sec>` | uint64（秒） | 累计墙钟时间上限 | `86400` |
| `MaxWallPerJob=<sec>` | uint64（秒） | 单作业最大墙钟时间 | `3600` |

**示例：**

为账户 `PKU` 在 `GPU` 分区设置最大并发运行作业数为 20，最大提交作业数为 100：

```bash
cacctmgr modify account where Name=PKU Partition=GPU set MaxJobs=20 MaxSubmitJobs=100
```

为账户 `PKU` 在 `CPU` 分区设置最大 TRES 总用量（CPU 不超过 200 核，内存不超过 400G）：

```bash
cacctmgr modify account where Name=PKU Partition=CPU set MaxTres=cpu:200,mem:400G
```

为账户 `PKU` 在 `CPU` 分区设置单作业最大 TRES 用量和最大墙钟时间：

```bash
cacctmgr modify account where Name=PKU Partition=CPU \
  set MaxTresPerJob=cpu:32,mem:64G MaxWallPerJob=3600
```

同时设置多个限制项：

```bash
cacctmgr modify account where Name=PKU Partition=GPU \
  set MaxJobs=20 MaxSubmitJobs=100 MaxTresPerJob=cpu:8,mem:32G MaxWallPerJob=7200
```

### 2.3 为用户设置分区资源限制

**语法：**

```bash
cacctmgr modify user where Name=<用户名> [Account=<账户名>] Partition=<分区名> set <限制项>=<值>
```

> **注意**：所有分区资源限制参数均**必须**在 `where` 子句中指定 `Partition=<分区名>`，否则命令将报错。

**示例：**

为用户 `alice` 在 `GPU` 分区设置最大并发运行作业数为 5：

```bash
cacctmgr modify user where Name=alice Partition=GPU set MaxJobs=5
```

为用户 `alice` 在账户 `PKU` 的 `CPU` 分区设置最大提交作业数为 30：

```bash
cacctmgr modify user where Name=alice Account=PKU Partition=CPU set MaxSubmitJobs=30
```

为用户 `alice` 在 `GPU` 分区设置单作业最大 TRES 用量和墙钟时间：

```bash
cacctmgr modify user where Name=alice Partition=GPU \
  set MaxTresPerJob=cpu:8,mem:32G MaxWallPerJob=7200
```

### 2.4 查看分区资源限制

使用 `--partition-limit`（简写 `-P`）全局选项，可在 `show account` 或 `show user` 时额外展示分区资源限制表格。

查看所有账户的分区资源限制：

```bash
cacctmgr show account --partition-limit
```

查看特定账户：

```bash
cacctmgr show account PKU -P
```

输出示例：

```
+------+-----------+---------+---------------+---------+---------------+-----------+--------------+
| NAME | PARTITION | MAXTRES | MAXTRESPERJOB | MAXJOBS | MAXSUBMITJOBS | MAXWALL   | MAXWALLPERJOB|
+------+-----------+---------+---------------+---------+---------------+-----------+--------------+
| PKU  | GPU       |         | cpu:8,mem:32G | 20      | 100           | unlimited | 02:00:00     |
| PKU  | CPU       | cpu:200 | cpu:32,mem:64G| unlimited| unlimited    | unlimited | 01:00:00     |
+------+-----------+---------+---------------+---------+---------------+-----------+--------------+
```

查看用户的分区资源限制：

```bash
cacctmgr show user alice -P
```

输出示例：

```
+-------+----------+------+-----------+---------+---------------+---------+---------------+-----------+--------------+
|ACCOUNT| USERNAME | UID  | PARTITION | MAXTRES | MAXTRESPERJOB | MAXJOBS | MAXSUBMITJOBS | MAXWALL   | MAXWALLPERJOB|
+-------+----------+------+-----------+---------+---------------+---------+---------------+-----------+--------------+
| PKU   | alice    | 1001 | GPU       |         | cpu:8,mem:32G | 5       | 30            | unlimited | 02:00:00     |
+-------+----------+------+-----------+---------+---------------+---------+---------------+-----------+--------------+
```

> **说明**：`unlimited` 表示该项未设置限制，不受约束。时间字段以 `HH:MM:SS` 格式显示。

### 2.5 分区限制生效规则

分区资源限制在作业提交和调度两个阶段分别生效：

**提交阶段（立即返回错误）：**

以下限制在作业提交时即刻检查，超限则直接拒绝提交：

| 错误码 | 说明 |
|--------|------|
| `ERR_PARTITION_TRES_PER_JOB_BEYOND` (106) | 单个作业的 TRES 超过分区 `MaxTresPerJob` 限制 |
| `ERR_PARTITION_TIME_BEYOND` (107) | 作业时间限制超过分区 `MaxWallPerJob` 限制 |
| `ERR_PARTITION_MAX_SUBMIT_JOBS_PER_USER` (108) | 用户在该分区的提交作业数超过 `MaxSubmitJobs` 限制 |
| `ERR_PARTITION_MAX_SUBMIT_JOBS_PER_ACCOUNT` (109) | 账号在该分区的提交作业数超过 `MaxSubmitJobs` 限制 |

**调度阶段（作业排队等待）：**

以下限制在调度时检查，超限则作业保持排队状态，并显示对应的排队原因：

| 排队原因 | 说明 |
|----------|------|
| `UserPartitionJobsLimit` | 用户在该分区的运行作业数超过 `MaxJobs` 限制 |
| `AccPartitionJobsLimit` | 账号在该分区的运行作业数超过 `MaxJobs` 限制 |
| `UserPartitionWallTimeLimit` | 用户在该分区的累计墙钟时间超过 `MaxWall` 限制 |
| `AccPartitionWallTimeLimit` | 账号在该分区的累计墙钟时间超过 `MaxWall` 限制 |
| `PartitionCpuResourceLimit` | CPU 使用量超过分区 `MaxTres` 中的 CPU 限制 |
| `PartitionMemResourceLimit` | 内存使用量超过分区 `MaxTres` 中的内存限制 |
| `PartitionGresResourceLimit` | GRES 使用量超过分区 `MaxTres` 中的 GRES 限制 |

---

## 三、完整配置示例

以下示例展示了同时使用 QoS 限制和分区限制的典型配置流程：

```bash
# 1. 创建 QoS 策略，设置全局资源限制
cacctmgr add qos gpu_qos Description="GPU队列" Priority=2000 \
  MaxJobsPerUser=20 \
  MaxTresPerUser=cpu:64,mem:128G \
  MaxTimeLimitPerJob=172800

# 2. 创建账户并关联 QoS
cacctmgr add account PKU Description="北京大学" Partition=CPU,GPU QosList=gpu_qos

# 3. 为账户 PKU 在 GPU 分区设置分区级资源限制
cacctmgr modify account where Name=PKU Partition=GPU \
  set MaxJobs=50 \
  set MaxSubmitJobs=200 \
  set MaxTresPerJob=cpu:8,mem:32G \
  set MaxWallPerJob=7200

# 4. 为账户 PKU 在 CPU 分区设置分区级资源限制
cacctmgr modify account where Name=PKU Partition=CPU \
  set MaxTres=cpu:200,mem:400G \
  set MaxTresPerJob=cpu:32,mem:64G \
  set MaxWallPerJob=3600

# 5. 为用户 alice 在 GPU 分区设置更严格的个人限制
cacctmgr modify user where Name=alice Account=PKU Partition=GPU \
  set MaxJobs=5 \
  set MaxSubmitJobs=20 \
  set MaxWallPerJob=3600

# 6. 查看 QoS 配置
cacctmgr show qos gpu_qos

# 7. 查看账户的分区资源限制
cacctmgr show account PKU -P

# 8. 查看用户的分区资源限制
cacctmgr show user alice -P
```

---

## 四、注意事项

1. **两套限制并列检查**：对每个用户和账号，系统先检查 QoS 维度的限制，再检查分区维度的限制，两者均通过才允许作业提交或调度。分区限制仅在 QoS 未覆盖对应维度时才额外生效：若 QoS 已设置 `MaxJobs`/`MaxWall`/`MaxTres`，则分区对应的 `MaxJobs`/`MaxWall`/`MaxTres` 不再重复检查，避免双重限制。

2. **分区必须在 where 子句中指定**：设置分区资源限制时，必须在 `where` 子句中指定 `Partition=<分区名>`，否则命令将返回错误。

3. **账户与用户限制的关系**：账户级别的分区限制对该账户下所有用户的总量生效；用户级别的分区限制仅对该用户生效。两者均配置时，作业需同时满足账户和用户的限制。

4. **TRES 格式**：TRES 字符串格式为 `资源类型:数量`，多个资源用逗号分隔，例如 `cpu:32,mem:64G`。内存支持 `K`、`M`、`G`、`T` 等单位后缀。GRES 格式为 `gres/type[:name]:num`，例如 `gres/gpu:4`。

5. **时间单位**：
   - QoS 的 `MaxTimeLimitPerJob` 支持时长格式（`天-时:分:秒`，如 `1-2:30:0`）或秒数
   - 分区限制的 `MaxWall` 和 `MaxWallPerJob` 单位均为**秒**，例如 1 小时填写 `3600`

6. **unlimited 的含义**：未设置某项限制时，该项显示为 `unlimited`，表示不受该维度约束。

## 另请参阅

- [cacctmgr - 账户管理器](../../command/cacctmgr.md)
- [作业排队原因](../../reference/pending_reason.md)
- [错误码参考](../../reference/error_code.md)
