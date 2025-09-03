# creport - 查询作业相关统计信息

**creport 主要用于查询集群指定时间范围的用户、账户相关作业统计信息。**

```bash
$ creport [<OPTION>] [<COMMAND>]
```

## 通用选项

以下选项适用于多数子命令。为避免重复，后续各子命令小节中不再重复列出。

**-S, --start-time string**

:   指定查询开始时间（默认为前一天 00:00:00），格式：`2023-03-14T10:00:00`。

**-E, --end-time string**

:   指定查询结束时间（默认为前一天 23:59:59），格式：`2023-03-14T10:00:00`。

**-t, --time string**

:   指定输出作业数据的时间单位（默认为 minutes）。

**-C, --config string**

:   配置文件路径（默认：`/etc/crane/config.yaml`）。

**-h, --help**

:   显示 creport 命令的帮助信息。

## user 子命令

### user topusage - 用户资源消耗排行

```bash
creport user topusage [--start-time=...] [--end-time=...] [--account=...] ...
```

#### 命令行选项

以下为该子命令特有选项（通用选项见前文"通用选项"）：

**-A, --account string**

:   指定查询账户，指定多个账户时用逗号隔开。

**-u, --user string**

:   指定查询的用户，指定多个用户时用逗号隔开。

**--group bool**

:   将每个用户的所有账户分组在一起（默认为每个用户-账户引用单独展示）。

**--topcount uint32**

:   指定输出条数（默认 10 条）。

#### 输出字段

- **CLUSTER**: 集群名
- **LOGIN**: 用户名
- **PROPER NAME**: Linux系统全名
- **ACCOUNT**: 账户名
- **USED**: 用户下每个作业总CPUs*运行时间值的总和

#### 输出示例

```bash
$ creport  user topusage -S=2024-09-01T01:00:00 -E=2025-09-01T00:20:10
--------------------------------------------------------------------------------
Top 10 Users 2024-09-01T01:00:00 - 2025-09-01T00:20:10 (31533610 secs)
Usage reported in CPU Minutes
|-------------|---------|-------------|------------|------|--------|
| CLUSTER     | LOGIN   | PROPER NAME | ACCOUNT    | USED | ENERGY |
|-------------|---------|-------------|------------|------|--------|
| crane       | user_5  | user_5      | account_5  | 0.67 | 0      |
| crane       | user_3  | user_3      | account_3  | 0.33 | 0      |
| crane       | user_4  | user_4      | account_4  | 0.33 | 0      |
| crane       | user_9  | user_9      | account_9  | 0.33 | 0      |
| crane       | user_10 | user_10     | account_10 | 0.33 | 0      |
| crane       | user_7  | user_7      | account_7  | 0.33 | 0      |
| crane       | user_2  | user_2      | account_2  | 0.33 | 0      |
| crane       | user_8  | user_8      | account_8  | 0.33 | 0      |
| crane       | user_6  | user_6      | account_6  | 0.33 | 0      |
|-------------|---------|-------------|------------|------|--------|
```

## cluster 子命令
<!---
### cluster utilization - 集群整体利用率

```bash
creport cluster utilization [--start-time=...] [--end-time=...] ...
```

#### 命令行选项

无子命令特有选项（通用选项见前文"通用选项"）。

#### 输出字段

- **CLUSTER**: 集群名
- **ALLOCATE**: 统计区间内，所有作业实际分配的资源总量（CPU分钟），即所有作业分配核数 × 运行分钟累加
- **DOWN**: 统计区间内，节点因故障、维护等不可用的总时间（CPU分钟），即停机核数 × 停机分钟累加
- **PLANNED**: 统计区间内，有作业排队但未分配资源的时间（CPU分钟），通常表示资源紧张或排队溢出
- **REPORTED**: 统计区间内，所有资源的理论最大可用时间（CPU分钟），即集群总核数 × 时间跨度

#### 输出示例

```bash
$ creport  cluster  utilization  -S=2024-09-01T00:00:00 -E=2025-09-01T00:00:00
--------------------------------------------------------------------------------
Cluster Utilization 2024-09-01T00:00:00 - 2025-09-01T00:00:00 (31536000 secs)
Usage reported in CPU Minutes
|-------------|----------|------|---------|----------|
| CLUSTER     | ALLOCATE | DOWN | PLANNED | REPORTED |
|-------------|----------|------|---------|----------|
| crane       | 3.33     | -    | -       | -        |
|-------------|----------|------|---------|----------|
```
--->
### cluster accountutilizationbyuser - 账户-用户资源利用率

```bash
creport cluster accountutilizationbyuser [--start-time=...] [--end-time=...] [--account=...] ...
```

#### 命令行选项

以下为该子命令特有选项（通用选项见前文"通用选项"）：

**-A, --account string**

:   指定查询账户，指定多个账户时用逗号隔开。

**-u, --user string**

:   指定查询的用户，指定多个用户时用逗号隔开。

#### 输出字段

- **CLUSTER**: 集群名
- **ACCOUNT**: 账户名
- **LOGIN**: 用户名
- **PROPER NAME**: Linux系统全名
- **USED**: 用户下每个作业总CPUs*运行时间值的总和
- **ENERGY**: 作业消耗的能源

#### 输出示例

```bash
$ creport  cluster  accountutilizationbyuser  -S=2024-09-01T00:00:00 -E=2025-09-01T00:00:00
--------------------------------------------------------------------------------
Cluster/Account/User Utilization 2024-09-01T00:00:00 - 2025-09-01T00:00:00 (31536000 secs)
Usage reported in CPU Minutes
|-------------|------------|---------|-------------|------|--------|
| CLUSTER     | ACCOUNT    | LOGIN   | PROPER NAME | USED | ENERGY |
|-------------|------------|---------|-------------|------|--------|
| crane       | account_10 | user_10 | user_10     | 0.33 | 0      |
| crane       | account_2  | user_2  | user_2      | 0.33 | 0      |
| crane       | account_3  | user_3  | user_3      | 0.33 | 0      |
| crane       | account_4  | user_4  | user_4      | 0.33 | 0      |
| crane       | account_5  | user_5  | user_5      | 0.67 | 0      |
| crane       | account_6  | user_6  | user_6      | 0.33 | 0      |
| crane       | account_7  | user_7  | user_7      | 0.33 | 0      |
| crane       | account_8  | user_8  | user_8      | 0.33 | 0      |
| crane       | account_9  | user_9  | user_9      | 0.33 | 0      |
|-------------|------------|---------|-------------|------|--------|
```

### cluster userutilizationbyaccount - 用户-账户资源利用率

```bash
creport cluster userutilizationbyaccount [--start-time=...] [--end-time=...] [--user=...] ...
```

#### 命令行选项

以下为该子命令特有选项（通用选项见前文"通用选项"）：

**-A, --account string**

:   指定查询账户，指定多个账户时用逗号隔开。

**-u, --user string**

:   指定查询的用户，指定多个用户时用逗号隔开。

#### 输出字段

- **CLUSTER**: 集群名
- **LOGIN**: 用户名
- **PROPER NAME**: Linux系统全名
- **ACCOUNT**: 账户名
- **USED**: 用户下每个作业总CPUs*运行时间值的总和
- **ENERGY**: 作业消耗的能源

#### 输出示例

```bash
$ creport  cluster  userutilizationbyaccount  -S=2024-09-01T00:00:00 -E=2025-09-01T00:00:00
--------------------------------------------------------------------------------
Cluster/User/Account Utilization 2024-09-01T00:00:00 - 2025-09-01T00:00:00 (31536000 secs)
Usage reported in CPU Minutes
|-------------|---------|-------------|------------|------|--------|
| CLUSTER     | LOGIN   | PROPER NAME | ACCOUNT    | USED | ENERGY |
|-------------|---------|-------------|------------|------|--------|
| crane       | user_10 | user_10     | account_10 | 0.33 | 0      |
| crane       | user_2  | user_2      | account_2  | 0.33 | 0      |
| crane       | user_3  | user_3      | account_3  | 0.33 | 0      |
| crane       | user_4  | user_4      | account_4  | 0.33 | 0      |
| crane       | user_5  | user_5      | account_5  | 0.67 | 0      |
| crane       | user_6  | user_6      | account_6  | 0.33 | 0      |
| crane       | user_7  | user_7      | account_7  | 0.33 | 0      |
| crane       | user_8  | user_8      | account_8  | 0.33 | 0      |
| crane       | user_9  | user_9      | account_9  | 0.33 | 0      |
|-------------|---------|-------------|------------|------|--------|
```

### cluster userutilizationbywckey - 用户-WCKEY资源利用率

```bash
creport cluster userutilizationbywckey [--start-time=...] [--end-time=...] [--user=...] ...
```

#### 命令行选项

以下为该子命令特有选项（通用选项见前文"通用选项"）：

**-u, --user string**

:   指定查询的用户，指定多个用户时用逗号隔开。

#### 输出字段

- **CLUSTER**: 集群名
- **LOGIN**: 用户名
- **PROPER NAME**: Linux系统全名
- **WCKEY**: WCKEY名
- **USED**: 用户下每个作业总CPUs*运行时间值的总和

#### 输出示例

```bash
$ creport  cluster  userutilizationbywckey  -S=2024-09-01T00:00:00 -E=2025-09-01T00:00:00
--------------------------------------------------------------------------------
Cluster/Account/User Utilization 2024-09-01T00:00:00 - 2025-09-01T00:00:00 (31536000 secs)
Usage reported in CPU Minutes
|-------------|---------|-------------|-------|------|
| CLUSTER     | LOGIN   | PROPER NAME | WCKEY | USED |
|-------------|---------|-------------|-------|------|
| crane       | user_9  | user_9      |       | 0.33 |
| crane       | user_8  | user_8      |       | 0.33 |
| crane       | user_10 | user_10     |       | 0.33 |
| crane       | user_7  | user_7      |       | 0.33 |
| crane       | user_3  | user_3      |       | 0.33 |
| crane       | user_4  | user_4      |       | 0.33 |
| crane       | user_6  | user_6      |       | 0.33 |
| crane       | user_2  | user_2      |       | 0.33 |
| crane       | user_5  | user_5      |       | 0.67 |
|-------------|---------|-------------|-------|------|
```

### cluster wckeyutilizationbyuser - WCKEY-用户资源利用率

```bash
creport cluster wckeyutilizationbyuser [--start-time=...] [--end-time=...] [--wckeys=...] ...
```

#### 命令行选项

以下为该子命令特有选项（通用选项见前文"通用选项"）：

**-w, --wckeys string**

:   指定查询的 WCKEY，指定多个 WCKEY 时用逗号隔开。

#### 输出字段

- **CLUSTER**: 集群名
- **WCKEY**: WCKEY名
- **LOGIN**: 用户名
- **PROPER NAME**: Linux系统全名
- **USED**: 用户下每个作业总CPUs*运行时间值的总和

#### 输出示例

```bash
$ creport  cluster  wckeyutilizationbyuser  -S=2024-09-01T00:00:00 -E=2025-09-01T00:00:00
--------------------------------------------------------------------------------
Cluster/WCKey/User Utilization 2024-09-01T00:00:00 - 2025-09-01T00:00:00 (31536000 secs)
Usage reported in CPU Minutes
|-------------|-------|---------|-------------|------|
| CLUSTER     | WCKEY | LOGIN   | PROPER NAME | USED |
|-------------|-------|---------|-------------|------|
| crane       |       | user_3  | user_3      | 0.33 |
| crane       |       | user_8  | user_8      | 0.33 |
| crane       |       | user_7  | user_7      | 0.33 |
| crane       |       | user_10 | user_10     | 0.33 |
| crane       |       | user_5  | user_5      | 0.67 |
| crane       |       | user_2  | user_2      | 0.33 |
| crane       |       | user_9  | user_9      | 0.33 |
| crane       |       | user_4  | user_4      | 0.33 |
| crane       |       | user_6  | user_6      | 0.33 |
|-------------|-------|---------|-------------|------|
```

### cluster accountutilizationbyqos - 账户-QOS资源利用率

```bash
creport cluster accountutilizationbyqos [--start-time=...] [--end-time=...] [--account=...] [--qos=...] ...
```

#### 命令行选项

以下为该子命令特有选项（通用选项见前文"通用选项"）：

**-A, --account string**

:   指定查询账户，指定多个账户时用逗号隔开。

**-q, --qos string**

:   指定查询的 QOS，指定多个 QOS 时用逗号隔开。

#### 输出字段

- **CLUSTER**: 集群名
- **ACCOUNT**: 账户名
- **QOS**: QOS名
- **USED**: 用户下每个作业总CPUs*运行时间值的总和
- **ENERGY**: 作业消耗的能源

#### 输出示例

```bash
$ creport cluster accountutilizationbyqos -S=2024-09-01T00:00:00 -E=2025-09-01T00:00:00
--------------------------------------------------------------------------------
Cluster/Account/Qos Utilization 2024-09-01T00:00:00 - 2025-09-01T00:00:00 (31536000 secs)
Usage reported in CPU Minutes
|-------------|------------|-----------|------|--------|
| CLUSTER     | ACCOUNT    | QOS       | USED | ENERGY |
|-------------|------------|-----------|------|--------|
| crane       | account_10 | UNLIMITED | 0.33 | 0      |
| crane       | account_2  | UNLIMITED | 0.33 | 0      |
| crane       | account_3  | UNLIMITED | 0.33 | 0      |
| crane       | account_4  | UNLIMITED | 0.33 | 0      |
| crane       | account_5  | UNLIMITED | 0.67 | 0      |
| crane       | account_6  | UNLIMITED | 0.33 | 0      |
| crane       | account_7  | UNLIMITED | 0.33 | 0      |
| crane       | account_8  | UNLIMITED | 0.33 | 0      |
| crane       | account_9  | UNLIMITED | 0.33 | 0      |
|-------------|------------|-----------|------|--------|
```

## job 子命令

### job 子命令公共选项

以下选项适用于所有 job 子命令。为避免重复，后续各子命令小节中不再重复列出。

**--gid string**

:   指定查询的 gid，指定多个 gid 时用逗号隔开。

**--grouping string**

:   以逗号分隔的尺寸分组列表（默认：`50,100,250,500,1000`）。

**--printjobcount bool**

:   报告将打印作业范围的数量，而不是使用的时间。

**-j, --jobs string**

:   指定查询作业号，指定多个作业号时用逗号隔开（如 `-j=2,3,4`）。

**-n, --nodes string**

:   指定查询的节点名，指定多个节点时用逗号隔开。

**-p, --partition string**

:   指定查询的分区，指定多个分区时用逗号隔开。

### job sizesbyaccount - 按账户分组的作业规模分布

```bash
creport job sizesbyaccount [--start-time=...] [--end-time=...] [--account=...] ...
```

#### 命令行选项

以下为该子命令特有选项（公共选项见前文"job 子命令公共选项"）：

**-A, --account string**

:   指定查询账户，指定多个账户时用逗号隔开。

#### 输出字段

- **CLUSTER**: 集群名
- **ACCOUNT**: 账户名
- **0-49 CPUs**: 0-49CPUs范围CPU分钟
- **50-249 CPUs**: 50-249CPUs范围CPU分钟
- **250-499 CPUs**: 250-499CPUs范围CPU分钟
- **500-999 CPUs**: 500-999CPUs范围CPU分钟
- **>= 1000 CPUs**: 大于等于1000CPUs范围CPU分钟
- **TOTAL CPU TIME**: 特定账户下所有的CPU分钟总和
- **% OF CLUSTER**: 占用集群所有作业CPU分钟数的百分比

#### 输出示例

```bash
$ creport  job  sizesbyaccount  -S=2024-09-01T00:00:00 -E=2025-09-01T00:00:00
------------------------------------------------------------------------------------------------------------------------------------
Job Sizes 2024-09-01T00:00:00 - 2025-09-01T00:00:00 (31536000 secs)
Time reported in Minutes
+-------------+------------+-----------+-------------+--------------+--------------+-------------+----------------+----------------+
| CLUSTER     | ACCOUNT    | 0-49 CPUS | 50-249 CPUS | 250-499 CPUS | 500-999 CPUS | >= 1000 CPUS| TOTAL CPU TIME | % OF CLUSTER   |
+-------------+------------+-----------+-------------+--------------+--------------+-------------+----------------+----------------+
| crane       | account_10 |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | account_2  |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | account_3  |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | account_4  |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | account_5  |      0.67 |        0.00 |         0.00 |         0.00 |        0.00 |           0.67 | 20.00%         |
| crane       | account_6  |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | account_7  |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | crane      |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | account_9  |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
+-------------+------------+-----------+-------------+--------------+--------------+-------------+----------------+----------------+
```

### job sizesbywckey - 按WCKEY分组的作业规模分布

```bash
creport job sizesbywckey [--start-time=...] [--end-time=...] [--wckeys=...] ...
```

#### 命令行选项

以下为该子命令特有选项（公共选项见前文"job 子命令公共选项"）：

**-w, --wckeys string**

:   指定查询的 WCKEY，指定多个 WCKEY 时用逗号隔开。

#### 输出字段

- **CLUSTER**: 集群名
- **WCKEY**: WCKEY名
- **0-49 CPUs**: 0-49CPUs范围CPU分钟
- **50-249 CPUs**: 50-249CPUs范围CPU分钟
- **250-499 CPUs**: 250-499CPUs范围CPU分钟
- **500-999 CPUs**: 500-999CPUs范围CPU分钟
- **>= 1000 CPUs**: 大于等于1000CPUs范围CPU分钟
- **TOTAL CPU TIME**: 特定账户下所有的CPU分钟总和
- **% OF CLUSTER**: 占用集群所有作业CPU分钟数的百分比

#### 输出示例

```bash
$ creport  job  sizesbywckey  -S=2024-09-01T00:00:00 -E=2025-09-01T00:00:00
----------------------------------------------------------------------------------------------------
Job Sizes by Wckey 2024-09-01T00:00:00 - 2025-09-01T00:00:00 (31536000 secs)
Time reported in Minutes
+-------------+-------+-----------+-------------+--------------+--------------+-------------+----------------+----------------+
| CLUSTER     | WCKEY | 0-49 CPUS | 50-249 CPUS | 250-499 CPUS | 500-999 CPUS | >= 1000 CPUS| TOTAL CPU TIME | % OF CLUSTER   |
+-------------+-------+-----------+-------------+--------------+--------------+-------------+----------------+----------------+
| crane       |       |      3.33 |        0.00 |         0.00 |         0.00 |        0.00 |           3.33 | 100.00%        |
+-------------+-------+-----------+-------------+--------------+--------------+-------------+----------------+----------------+
```

### job sizesbyaccountandwckey - 按账户和WCKEY分组的作业规模分布

```bash
creport job sizesbyaccountandwckey [--start-time=...] [--end-time=...] [--wckeys=...] ...
```

#### 命令行选项

以下为该子命令特有选项（公共选项见前文"job 子命令公共选项"）：

**-A, --account string**

:   指定查询账户，指定多个账户时用逗号隔开。

**-w, --wckeys string**

:   指定查询的 WCKEY，指定多个 WCKEY 时用逗号隔开。

#### 输出字段

- **CLUSTER**: 集群名
- **ACCOUNT:WCKEY**: 账户名与WCKEY名的组合（格式：`<ACCOUNT>:<WCKEY>`）
- **0-49 CPUs**: 0-49CPUs范围CPU分钟
- **50-249 CPUs**: 50-249CPUs范围CPU分钟
- **250-499 CPUs**: 250-499CPUs范围CPU分钟
- **500-999 CPUs**: 500-999CPUs范围CPU分钟
- **>= 1000 CPUs**: 大于等于1000CPUs范围CPU分钟
- **TOTAL CPU TIME**: 特定账户下所有的CPU分钟总和
- **% OF CLUSTER**: 占用集群所有作业CPU分钟数的百分比

#### 输出示例

```bash
$ creport  job  sizesbyaccountandwckey  -S=2024-09-01T00:00:00 -E=2025-09-01T00:00:00
------------------------------------------------------------------------------------------------------------------------------------
Job Sizes 2024-09-01T00:00:00 - 2025-09-01T00:00:00 (31536000 secs)
Time reported in Minutes
+-------------+----------------+-----------+-------------+--------------+--------------+-------------+----------------+----------------+
| CLUSTER     | ACCOUNT:WCKEY  | 0-49 CPUS | 50-249 CPUS | 250-499 CPUS | 500-999 CPUS | >= 1000 CPUS| TOTAL CPU TIME | % OF CLUSTER   |
+-------------+----------------+-----------+-------------+--------------+--------------+-------------+----------------+----------------+
| crane       | account_10:    |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | account_2:     |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | account_3:     |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | account_4:     |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | account_5:     |      0.67 |        0.00 |         0.00 |         0.00 |        0.00 |           0.67 | 20.00%         |
| crane       | account_6:     |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | account_7:     |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | account_8:     |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | account_9:     |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
+-------------+----------------+-----------+-------------+--------------+--------------+-------------+----------------+----------------+
```


## 另请参阅

- [cqueue](cqueue.md) - 查看作业队列（当前/挂起作业）
- [cbatch](cbatch.md) - 提交批处理作业
- [ccancel](ccancel.md) - 取消作业
- [ceff](ceff.md) - 查看作业效率统计
- [cacct](cacct.md) - 查询已完成作业
