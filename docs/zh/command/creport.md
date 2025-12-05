# creport - 查询作业相关统计信息

**creport 主要用于查询集群指定时间范围的用户、账户相关作业统计信息。**

```bash
creport [<OPTION>] [<COMMAND>]
```

## 展示用户资源消耗排行
```bash
creport user topusage [--start-time=...] [--end-time=...] [--account=...] ...
```
### 输出示例

![creport](../../images/creport/topusr.png)

### 输出字段

- **CLUSTER**: 集群名
- **LOGIN**: 用户名
- **PROPER NAME**: Linux系统全名
- **ACCOUNT**: 账户名
- **USED**: 用户下每个作业总CPUs*运行时间值的总和

### 命令行选项

#### 过滤选项
- **-A, --account string**: 指定查询账户，指定多个账户时用逗号隔开
- **-u, --user string**: 指定查询的用户，指定多个用户时用逗号隔开
- **--group bool**: 将每个用户的所有账户分组在一起（默认为每个用户和账户引用的单独条目）
- **-t, --time string**: 指定输出作业数据的时间单位（默认为minutes）
- **--topcount uint32**: 指定输出条数（默认10条）

#### 时间范围过滤
- **-S, --start-time string**: 指定查询开始的时间（默认为前一天00:00:00）格式：`2023-03-14T10:00:00`
- **-E, --end-time string**: 指定查询结束的时间（默认为前一天23:59:59）格式：`2023-03-14T10:00:00`

#### 其他选项
- **-C, --config string**: 配置文件路径（默认：`/etc/crane/config.yaml`）
- **-h, --help**: 显示帮助信息

## 展示账户-用户资源利用率
```bash
creport cluster accountutilizationbyuser [--start-time=...] [--end-time=...] [--account=...] ...
```
### 输出示例

![creport](../../images/creport/accountutilizationbyuser.png)

### 输出字段

- **CLUSTER**: 集群名
- **ACCOUNT**: 账户名
- **LOGIN**: 用户名
- **PROPER NAME**: Linux系统全名
- **USED**: 用户下每个作业总CPUs*运行时间值的总和
- **ENERGY**: 作业消耗的能源

### 命令行选项

#### 过滤选项
- **-A, --account string**: 指定查询账户，指定多个账户时用逗号隔开
- **-u, --user string**: 指定查询的用户，指定多个用户时用逗号隔开
- **-t, --time string**: 指定输出作业数据的时间单位（默认为minutes）

#### 时间范围过滤
- **-S, --start-time string**: 指定查询开始的时间（默认为前一天00:00:00）格式：`2023-03-14T10:00:00`
- **-E, --end-time string**: 指定查询结束的时间（默认为前一天23:59:59）格式：`2023-03-14T10:00:00`

#### 其他选项
- **-C, --config string**: 配置文件路径（默认：`/etc/crane/config.yaml`）
- **-h, --help**: 显示帮助信息

## 展示用户-账户资源利用率
```bash
creport cluster userutilizationbyaccount [--start-time=...] [--end-time=...] [--user=...] ...
```
### 输出示例

![creport](../../images/creport/userutilizationbyaccount.png)

### 输出字段

- **CLUSTER**: 集群名
- **LOGIN**: 用户名
- **PROPER NAME**: Linux系统全名
- **ACCOUNT**: 账户名
- **USED**: 用户下每个作业总CPUs*运行时间值的总和
- **ENERGY**: 作业消耗的能源

### 命令行选项

#### 过滤选项
- **-A, --account string**: 指定查询账户，指定多个账户时用逗号隔开
- **-u, --user string**: 指定查询的用户，指定多个用户时用逗号隔开
- **-t, --time string**: 指定输出作业数据的时间单位（默认为minutes）

#### 时间范围过滤
- **-S, --start-time string**: 指定查询开始的时间（默认为前一天00:00:00）格式：`2023-03-14T10:00:00`
- **-E, --end-time string**: 指定查询结束的时间（默认为前一天23:59:59）格式：`2023-03-14T10:00:00`

#### 其他选项
- **-C, --config string**: 配置文件路径（默认：`/etc/crane/config.yaml`）
- **-h, --help**: 显示帮助信息

## 展示用户-wckey资源利用率
```bash
creport cluster userutilizationbywckey [--start-time=...] [--end-time=...] [--user=...] ...
```
### 输出示例

![creport](../../images/creport/userutilizationbywckey.png)

### 输出字段

- **CLUSTER**: 集群名
- **LOGIN**: 用户名
- **PROPER NAME**: Linux系统全名
- **WCKEY**: wckey名
- **USED**: 用户下每个作业总CPUs*运行时间值的总和

### 命令行选项

#### 过滤选项
- **-u, --user string**: 指定查询的用户，指定多个用户时用逗号隔开
- **-t, --time string**: 指定输出作业数据的时间单位（默认为minutes）

#### 时间范围过滤
- **-S, --start-time string**: 指定查询开始的时间（默认为前一天00:00:00）格式：`2023-03-14T10:00:00`
- **-E, --end-time string**: 指定查询结束的时间（默认为前一天23:59:59）格式：`2023-03-14T10:00:00`

#### 其他选项
- **-C, --config string**: 配置文件路径（默认：`/etc/crane/config.yaml`）
- **-h, --help**: 显示帮助信息

## 展示wckey-用户资源利用率
```bash
creport cluster wckeyutilizationbyuser [--start-time=...] [--end-time=...] [--wckeys=...] ...
```
### 输出示例

![creport](../../images/creport/wckeyutilizationbyuser.png)

### 输出字段

- **CLUSTER**: 集群名
- **WCKEY**: wckey名
- **LOGIN**: 用户名
- **PROPER NAME**: Linux系统全名
- **USED**: 用户下每个作业总CPUs*运行时间值的总和

### 命令行选项

#### 过滤选项
- **-w, --wckeys string**: 指定查询的wckeys，指定多个wckey时用逗号隔开
- **-t, --time string**: 指定输出作业数据的时间单位（默认为minutes）

#### 时间范围过滤
- **-S, --start-time string**: 指定查询开始的时间（默认为前一天00:00:00）格式：`2023-03-14T10:00:00`
- **-E, --end-time string**: 指定查询结束的时间（默认为前一天23:59:59）格式：`2023-03-14T10:00:00`

#### 其他选项
- **-C, --config string**: 配置文件路径（默认：`/etc/crane/config.yaml`）
- **-h, --help**: 显示帮助信息

## 展示账户-qos资源利用率
```bash
creport cluster accountutilizationbyqos [--start-time=...] [--end-time=...] [--account=...] [--qos=...] ...
```
### 输出示例

![creport](../../images/creport/accountutilizationbyqos.png)

### 输出字段

- **CLUSTER**: 集群名
- **ACCOUNT**: 账户名
- **QOS**: Qos名
- **USED**: 用户下每个作业总CPUs*运行时间值的总和
- **ENERGY**: 作业消耗的能源

### 命令行选项

#### 过滤选项
- **-A, --account string**: 指定查询账户，指定多个账户时用逗号隔开
- **-q, --qos string**: 指定查询的Qos，指定多个Qos时用逗号隔开
- **-t, --time string**: 指定输出作业数据的时间单位（默认为minutes）

#### 时间范围过滤
- **-S, --start-time string**: 指定查询开始的时间（默认为前一天00:00:00）格式：`2023-03-14T10:00:00`
- **-E, --end-time string**: 指定查询结束的时间（默认为前一天23:59:59）格式：`2023-03-14T10:00:00`

#### 其他选项
- **-C, --config string**: 配置文件路径（默认：`/etc/crane/config.yaml`）
- **-h, --help**: 显示帮助信息

## 展示集群整体利用率
```bash
creport cluster utilization [--start-time=...] [--end-time=...] ...
```
### 输出示例

![creport](../../images/creport/utilization.png)

### 输出字段

- **CLUSTER**: 集群名
- **ALLOCATE**: 统计区间内，所有作业实际分配的资源总量（CPU分钟），即所有作业分配核数 × 运行分钟累加
- **QOWN**: 统计区间内，节点因故障、维护等不可用的总时间（CPU分钟），即停机核数 ×停机分钟累加
- **PLANEED**: 统计区间内，有作业排队但未分配资源的时间（CPU分钟），通常表示资源紧张或排队溢出
- **REPORTED**: 统计区间内，所有资源的理论最大可用时间（CPU分钟），即集群总核数 × 时间跨度

### 命令行选项

#### 过滤选项
- **-t, --time string**: 指定输出作业数据的时间单位（默认为minutes）

#### 时间范围过滤
- **-S, --start-time string**: 指定查询开始的时间（默认为前一天00:00:00）格式：`2023-03-14T10:00:00`
- **-E, --end-time string**: 指定查询结束的时间（默认为前一天23:59:59）格式：`2023-03-14T10:00:00`

#### 其他选项
- **-C, --config string**: 配置文件路径（默认：`/etc/crane/config.yaml`）
- **-h, --help**: 显示帮助信息

## 展示按账户分组的作业规模分布
```bash
creport job sizesbyaccount [--start-time=...] [--end-time=...] [--account=...] ...
```
### 输出示例

![creport](../../images/creport/sizesbyaccount.png)

### 输出字段

- **CLUSTER**: 集群名
- **ACCOUNT**: 账户名
- **0-49 CPUs**: 0-49CPUs范围CPU分钟
- **50-249 CPUs**: 50-249CPUs范围CPU分钟
- **250-499 CPUs**: 250-499CPUs范围CPU分钟
- **500-999 CPUs**: 500-999CPUs范围CPU分钟
- **>= 1000 CPUs**: 大于等于1000CPUs范围CPU分钟
- **TOTAL CPU TIME**: 特定账户下所有的CPU分钟总和
- **% OF CLUSTER**: 占用集群所有作业CPU分钟数的百分比

### 命令行选项

#### 过滤选项
- **-A, --account string**: 指定查询账户，指定多个账户时用逗号隔开
- **--gid string**: 指定查询的gid, 指定多个gid时用逗号隔开
- **--grouping string**: 以逗号分隔的尺寸分组列表，（默认是50,100,250,500,1000）
- **--printjobcount bool**: 报告将打印作业范围的数量，而不是使用的时间
- **-j, --jobs string**: 指定查询作业号，指定多个作业号时用逗号隔开（如 `-j=2,3,4`）
- **-n, --nodes string**: 指定查询的节点名，指定多个节点时用逗号隔开
- **-p, --partition string**: 指定查询的分区，指定多个分区时用逗号隔开
- **-t, --time string**: 指定输出作业数据的时间单位（默认为minutes）

#### 时间范围过滤
- **-S, --start-time string**: 指定查询开始的时间（默认为前一天00:00:00）格式：`2023-03-14T10:00:00`
- **-E, --end-time string**: 指定查询结束的时间（默认为前一天23:59:59）格式：`2023-03-14T10:00:00`

#### 其他选项
- **-C, --config string**: 配置文件路径（默认：`/etc/crane/config.yaml`）
- **-h, --help**: 显示帮助信息

## 展示按wckey分组的作业规模分布
```bash
creport job sizesbywckey [--start-time=...] [--end-time=...] [--wckeys=...] ...
```
### 输出示例

![creport](../../images/creport/sizesbywckey.png)

### 输出字段

- **CLUSTER**: 集群名
- **ACCOUNT**: 账户名
- **0-49 CPUs**: 0-49CPUs范围CPU分钟
- **50-249 CPUs**: 50-249CPUs范围CPU分钟
- **250-499 CPUs**: 250-499CPUs范围CPU分钟
- **500-999 CPUs**: 500-999CPUs范围CPU分钟
- **>= 1000 CPUs**: 大于等于1000CPUs范围CPU分钟
- **TOTAL CPU TIME**: 特定账户下所有的CPU分钟总和
- **% OF CLUSTER**: 占用集群所有作业CPU分钟数的百分比

### 命令行选项

#### 过滤选项
- **-w --wckeys string**: 指定查询的wckeys，指定多个wckey时用逗号隔开
- **--gid string**: 指定查询的gid, 指定多个gid时用逗号隔开
- **--grouping string**: 以逗号分隔的尺寸分组列表，（默认是50,100,250,500,1000）
- **--printjobcount bool**: 报告将打印作业范围的数量，而不是使用的时间
- **-j, --jobs string**: 指定查询作业号，指定多个作业号时用逗号隔开（如 `-j=2,3,4`）
- **-n, --nodes string**: 指定查询的节点名，指定多个节点时用逗号隔开
- **-p, --partition string**: 指定查询的分区，指定多个分区时用逗号隔开
- **-t, --time string**: 指定输出作业数据的时间单位（默认为minutes）

#### 时间范围过滤
- **-S, --start-time string**: 指定查询开始的时间（默认为前一天00:00:00）格式：`2023-03-14T10:00:00`
- **-E, --end-time string**: 指定查询结束的时间（默认为前一天23:59:59）格式：`2023-03-14T10:00:00`

#### 其他选项
- **-C, --config string**: 配置文件路径（默认：`/etc/crane/config.yaml`）
- **-h, --help**: 显示帮助信息

## 展示按wckey和account分组的作业规模分布
```bash
creport job sizesbyaccountandwckey [--start-time=...] [--end-time=...] [--wckeys=...] ...
```
### 输出示例

![creport](../../images/creport/sizesbyaccountandwckey.png)

### 输出字段

- **CLUSTER**: 集群名
- **ACCOUNT**: 账户名
- **0-49 CPUs**: 0-49CPUs范围CPU分钟
- **50-249 CPUs**: 50-249CPUs范围CPU分钟
- **250-499 CPUs**: 250-499CPUs范围CPU分钟
- **500-999 CPUs**: 500-999CPUs范围CPU分钟
- **>= 1000 CPUs**: 大于等于1000CPUs范围CPU分钟
- **TOTAL CPU TIME**: 特定账户下所有的CPU分钟总和
- **% OF CLUSTER**: 占用集群所有作业CPU分钟数的百分比

### 命令行选项

#### 过滤选项
- **-A, --account string**: 指定查询账户，指定多个账户时用逗号隔开
- **-w --wckeys string**: 指定查询的wckeys，指定多个wckey时用逗号隔开
- **--gid string**: 指定查询的gid, 指定多个gid时用逗号隔开
- **--grouping string**: 以逗号分隔的尺寸分组列表，（默认是50,100,250,500,1000）
- **--printjobcount bool**: 报告将打印作业范围的数量，而不是使用的时间
- **-j, --jobs string**: 指定查询作业号，指定多个作业号时用逗号隔开（如 `-j=2,3,4`）
- **-n, --nodes string**: 指定查询的节点名，指定多个节点时用逗号隔开
- **-p, --partition string**: 指定查询的分区，指定多个分区时用逗号隔开
- **-t, --time string**: 指定输出作业数据的时间单位（默认为minutes）

#### 时间范围过滤
- **-S, --start-time string**: 指定查询开始的时间（默认为前一天00:00:00）格式：`2023-03-14T10:00:00`
- **-E, --end-time string**: 指定查询结束的时间（默认为前一天23:59:59）格式：`2023-03-14T10:00:00`

#### 其他选项
- **-C, --config string**: 配置文件路径（默认：`/etc/crane/config.yaml`）
- **-h, --help**: 显示帮助信息

## 手动触发数据聚合

**用于手动触发数据聚合，仅限root用户触发**

```bash
creport active
```

## 另请参阅

- [cqueue](cqueue.md) - 查看作业队列（当前/挂起作业）
- [cbatch](cbatch.md) - 提交批处理作业
- [ccancel](ccancel.md) - 取消作业
- [ceff](ceff.md) - 查看作业效率统计
- [cacct](cacct.md) - 查询已完成作业
