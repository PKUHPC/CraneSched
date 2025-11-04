# cacct - 查看作业统计信息

**cacct 可以查看集群中作业的统计信息。**

查看集群中所有队列的作业信息（包括所有状态），默认输出 100 条信息。

```bash
cacct
```

**cacct 运行结果展示**

![cacct](../../images/cacct/cacct.png)

## 输出字段

- **TaskId**: 作业号
- **TaskName**: 作业名
- **Partition**: 作业所在分区
- **Account**: 作业所属账户
- **AllocCPUs**: 作业分配的 CPU 数量
- **State**: 作业状态
- **ExitCode**: 作业退出码

## 命令行选项

### 过滤选项
- **-j, --job string**: 指定查询作业号，指定多个作业号时用逗号隔开（如 `-j=2,3,4`）
- **-n, --name string**: 指定查询作业名，指定多个作业名时用逗号隔开
- **-u, --user string**: 指定查询某个用户的作业，指定多个用户时用逗号隔开
- **-A, --account string**: 指定查询作业的所属账户，指定多个账户时用逗号隔开
- **-p, --partition string**: 指定要查看的分区，多个分区名用逗号隔开（默认为全部）
- **-q, --qos string**: 指定要查看的 QoS，多个 QoS 用逗号隔开（默认为全部）

### 时间范围过滤
- **-s, --submit-time string**: 筛选提交时间在特定时间段内的作业。支持闭区间（格式：`2024-01-02T15:04:05~2024-01-11T11:12:41`）或半开区间（格式：`2024-01-02T15:04:05~` 或 `~2024-01-11T11:12:41`）
- **-S, --start-time string**: 筛选开始时间在特定时间段内的作业。格式同 submit-time
- **-E, --end-time string**: 筛选结束时间在指定时间之前的作业。格式：`~2023-03-14T10:00:00`

### 状态过滤
- **-t, --state string**: 指定要查看的作业状态。支持的状态：
  - `pending` 或 `p`：挂起中的作业
  - `running` 或 `r`：运行中的作业
  - `completed` 或 `c`：已完成的作业
  - `failed` 或 `f`：失败的作业
  - `cancelled` 或 `x`：已取消的作业
  - `time-limit-exceeded` 或 `t`：超时的作业
  - `all`：所有状态（默认）

### 输出格式
- **-o, --format string**: 指定输出格式。由百分号（%）后接一个字符或字符串标识。在 % 和格式字符/字符串之间用点（.）和数字，可指定字段的最小宽度。支持的格式标识符（不区分大小写）：
  - **%a / %Account**: 显示作业关联的账户
  - **%c / %AllocCpus**: 显示作业已分配的 CPU 数量
  - **%deadline/%Deadline**: 显示作业的截止时间
  - **%e / %ExitCode**: 显示作业退出码（特殊格式：exitcode[:signal]）
  - **%h / %ElapsedTime**: 显示作业自启动以来的已用时间
  - **%j / %JobId**: 显示作业 ID
  - **%k / %Comment**: 显示作业的备注
  - **%l / %NodeList**: 显示作业正在运行的节点列表
  - **%m / %TimeLimit**: 显示作业的时间限制
  - **%n / %MemPerNode**: 显示作业每个节点请求的内存量
  - **%N / %NodeNum**: 显示作业请求的节点数量
  - **%n / %Name**: 显示作业名称
  - **%P / %Partition**: 显示作业运行所在的分区
  - **%p / %Priority**: 显示作业的优先级
  - **%Q / %QOS**: 显示作业的服务质量（QoS）级别
  - **%R / %Reason**: 显示作业挂起的原因
  - **%r / %ReqNodes**: 显示作业请求的节点
  - **%S / %StartTime**: 显示作业的开始时间
  - **%s / %SubmitTime**: 显示作业的提交时间
  - **%t / %State**: 显示作业的当前状态
  - **%T / %JobType**: 显示作业类型
  - **%u / %Uid**: 显示作业的 UID
  - **%U / %User**: 显示提交作业的用户
  - **%x / %ExcludeNodes**: 显示作业排除的节点
  - 宽度规格说明：`%.5j`（右对齐，最小宽度 5）或 `%5j`（左对齐，最小宽度 5）
  - 示例：`--format "%.5j %.20n %t"` 会输出作业 ID（最小宽度 5）、名称（最小宽度 20）和状态

### 显示选项
- **-F, --full**: 显示完整信息（不截断字段）
- **-N, --no-header**: 输出时隐藏表头
- **-m, --max-lines uint32**: 指定输出结果的最大条数（如 `-m=500` 表示最多输出 500 行）
- **--json**: 以 JSON 格式输出
- **--deadline string**: 显示作业的截止时间

### 其他选项
- **-C, --config string**: 配置文件路径（默认：`/etc/crane/config.yaml`）
- **-h, --help**: 显示帮助信息
- **-v, --version**: 显示版本号

## 使用示例

### 基本查询

查看所有作业：
```bash
cacct
```
![cacct](../../images/cacct/cacct.png)

### 帮助信息

显示帮助：
```bash
cacct -h
```
![cacct](../../images/cacct/h.png)

### 隐藏表头

不显示表头的输出：
```bash
cacct -N
```
![cacct](../../images/cacct/N.png)

### 时间范围过滤

按开始时间范围过滤：
```bash
cacct -S=2024-07-22T10:00:00~2024-07-24T10:00:00
```
![cacct](../../images/cacct/S.png)

按结束时间范围过滤：
```bash
cacct -E=2024-07-22T10:00:00~2024-07-24T10:00:00
```
![cacct](../../images/cacct/E.png)

### 作业号过滤

查询特定作业号：
```bash
cacct -j=30618,30619,30620
```
![cacct](../../images/cacct/j.png)

### 用户过滤

按用户查询作业：
```bash
cacct -u=cranetest
```
![cacct](../../images/cacct/u.png)

### 账户过滤

按账户查询作业：
```bash
cacct -A=CraneTest
```
![cacct](../../images/cacct/A.png)

### 限制输出行数

限制为 10 行：
```bash
cacct -m=10
```
![cacct](../../images/cacct/m.png)

### 分区过滤

查询特定分区的作业：
```bash
cacct -p GPU
```
![cacct](../../images/cacct/p.png)

### 作业名过滤

按作业名查询：
```bash
cacct -n=Test_Job
```
![cacct](../../images/cacct/nt.png)

### 自定义格式

指定自定义输出格式：
```bash
cacct -o="%j %.10n %P %a %t"
```
![cacct](../../images/cacct/o.png)

### 组合过滤

组合账户和最大行数：
```bash
cacct -A ROOT -m 10
```
![cacct](../../images/cacct/am.png)

多个过滤器与完整输出：
```bash
cacct -m 10 -j 783925,783889 -t=c -F
```
![cacct](../../images/cacct/mj.png)

按名称查询：
```bash
cacct -n test
```
![cacct](../../images/cacct/ntest.png)

按 QoS 查询：
```bash
cacct -q test_qos
```
![cacct](../../images/cacct/qt.png)

复杂组合查询：
```bash
cacct -m 10 -E=2024-10-08T10:00:00~2024-10-10T110:00:00 -p CPU -t c
```
![cacct](../../images/cacct/me.png)

## 高级功能

### JSON 输出

以 JSON 格式获取结果便于解析：
```bash
cacct --json -j 12345
```

### 时间范围查询

查询特定时间范围内提交的作业：
```bash
cacct -s=2024-01-01T00:00:00~2024-01-31T23:59:59
```

查询在特定时间之后开始的作业：
```bash
cacct -S=2024-01-15T00:00:00~
```

查询在特定时间之前结束的作业：
```bash
cacct -E=~2024-01-31T23:59:59
```

### 状态过滤示例

仅查看已完成的作业：
```bash
cacct -t completed
```

查看失败和取消的作业：
```bash
cacct -t failed,cancelled
```

查看超时的作业：
```bash
cacct -t time-limit-exceeded
```

### 格式规格详解

格式字符串支持宽度控制：
- `%5j` - 左对齐，最小宽度 5
- `%.5j` - 右对齐，最小宽度 5

多个宽度规格的示例：
```bash
cacct -o="%.8j %20n %-10P %.15U %t"
```

## 另请参阅

- [cqueue](cqueue.md) - 查看作业队列（当前/挂起作业）
- [cbatch](cbatch.md) - 提交批处理作业
- [ccancel](ccancel.md) - 取消作业
- [ceff](ceff.md) - 查看作业效率统计
