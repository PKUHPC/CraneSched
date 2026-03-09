# cacct 查看作业统计信息

cacct显示集群中作业和作业步的统计信息。它查询所有作业状态，包括已完成、失败和取消的作业。输出会自动包含作业及其相关的作业步。

查看集群中所有作业和作业步信息：

```bash
cacct
```

## 选项

**-h, --help**

:   **适用于：** `作业`, `作业步`  
显示cacct命令的帮助信息。

**-v, --version**

:   **适用于：** `作业`, `作业步`  
显示cacct版本信息。

**-C, --config=&lt;path&gt;**

:   **适用于：** `作业`, `作业步`  
配置文件路径。默认值："/etc/crane/config.yaml"。

**-j, --job=&lt;jobid1,jobid2,...&gt;**

:   **适用于：** `作业`, `作业步`  
指定查询的作业ID（逗号分隔列表）。例如，`-j=2,3,4`。查询作业时，会按作业ID过滤。输出将包含匹配的作业及其相关的作业步。支持使用作业步ID格式
`jobid.stepid`查询特定作业步。

**-n, --name=&lt;name1,name2,...&gt;**

:   **适用于：** `作业`, `作业步`  
指定查询的作业名（逗号分隔列表，用于多个名称）。

**-u, --user=&lt;username1,username2,...&gt;**

:   **适用于：** `作业`, `作业步`  
指定查询的用户（逗号分隔列表，用于多个用户）。按指定的用户名过滤作业和作业步。

**-A, --account=&lt;account1,account2,...&gt;**

:   **适用于：** `作业`, `作业步`  
指定查询的账户（逗号分隔列表，用于多个账户）。按指定的账户过滤作业和作业步。

**-p, --partition=&lt;partition1,partition2,...&gt;**

:   **适用于：** `作业`, `作业步`  
指定要查看的分区（逗号分隔列表，用于多个分区）。默认：所有分区。

**-q, --qos=&lt;qos1,qos2,...&gt;**

:   **适用于：** `作业`, `作业步`  
指定要查看的QoS（逗号分隔列表，用于多个QoS）。默认：所有QoS级别。

**-t, --state=&lt;state&gt;**

:   **适用于：** `作业`, `作业步`  
指定要查看的作业状态。支持的状态：'pending'或'p'、'running'或'r'、'completed'或'c'、'failed'或'f'、'cancelled'或'x'
、'time-limit-exceeded'或't'以及'all'。默认：'all'。可以以逗号分隔列表的形式指定多个状态。

**-s, --submit-time=&lt;time_range&gt;**

:   **适用于：** `作业`, `作业步`  
按提交时间范围过滤作业。支持闭区间（格式：`2024-01-02T15:04:05~2024-01-11T11:12:41`）或半开区间（格式：
`2024-01-02T15:04:05~`  
表示特定时间之后，或 `~2024-01-11T11:12:41` 表示特定时间之前）。

**-S, --start-time=&lt;time_range&gt;**

:   **适用于：** `作业`, `作业步`  
按开始时间范围过滤作业。格式同submit-time。

**-E, --end-time=&lt;time_range&gt;**

:   **适用于：** `作业`, `作业步`  
按结束时间范围过滤作业。格式同submit-time。例如，`~2023-03-14T10:00:00` 过滤在指定时间之前结束的作业。

**-w, --nodelist=&lt;node1,node2,...&gt;**

:   **适用于：** `作业`, `作业步`  
指定要查看的节点名称（逗号分隔列表或模式，如node[1-10]）。默认：所有节点。

**--type=&lt;type1,type2,...&gt;**

:   **适用于：** `作业`, `作业步`  
指定要查看的任务类型（逗号分隔列表）。有效值：'Interactive'、'Batch'、'Container'。默认：所有类型。

**-F, --full**

:   **适用于：** `作业`, `作业步`  
显示完整信息，不截断字段。默认情况下，每个单元格仅显示30个字符。

**-N, --noheader**

:   **适用于：** `作业`, `作业步`  
输出时隐藏表头。

**-m, --max-lines=&lt;number&gt;**

:   **适用于：** `作业`, `作业步`  
指定输出结果的最大条数。例如，`-m=500` 将输出限制为500行。默认：100条。

**--json**

:   **适用于：** `作业`, `作业步`  
以JSON格式输出命令执行结果，而不是表格格式。

**-o, --format=&lt;format_string&gt;**

:   **适用于：** `作业`, `作业步`  
使用格式说明符自定义输出格式。字段由百分号(%)后跟字符或字符串标识。格式规范语法：`%[.]<size><type>`。不带大小：字段使用自然宽度。仅带大小（
`%5j`）：最小宽度，左对齐。带点和大小（`%.5j`）：最小宽度，右对齐。可用的格式标识符请参见下面的格式说明符部分。

## 默认输出字段

显示默认格式时，会显示以下字段：

- **JobId**：作业或作业步标识（格式：作业为jobid，作业步为jobid.stepid）
- **JobName**：作业或作业步名称
- **Partition**：作业/作业步运行的分区
- **Account**：作业/作业步计费的账户
- **AllocCPUs**：分配的CPU数量
- **State**：作业/作业步状态（如COMPLETED、FAILED、CANCELLED）
- **ExitCode**：退出码（格式：exitcode:signal，见[退出码参考](../reference/exit_code.md)）

## 格式说明符

支持以下格式标识符（不区分大小写）：

| 标识符                   | 描述                           |
|-----------------------|------------------------------|
| %a / %Account         | 与作业/作业步关联的账户                 |
| %C / %ReqCpus         | 请求的CPU数量                     |
| %c / %AllocCpus       | 分配的CPU数量                     |
| %D / %ElapsedTime     | 作业/作业步启动以来的经过时间              |
| %E / %EndTime         | 作业/作业步的结束时间                  |
| %e / %ExitCode        | 退出码（格式：exitcode:signal）      |
| %h / %Held            | 作业的保持状态                      |
| %j / %JobID           | 作业ID（或作业步ID，格式为jobid.stepid） |
| %K / %Wckey           | 工作负载特征键                      |
| %k / %Comment         | 作业的备注                        |
| %L / %NodeList        | 作业/作业步运行的节点列表                |
| %l / %TimeLimit       | 作业/作业步的时间限制                  |
| %M / %ReqMemPerNode   | 每个节点请求的内存                    |
| %m / %AllocMemPerNode | 每个节点分配的内存                    |
| %N / %NodeNum         | 节点数量                         |
| %n / %JobName         | 作业/作业步的名称                    |
| %P / %Partition       | 与作业/作业步关联的分区                 |
| %p / %Priority        | 作业的优先级                       |
| %q / %Qos             | 服务质量级别                       |
| %R / %Reason          | pending状态的原因                 |
| %r / %ReqNodes        | 请求的节点                        |
| %S / %StartTime       | 作业/作业步的开始时间                  |
| %s / %SubmitTime      | 作业的提交时间                      |
| %t / %State           | 作业/作业步的当前状态                  |
| %T / %JobType         | 作业类型（如Batch、Interactive）     |
| %U / %UserName        | 提交作业的用户名                     |
| %u / %Uid             | 用户ID                         |
| %x / %ExcludeNodes    | 从作业中排除的节点                    |
| %X / %Exclusive       | 作业的独占状态                      |

## 使用示例

### 基本查询

**查看所有作业和作业步：**

```bash
cacct
```

```text
[cranetest@crane01 ~]$ cacct
JOBID   JOBNAME   PARTITION ACCOUNT ALLOCCPUS STATE     EXITCODE
222112  Test_Job  CPU       PKU     1.00     Completed 0:0
222111  Test_Job  CPU       PKU     1.00     Completed 0:0
222110  Test_Job  CPU       PKU     1.00     Completed 0:0
222109  Test_Job  CPU       PKU     1.00     Completed 0:0
222108  Test_Job  CPU       PKU     1.00     Completed 0:0
222107  Test_Job  CPU       PKU     1.00     Completed 0:0
222106  Test_Job  CPU       PKU     1.00     Completed 0:0
222105  Test_Job  CPU       PKU     1.00     Completed 0:0
222104  Test_Job  CPU       PKU     1.00     Completed 0:0
222103  Test_Job  CPU       PKU     1.00     Completed 0:0
```

**显示帮助：**

```bash
cacct -h
```

```text
[cranetest@crane01 ~]$ cacct -h
Display the recent job information

Usage:
  cacct [flags]

Flags:
  -A, --account string       Select accounts to view (comma separated list)
  -C, --config string        Path to configuration file (default "/etc/crane/config.yaml")
  -E, --end-time string      Filter jobs with an end time within a certain time period, which can use closed intervals(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41) or semi open intervals(timeFormat: 2024-01-02T15:04:05~ or ~2024-01-11T11:12:41)
  -o, --format string        Specify the output format.
                             
                             Fields are identified by a percent sign (%) followed by a character or string.
                             Format specification: %[[.]size]type
                               - Without size: field uses natural width
                               - With size only (%5j): field uses minimum width, left-aligned (padding on right)
                               - With dot and size (%.5j): field uses minimum width, right-aligned (padding on left)
                             
                             Supported format identifiers or string, string case insensitive:
                                %a/%Account           - Display the account associated with the job.
                                %C/%ReqCpus           - Display the number of requested CPUs, formatted to two decimal places
                                %c/%AllocCpus         - Display the number of allocated CPUs, formatted to two decimal places.
                                %D/%ElapsedTime       - Display the elapsed time from the start of the job.
                                %E/%EndTime           - Display the end time of the job.
                                %e/%ExitCode          - Display the exit code of the job. 
                                                          If the exit code is based on a specific base (e.g., kCraneExitCodeBase),
                                                          it formats as "0:<code>" or "<code>:0" based on the condition.
                                %h/%Held              - Display the hold status of the job.
                                %j/%JobID             - Display the ID of the job.
                                %K/%Wckey             - Display the wckey of the job.
                                %k/%Comment           - Display the comment of the job.
                                %L/%NodeList          - Display the list of nodes the job is running on.
                                %l/%TimeLimit         - Display the time limit of the job.
                                %M/%ReqMemPerNode     - Display the requested mem per node of the job.
                                %m/%AllocMemPerNode   - Display the allocted mem per node of the job.
                                %N/%NodeNum           - Display the node num of the job.
                                %n/%JobName           - Display the name of the job.
                                %P/%Partition         - Display the partition associated with the job.
                                %p/%Priority          - Display the priority of the job.
                                %q/%Qos               - Display the QoS of the job.
                                %R/%Reason            - Display the reason of pending.
                                %r/%ReqNodes          - Display the reqnodes of the job.
                                %S/%StartTime         - Display the start time of the job.
                                %s/%SubmitTime        - Display the submit time num of the job.
                                %t/%State             - Display the state of the job.
                                %T/%JobType           - Display the job type.
                                %U/%UserName          - Display the username of the job.
                                %u/%Uid               - Display the uid of the job.
                                %x/%ExcludeNodes      - Display the excludenodes of the job.
                                %X/%Exclusive         - Display the exclusive status of the job.
                             
                             Examples:
                               --format "%j %n %t"              # Natural width for all fields
                               --format "%5j %20n %t"           # Left-aligned: JobID (min 5), JobName (min 20), State
                               --format "%.5j %.20n %t"         # Right-aligned: JobID (min 5), JobName (min 20), State
                               --format "ID:%8j | Name:%.15n"   # Mixed: left-aligned JobID, right-aligned JobName with prefix
                             
                             Note: If the format is invalid or unrecognized, the program will terminate with an error message.
                             
  -F, --full                 Display full information (If not set, only display 30 characters per cell)
  -h, --help                 help for cacct
  -j, --job string           Select job ids to view (comma separated list), default is all
      --json                 Output in JSON format
  -m, --max-lines uint32     Limit the number of lines in the output, 0 means no limit (default 1000)
  -n, --name string          Select job names to view (comma separated list), default is all
  -w, --nodelist string      Specify node names to view (comma separated list or patterns like node[1-10]), default is all
  -N, --noheader             Do not print header line in the output
  -p, --partition string     Specify partitions to view (comma separated list), default is all
  -q, --qos string           Specify QoS of jobs to view (comma separated list), default is all.
  -S, --start-time string    Filter jobs with a start time within a certain time period, which can use closed intervals(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41) or semi open intervals(timeFormat: 2024-01-02T15:04:05~ or ~2024-01-11T11:12:41)
  -t, --state string         Specify job states to view, supported states: pending(p), running(r), completed(c), failed(f), cancelled(x), time-limit-exceeded(t), all. (default "all")
  -s, --submit-time string   Filter jobs with a submit time within a certain time period, which can use closed intervals(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41) or semi open intervals(timeFormat: 2024-01-02T15:04:05~ or ~2024-01-11T11:12:41)
      --type string          Specify task types to view (comma separated list), 
                             valid values are 'Interactive', 'Batch', 'Container', default is all types
  -u, --user string          Select users to view (comma separated list)
  -v, --version              version for cacct
```

**隐藏表头：**

```bash
cacct -N
```
```text
[cranetest@crane01 ~]$ cacct -N
222112 Test_Job  CPU PKU     1.00 Completed   0:0
222111 Test_Job  CPU PKU     1.00 Completed   0:0
222110 Test_Job  CPU PKU     1.00 Completed   0:0
222109 Test_Job  CPU PKU     1.00 Completed   0:0
222108 Test_Job  CPU PKU     1.00 Completed   0:0
222107 Test_Job  CPU PKU     1.00 Completed   0:0
222106 Test_Job  CPU PKU     1.00 Completed   0:0
222105 Test_Job  CPU PKU     1.00 Completed   0:0
222104 Test_Job  CPU PKU     1.00 Completed   0:0
222103 Test_Job  CPU PKU     1.00 Completed   0:0
222102 Test_Job  CPU PKU     1.00 Completed   0:0
222101 Test_Job  CPU PKU     1.00 Completed   0:0
222100 Test_Job  CPU PKU     1.00 Completed   0:0
222099 Test_Job  CPU PKU     1.00 Completed   0:0
```

### 按ID和名称过滤

**查询特定作业ID：**

```bash
cacct -j=30618,30619,30620
```

```text
[cranetest@crane01 ~]$ cacct -j=30618,30619,30620
JOBID JOBNAME  PARTITION ACCOUNT ALLOCCPUS STATE          EXITCODE
30620 Test_Job CPU       ROOT    0.00     Cancelled      0:0
30619 Test_Job CPU       ROOT    2.00     ExceededTimeLimit 0:15
30618 Test_Job CPU       ROOT    2.00     Cancelled      0:15
```

**按作业名查询：**

```bash
cacct -n=Test_Job
```

```text
[cranetest@crane01 ~]$ cacct -n=Test_Job
JOBID   JOBNAME   PARTITION ACCOUNT ALLOCCPUS STATE     EXITCODE
222112  Test_Job  CPU       PKU     1.00     Completed 0:0
222111  Test_Job  CPU       PKU     1.00     Completed 0:0
222110  Test_Job  CPU       PKU     1.00     Completed 0:0
222109  Test_Job  CPU       PKU     1.00     Completed 0:0
222108  Test_Job  CPU       PKU     1.00     Completed 0:0
222107  Test_Job  CPU       PKU     1.00     Completed 0:0
222106  Test_Job  CPU       PKU     1.00     Completed 0:0
222105  Test_Job  CPU       PKU     1.00     Completed 0:0
222104  Test_Job  CPU       PKU     1.00     Completed 0:0
222103  Test_Job  CPU       PKU     1.00     Completed 0:0
222102  Test_Job  CPU       PKU     1.00     Completed 0:0
222101  Test_Job  CPU       PKU     1.00     Completed 0:0
222100  Test_Job  CPU       PKU     1.00     Completed 0:0
```

**按名称模式查询：**

```bash
cacct -n test
```
```text
[root@cranetest-rocky01 zhouhao]# cacct -n test
JOBID     JOBNAME PARTITION ACCOUNT ALLOCCPUS STATE     EXITCODE
1276686  test    CPU       ROOT    0.00     Cancelled 0:0
```

### 按用户和账户过滤

**按用户查询作业：**

```bash
cacct -u=cranetest
```

```text
[cranetest@crane01 ~]$ cacct -u=cranetest
JOBID   JOBNAME     PARTITION ACCOUNT   ALLOCCPUS STATE          EXITCODE
30689   Interactive CPU       CraneTest 2.00     Completed      0:0
30688   Interactive CPU       CraneTest 2.00     Completed      0:64
30687   Test_Job    CPU       CraneTest 0.00     Cancelled      0:0
30686   Test_Job    CPU       CraneTest 2.00     Cancelled      0:15
30685   Test_Job    CPU       CraneTest 2.00     ExceedTimeLimit 0:15
30684   Test_Job    CPU       CraneTest 2.00     Cancelled      0:15
30683   Test_Job    CPU       CraneTest 2.00     Cancelled      0:15
```

**按账户查询作业：**

```bash
cacct -A=CraneTest
```

```text
[cranetest@crane01 ~]$ cacct -A=CraneTest
JOBID   JOBNAME     PARTITION ACCOUNT   ALLOCCPUS STATE          EXITCODE
30689   Interactive CPU       CraneTest 2.00     Completed      0:0
30688   Interactive CPU       CraneTest 2.00     Completed      0:64
30687   Test_Job    CPU       CraneTest 0.00     Cancelled      0:0
30686   Test_Job    CPU       CraneTest 2.00     Cancelled      0:15
30685   Test_Job    CPU       CraneTest 2.00     ExceedTimeLimit 0:15
30684   Test_Job    CPU       CraneTest 2.00     Cancelled      0:15
30683   Test_Job    CPU       CraneTest 2.00     Cancelled      0:15
```

**组合账户和最大行数：**

```bash
cacct -A ROOT -m 10
```

```text
[zhouhao@cranetest-rocky02 ~]$ cacct -A ROOT -m 10
JOBID     JOBNAME   PARTITION ACCOUNT ALLOCCPUS STATE    EXITCODE
1418574   CPU       CPU       ROOT    0.00     Pending  0:0
1418573   CPU       CPU       ROOT    0.00     Pending  0:0
1418572   CPU       CPU       ROOT    0.00     Pending  0:0
1418571   CPU       CPU       ROOT    0.00     Pending  0:0
1418570   CPU       CPU       ROOT    0.00     Pending  0:0
1418569   CPU       CPU       ROOT    0.00     Pending  0:0
1418568   CPU       CPU       ROOT    0.00     Pending  0:0
1418567   CPU       CPU       ROOT    0.00     Pending  0:0
1418566   CPU       CPU       ROOT    0.00     Pending  0:0
1412536   Test_Job  CPU       ROOT    0.00     Pending  0:0
```

### 按分区和QoS过滤

**查询特定分区的作业：**

```bash
cacct -p GPU
```
```text
[cranetest@crane01 ~]$ cacct -p GPU
JOBID   JOBNAME     PARTITION ACCOUNT   ALLOCCPUS STATE          EXITCODE
30736   Interactive GPU       acct-test 2.00     Completed      0:64
30735   Interactive GPU       acct-test 0.00     Cancelled      0:0
30734   Interactive GPU       acct-test 0.00     Cancelled      0:0
30733   Interactive GPU       acct-test 2.00     Completed      0:64
30730   Test_Job    GPU       CraneTest 1.00     ExceedTimeLimit 0:15
30691   Interactive GPU       yanyan    1.00     Completed      0:64
30690   Interactive GPU       yanyan    1.00     Completed      0:64
21670   Test_Job    GPU       ROOT      1.00     Completed      0:0
21669   Test_Job    GPU       ROOT      1.00     Completed      0:0
21668   Test_Job    GPU       ROOT      1.00     Completed      0:0
21667   Test_Job    GPU       ROOT      1.00     Completed      0:0
21666   Test_Job    GPU       ROOT      1.00     Completed      0:0
```

**按QoS查询：**

```bash
cacct -q test_qos
```

```text
[root@cranetest01 zhouhao]# cacct -q test_qos
JOBID JOBNAME     PARTITION ACCOUNT ALLOCCPUS STATE          EXITCODE
231              CPU       ROOT    2.00     ExceedTimeLimit 0:15
230              CPU       ROOT    2.00     Cancelled      0:15
229              CPU       ROOT    2.00     ExceedTimeLimit 0:15
217              CPU       ROOT    2.00     Completed      0:0
216              CPU       ROOT    2.00     Completed      0:0
188  test_crun    computing ROOT    2.00     Completed      0:0
185  test_calloc  CPU       ROOT    8.00     Completed      0:64
183  Interactive  CPU       ROOT    2.00     Completed      0:64
182  Interactive  CPU       ROOT    2.00     Completed      0:64
179  Test_Job     CPU       ROOT    4.00     Completed      0:0
178  Test_Job     CPU       ROOT    4.00     Completed      0:0
177  Test_Job     CPU       ROOT    1.00     Completed      0:0
```

### 时间范围过滤

**按开始时间范围过滤：**

```bash
cacct -S=2024-07-22T10:00:00~2024-07-24T10:00:00
```

```text
[cranetest@crane01 ~]$ cacct -S=2024-07-22T10:00:00~2024-07-24T10:00:00
JOBID   JOBNAME     PARTITION ACCOUNT ALLOCCPUS STATE          EXITCODE STARTTIME
30680   Test_Job    CPU       yanyan  2.00     ExceedTimeLimit 0:15   2024-07-24 09:48:27 +0800 CST
30679   Test_Job    CPU       yanyan  2.00     ExceedTimeLimit 0:15   2024-07-24 09:47:51 +0800 CST
30678   Interactive CPU       yanyan  2.00     Completed      0:0    2024-07-24 09:45:23 +0800 CST
30677   Interactive CPU       yanyan  2.00     Completed      0:64   2024-07-24 09:44:03 +0800 CST
30674   Test_Job    CPU       yanyan  2.00     Cancelled      0:15   2024-07-24 09:38:30 +0800 CST
30673   Test_Job    CPU       yanyan  2.00     Cancelled      0:15   2024-07-24 09:38:29 +0800 CST
30672   Test_Job    CPU       yanyan  2.00     Failed         0:66   2024-07-24 09:38:29 +0800 CST
30671   Test_Job    CPU       yanyan  2.00     Cancelled      0:15   2024-07-24 09:24:56 +0800 CST
30670   Test_Job    CPU       yanyan  2.00     Cancelled      0:15   2024-07-24 09:24:54 +0800 CST
30669   Interactive CPU       ROOT    1.00     Completed      0:0    2024-07-23 14:43:20 +0800 CST
30668   Interactive CPU       ROOT    2.00     Completed      0:64   2024-07-23 14:43:12 +0800 CST
30667   Interactive CPU       ROOT    1.00     Completed      0:0    2024-07-23 14:42:26 +0800 CST
30666   Interactive CPU       ROOT    1.00     Completed      0:0    2024-07-23 14:42:22 +0800 CST
30665   Interactive CPU       ROOT    1.00     Completed      0:0    2024-07-23 14:42:14 +0800 CST
30664   Interactive CPU       ROOT    2.00     Completed      0:64   2024-07-23 14:42:06 +0800 CST
30663   Interactive CPU       ROOT    1.00     Completed      0:0    2024-07-23 14:41:28 +0800 CST
30662   Interactive CPU       ROOT    1.00     Completed      0:0    2024-07-23 14:41:27 +0800 CST
30661   Interactive CPU       ROOT    1.00     Completed      0:0    2024-07-23 14:41:19 +0800 CST
```

**按结束时间范围过滤：**

```bash
cacct -E=2024-07-22T10:00:00~2024-07-24T10:00:00
```

```text
[cranetest@crane01 ~]$ cacct -E=2024-07-22T10:00:00~2024-07-24T10:00:00
JOBID   JOBNAME     PARTITION ACCOUNT ALLOCCPUS STATE     EXITCODE ENDTIME
30678   Interactive CPU       yanyan  2.00     Completed 0:0    2024-07-24 09:45:42 +0800 CST
30677   Interactive CPU       yanyan  2.00     Completed 0:64   2024-07-24 09:44:10 +0800 CST
30676   Interactive CPU       yanyan  0.00     Cancelled 0:0    2024-07-24 09:27:25 +0800 CST
30675   Test_Job    CPU       yanyan  0.00     Cancelled 0:0    2024-07-24 09:38:38 +0800 CST
30674   Test_Job    CPU       yanyan  2.00     Cancelled 0:15   2024-07-24 09:38:11 +0800 CST
30673   Test_Job    CPU       yanyan  2.00     Cancelled 0:15   2024-07-24 09:39:57 +0800 CST
30672   Test_Job    CPU       yanyan  2.00     Failed    0:66   2024-07-24 09:38:29 +0800 CST
30671   Test_Job    CPU       yanyan  2.00     Cancelled 0:15   2024-07-24 09:38:28 +0800 CST
30670   Test_Job    CPU       yanyan  2.00     Cancelled 0:15   2024-07-24 09:38:28 +0800 CST
30669   Interactive CPU       ROOT    1.00     Completed 0:0    2024-07-23 14:43:21 +0800 CST
30668   Interactive CPU       ROOT    2.00     Completed 0:64   2024-07-23 16:54:40 +0800 CST
30667   Interactive CPU       ROOT    1.00     Completed 0:0    2024-07-23 14:42:27 +0800 CST
30666   Interactive CPU       ROOT    1.00     Completed 0:0    2024-07-23 14:42:23 +0800 CST
30665   Interactive CPU       ROOT    1.00     Completed 0:0    2024-07-23 14:42:15 +0800 CST
```

**查询在时间范围内提交的作业：**

```bash
cacct -s=2024-01-01T00:00:00~2024-01-31T23:59:59
```

**查询在特定时间之后开始的作业：**

```bash
cacct -S=2024-01-15T00:00:00~
```

**查询在特定时间之前结束的作业：**

```bash
cacct -E=~2024-01-31T23:59:59
```

### 状态过滤

**仅查看已完成的作业：**

```bash
cacct -t completed
```

**查看失败和取消的作业：**

```bash
cacct -t failed,cancelled
```

**查看超时的作业：**

```bash
cacct -t time-limit-exceeded
```

**按作业类型过滤：**

```bash
# 仅查看容器作业
cacct --type Container

# 查看批处理作业
cacct --type Batch

# 查看交互式作业
cacct --type Interactive
```

!!! tip "容器作业管理"
    除基本查询外，更多容器专用操作参见 [ccon 命令手册](ccon.md)。

### 输出控制

**限制输出到10行：**

```bash
cacct -m=10
```

```text
[cranetest@crane01 ~]$ cacct -m=10
JOBID   JOBNAME   PARTITION ACCOUNT ALLOCCPUS STATE     EXITCODE
222112  Test_Job  CPU       PKU     1.00     Completed 0:0
222111  Test_Job  CPU       PKU     1.00     Completed 0:0
222110  Test_Job  CPU       PKU     1.00     Completed 0:0
222109  Test_Job  CPU       PKU     1.00     Completed 0:0
222108  Test_Job  CPU       PKU     1.00     Completed 0:0
222107  Test_Job  CPU       PKU     1.00     Completed 0:0
222106  Test_Job  CPU       PKU     1.00     Completed 0:0
222105  Test_Job  CPU       PKU     1.00     Completed 0:0
222104  Test_Job  CPU       PKU     1.00     Completed 0:0
222103  Test_Job  CPU       PKU     1.00     Completed 0:0
```

**JSON输出：**

```bash
cacct --json -j 12345
```

### 自定义格式输出

**指定自定义输出格式：**

```bash
cacct -o="%j %.10n %P %a %t"
```

```text
[cranetest@crane01 ~]$ cacct -o="%j %.10n %P %a %t"
JOBID   JOBNAME     PARTITION ACCOUNT STATE
30558   Test_Jo     CPU       PKU     Completed
30571   Test_Jo     CPU       PKU     Completed
30570   Test_Jo     CPU       PKU     Completed
30569   Test_Jo     CPU       PKU     Completed
30568   Test_Jo     CPU       PKU     Completed
30567   Test_Jo     CPU       PKU     Completed
30566   Test_Jo     CPU       PKU     Completed
30565   Test_Jo     CPU       PKU     Completed
30564   Test_Jo     CPU       PKU     Completed
30563   Test_Jo     CPU       PKU     Completed
30562   Test_Jo     CPU       PKU     Completed
```

**所有字段使用自然宽度：**

```bash
cacct --format "%j %n %t"
```

**左对齐，带最小宽度：**

```bash
cacct --format "%5j %20n %t"
```

**右对齐，带最小宽度：**

```bash
cacct --format "%.5j %.20n %t"
```

**带标签的混合格式：**

```bash
cacct -o="%.8j %20n %-10P %.15U %t"
```

### 组合过滤

**多个过滤器与完整输出：**

```bash
cacct -m 10 -j 783925,783889 -t=c -F
```

```text
[root@cranetest-rocky01 zhouhao]# cacct -m 10 -j 783925,783889,783884,106040,106035 -t=c -F
JOBID   JOBNAME PARTITION ACCOUNT ALLOCCPUS STATE     EXITCODE
783925          CPU       ROOT    1.00     Completed 0:0
783889          CPU       ROOT    1.00     Completed 0:0
783884          CPU       ROOT    2.00     Completed 0:0
```

**复杂组合查询：**

```bash
cacct -m 10 -E=2024-10-08T10:00:00~2024-10-10T10:00:00 -p CPU -t c
```

```text
[root@cranetest-rocky01 zhouhao]# cacct -m 10 -E=2024-10-08T10:00:00~2024-10-10T10:00:00 -p CPU -t=c
JOBID     JOBNAME PARTITION ACCOUNT ALLOCCPUS STATE     EXITCODE ENDTIME
783446            CPU       ROOT    1.00     Completed 0:0    2024-10-10 09:55:47 +0800 CST
783433            CPU       ROOT    1.00     Completed 0:0    2024-10-10 09:57:10 +0800 CST
783309            CPU       ROOT    1.00     Completed 0:0    2024-10-10 09:29:07 +0800 CST
783272            CPU       ROOT    1.00     Completed 0:0    2024-10-10 07:08:05 +0800 CST
783267            CPU       ROOT    1.00     Completed 0:0    2024-10-10 07:55:35 +0800 CST
783249            CPU       ROOT    1.00     Completed 0:0    2024-10-10 09:58:43 +0800 CST
783192            CPU       ROOT    2.00     Completed 0:0    2024-10-10 09:34:04 +0800 CST
783180            CPU       ROOT    1.00     Completed 0:0    2024-10-10 09:34:58 +0800 CST
783177            CPU       ROOT    1.00     Completed 0:0    2024-10-10 09:29:34 +0800 CST
783176            CPU       ROOT    2.00     Completed 0:0    2024-10-10 09:59:51 +0800 CST
```

## 相关命令

- [cqueue](cqueue.md) - 查看作业队列（当前/挂起的作业和作业步）
- [cbatch](cbatch.md) - 提交批处理作业
- [crun](crun.md) - 运行交互式作业和作业步
- [ccancel](ccancel.md) - 取消作业和作业步
- [ceff](ceff.md) - 查看作业效率统计
- [ccon](ccon.md) - 容器作业管理
