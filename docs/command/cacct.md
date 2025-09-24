# cacct 查看作业信息

**cacct可以查看队列中的作业信息。**

查看集群中所有队列的作业信息（包括所有状态），默认输出100条信息。

```Bash
cacct
```

**cacct运行结果展示**

```
JOBID JOBNAME      PARTITION ACCOUNT ALLOCCPUS STATE            EXITCODE STARTTIME
30680 Test_Job     CPU       yanyan  2.00      ExceedTimeLimit  0:15     2024-07-24 09:48:27 +0800 CST
30679 Test_Job     CPU       yanyan  2.00      ExceedTimeLimit  0:15     2024-07-24 09:47:51 +0800 CST
30678 Interactive  CPU       yanyan  2.00      Completed        0:0      2024-07-24 09:45:23 +0800 CST
30677 Interactive  CPU       yanyan  2.00      Completed        0:64     2024-07-24 09:44:03 +0800 CST
30674 Test_Job     CPU       yanyan  2.00      Cancelled        0:15     2024-07-24 09:38:30 +0800 CST
30673 Test_Job     CPU       yanyan  2.00      Cancelled        0:15     2024-07-24 09:38:29 +0800 CST
30672 Test_Job     CPU       yanyan  2.00      Failed           0:66     2024-07-24 09:38:29 +0800 CST
30671 Test_Job     CPU       yanyan  2.00      Cancelled        0:15     2024-07-24 09:24:56 +0800 CST
30670 Test_Job     CPU       yanyan  2.00      Cancelled        0:15     2024-07-24 09:24:54 +0800 CST
30669 Interactive  CPU       ROOT    1.00      Completed        0:0      2024-07-23 14:43:20 +0800 CST
30668 Interactive  CPU       ROOT    2.00      Completed        0:64     2024-07-23 14:43:12 +0800 CST
30667 Interactive  CPU       ROOT    1.00      Completed        0:0      2024-07-23 14:42:26 +0800 CST
30666 Interactive  CPU       ROOT    1.00      Completed        0:0      2024-07-23 14:42:22 +0800 CST
30665 Interactive  CPU       ROOT    1.00      Completed        0:0      2024-07-23 14:42:14 +0800 CST
30664 Interactive  CPU       ROOT    2.00      Completed        0:64     2024-07-23 14:42:06 +0800 CST
30663 Interactive  CPU       ROOT    1.00      Completed        0:0      2024-07-23 14:41:48 +0800 CST
30662 Interactive  CPU       ROOT    1.00      Completed        0:0      2024-07-23 14:41:27 +0800 CST
30661 Interactive  CPU       ROOT    1.00      Completed        0:0      2024-07-23 14:41:19 +0800 CST
```

**主要输出项**

- **TaskId**：作业号
- **TaskName**: 作业名
- **Partition**：作业所在分区
- **Account**：作业所属账户
- **AllocCPUs**：作业分配的CPU数量
- **State**：作业状态
- **ExitCode**：作业状态码

#### **主要参数**

- **-A/--account string**：指定查询作业的所属账户，指定多个账户时用逗号隔开
- **-C/--config string：**配置文件路径(默认为 "/etc/crane/config.yaml")
- **-E/--end-time string**：指定查询该时间之前结束的作业，例：cacct -E=~2023-03-14T10:00:00
- **-o/--format string：**指定输出格式。由百分号（%）后接一个字符或字符串标识。 在 % 和格式字符/字符串之间用点（.）和数字，可指定字段的最小宽度。支持的格式标识符或字符串（不区分大小写）：
  - **%a/%Account：** 显示作业关联的账户
  - **%c/%AllocCpus：**显示作业已分配的 CPU 数量
  - **%e/%CpuPerNode：**显示作业每个节点请求的 CPU 数量
  - **%h/%ElapsedTime：**显示作业自启动以来的已用时间
  - **%j/%JobId：**显示作业 ID
  - **%k/%Comment：**显示作业的备注
  - **%l/%NodeList：**显示作业正在运行的节点列表
  - **%m/%TimeLimit：**显示作业的时间限制
  - **%n/%MemPerNode：**显示作业每个节点请求的内存量
  - **%N/%NodeNum：**显示作业请求的节点数量
  - **%n/%Name：**显示作业名称
  - **%P/%Partition：**显示作业运行所在的分区
  - **%p/%Priority：**显示作业的优先级
  - **%Q/%QOS**：显示作业的服务质量（QoS）级别
  - **%R/%Reason：**显示作业挂起的原因
  - **%r/%ReqNodes：**显示作业请求的节点
  - **%S/%StartTime：**显示作业的开始时间
  - **%s/%SubmitTime：**显示作业的提交时间
  - **%t/%State：**显示作业的当前状态
  - **%T/%JobType：**显示作业类型
  - **%u/%Uid**：**显示作业的 UID
  - **%U/%User：**显示提交作业的用户
  - **%x/%ExcludeNodes：**显示作业排除的节点
  - 每个格式标识符或字符串可用宽度说明符修改（如 "%.5j" ）。 若指定宽度，则会被格式化为至少达到该宽度。 若格式无效或无法识别，程序会报错并终止。 
    - **例：--format "%.5j %.20n %t"** 会输出作业 ID（最小宽度 5）、名称（最小宽度 20）和状态。
- **-F/-full**：显示完整信息
- **-h/--help**: 显示帮助
- **-j/--job string**：指定查询作业号，指定多个作业号时用逗号隔开。如 -j=2,3,4
  - **--json**：json格式输出命令执行结果
- **-m/--max-lines** **uint32**：指定输出结果的最大条数。如-m=500表示最多输出500行查询结果
- **-n/ --name string**：指定查询作业名，指定多个作业名时用逗号隔开
- **-N/--no header**：输出隐藏表头
- **-p/--partition string**：指定要查看的分区，多个分区名用逗号隔开，默认为全部
- **-q/--qos string**：指定要查看的Qos，多个Qos用逗号隔开，默认为全部
- **-S/--start-time string**：筛选开始时间在特定时间段内的作业，可使用闭区间（时间格式：2024-01-02T15:04:05~2024-01-11T11:12:41 ）或半开区间（时间格式：2024-01-02T15:04:05~ 或 ~2024-01-11T11:12:41 ）
- **-t/--state string**：指定要查看的作业状态，支持的状态：pending(p)（挂起 ）、running(r)（运行中 ）、completed(c)（已完成 ）、failed(f)（失败 ）、cancelled(x)（已取消 ）、time-limit-exceeded(t)（超时 ）、all（所有 ）。（默认 “all” ）
- **-s/--submit-time string**：筛选提交时间在特定时间段内的作业，可使用闭区间（时间格式：2024-01-02T15:04:05~2024-01-11T11:12:41 ）或半开区间（时间格式：2024-01-02T15:04:05~ 或 ~2024-01-11T11:12:41 ）
- **-u/--user string**：指定查询某个用户的作业，指定多个用户时用逗号隔开
- **-v/--version：**查询版本号
- 例：

```SQL
cacct
```

```
JOBID JOBNAME      PARTITION ACCOUNT ALLOCCPUS STATE            EXITCODE STARTTIME
30680 Test_Job     CPU       yanyan  2.00      ExceedTimeLimit  0:15     2024-07-24 09:48:27 +0800 CST
30679 Test_Job     CPU       yanyan  2.00      ExceedTimeLimit  0:15     2024-07-24 09:47:51 +0800 CST
30678 Interactive  CPU       yanyan  2.00      Completed        0:0      2024-07-24 09:45:23 +0800 CST
30677 Interactive  CPU       yanyan  2.00      Completed        0:64     2024-07-24 09:44:03 +0800 CST
30674 Test_Job     CPU       yanyan  2.00      Cancelled        0:15     2024-07-24 09:38:30 +0800 CST
30673 Test_Job     CPU       yanyan  2.00      Cancelled        0:15     2024-07-24 09:38:29 +0800 CST
30672 Test_Job     CPU       yanyan  2.00      Failed           0:66     2024-07-24 09:38:29 +0800 CST
30671 Test_Job     CPU       yanyan  2.00      Cancelled        0:15     2024-07-24 09:24:56 +0800 CST
30670 Test_Job     CPU       yanyan  2.00      Cancelled        0:15     2024-07-24 09:24:54 +0800 CST
30669 Interactive  CPU       ROOT    1.00      Completed        0:0      2024-07-23 14:43:20 +0800 CST
30668 Interactive  CPU       ROOT    2.00      Completed        0:64     2024-07-23 14:43:12 +0800 CST
30667 Interactive  CPU       ROOT    1.00      Completed        0:0      2024-07-23 14:42:26 +0800 CST
30666 Interactive  CPU       ROOT    1.00      Completed        0:0      2024-07-23 14:42:22 +0800 CST
30665 Interactive  CPU       ROOT    1.00      Completed        0:0      2024-07-23 14:42:14 +0800 CST
30664 Interactive  CPU       ROOT    2.00      Completed        0:64     2024-07-23 14:42:06 +0800 CST
30663 Interactive  CPU       ROOT    1.00      Completed        0:0      2024-07-23 14:41:48 +0800 CST
30662 Interactive  CPU       ROOT    1.00      Completed        0:0      2024-07-23 14:41:27 +0800 CST
30661 Interactive  CPU       ROOT    1.00      Completed        0:0      2024-07-23 14:41:19 +0800 CST
```

```SQL
```SQL
cacct -h
```

```
Usage: cacct [OPTION]... [JOB_IDS]...

Display accounting data for the jobs

Options:
  -A, --account string        Account name(s) to search for
  -C, --config string         Config file path (default "/etc/crane/config.yaml")
  -E, --end-time string       Query jobs ended before specified time
  -o, --format string         Output format specification
  -F, --full                  Show full information
  -h, --help                  Show this help message
  -j, --job string            Job ID(s) to search for
      --json                  Output results in JSON format
  -m, --max-lines uint32      Maximum number of lines to display
  -n, --name string           Job name(s) to search for  
  -N, --no-header             Suppress header output
  -p, --partition string      Partition name(s) to search for
  -q, --qos string            QOS name(s) to search for
  -S, --start-time string     Query jobs started in specified time range
  -t, --state string          Job state(s) to search for
  -s, --submit-time string    Query jobs submitted in specified time range
  -u, --user string           User name(s) to search for
  -v, --version               Show version information
```

```SQL
```SQL
cacct -N
```

```
30689 Interactive CPU       CraneTest  2.00      Completed        0:0
30688 Interactive CPU       CraneTest  2.00      Completed        0:64
30687 Test_Job    CPU       CraneTest  0.00      Cancelled        0:0
30686 Test_Job    CPU       CraneTest  2.00      Cancelled        0:15
30685 Test_Job    CPU       CraneTest  2.00      ExceedTimeLimit  0:15
30684 Test_Job    CPU       CraneTest  2.00      Cancelled        0:15
30683 Test_Job    CPU       CraneTest  2.00      Cancelled        0:15
30682 Test_Job    CPU       ROOT       2.00      Failed           0:66
30681 Test_Job    CPU       ROOT       2.00      Cancelled        0:15
30680 Interactive CPU       ROOT       1.00      Completed        0:0
```

```SQL
```SQL
cacct -S=2024-07-22T10:00:00~2024-07-24T10:00:00
```

```
JOBID JOBNAME      PARTITION ACCOUNT ALLOCCPUS STATE            EXITCODE STARTTIME
30680 Test_Job     CPU       yanyan  2.00      ExceedTimeLimit  0:15     2024-07-24 09:48:27 +0800 CST
30679 Test_Job     CPU       yanyan  2.00      ExceedTimeLimit  0:15     2024-07-24 09:47:51 +0800 CST
30678 Interactive  CPU       yanyan  2.00      Completed        0:0      2024-07-24 09:45:23 +0800 CST
30677 Interactive  CPU       yanyan  2.00      Completed        0:64     2024-07-24 09:44:03 +0800 CST
30674 Test_Job     CPU       yanyan  2.00      Cancelled        0:15     2024-07-24 09:38:30 +0800 CST
30673 Test_Job     CPU       yanyan  2.00      Cancelled        0:15     2024-07-24 09:38:29 +0800 CST
30672 Test_Job     CPU       yanyan  2.00      Failed           0:66     2024-07-24 09:38:29 +0800 CST
30671 Test_Job     CPU       yanyan  2.00      Cancelled        0:15     2024-07-24 09:24:56 +0800 CST
30670 Test_Job     CPU       yanyan  2.00      Cancelled        0:15     2024-07-24 09:24:54 +0800 CST
30669 Interactive  CPU       ROOT    1.00      Completed        0:0      2024-07-23 14:43:20 +0800 CST
30668 Interactive  CPU       ROOT    2.00      Completed        0:64     2024-07-23 14:43:12 +0800 CST
30667 Interactive  CPU       ROOT    1.00      Completed        0:0      2024-07-23 14:42:26 +0800 CST
30666 Interactive  CPU       ROOT    1.00      Completed        0:0      2024-07-23 14:42:22 +0800 CST
30665 Interactive  CPU       ROOT    1.00      Completed        0:0      2024-07-23 14:42:14 +0800 CST
30664 Interactive  CPU       ROOT    2.00      Completed        0:64     2024-07-23 14:42:06 +0800 CST
30663 Interactive  CPU       ROOT    1.00      Completed        0:0      2024-07-23 14:41:48 +0800 CST
30662 Interactive  CPU       ROOT    1.00      Completed        0:0      2024-07-23 14:41:27 +0800 CST
30661 Interactive  CPU       ROOT    1.00      Completed        0:0      2024-07-23 14:41:19 +0800 CST
```

```SQL
```SQL
cacct -E=2024-07-22T10:00:00~2024-07-24T10:00:00
```

```
JOBID JOBNAME      PARTITION ACCOUNT ALLOCCPUS STATE            EXITCODE ENDTIME
30678 Interactive  CPU       yanyan  2.00      Completed        0:0      2024-07-24 09:45:42 +0800 CST
30677 Interactive  CPU       yanyan  2.00      Completed        0:64     2024-07-24 09:44:10 +0800 CST
30676 Interactive  CPU       yanyan  0.00      Cancelled        0:0      2024-07-24 09:27:25 +0800 CST
30675 Test_Job     CPU       yanyan  0.00      Cancelled        0:0      2024-07-24 09:38:38 +0800 CST
30674 Test_Job     CPU       yanyan  2.00      Cancelled        0:15     2024-07-24 09:48:11 +0800 CST
30673 Test_Job     CPU       yanyan  2.00      Cancelled        0:15     2024-07-24 09:39:57 +0800 CST
30672 Test_Job     CPU       yanyan  2.00      Failed           0:66     2024-07-24 09:38:29 +0800 CST
30671 Test_Job     CPU       yanyan  2.00      Cancelled        0:15     2024-07-24 09:38:28 +0800 CST
30670 Test_Job     CPU       yanyan  2.00      Cancelled        0:15     2024-07-24 09:38:28 +0800 CST
30669 Interactive  CPU       ROOT    1.00      Completed        0:0      2024-07-23 14:43:21 +0800 CST
30668 Interactive  CPU       ROOT    2.00      Completed        0:64     2024-07-23 16:54:40 +0800 CST
30667 Interactive  CPU       ROOT    1.00      Completed        0:0      2024-07-23 14:42:27 +0800 CST
30666 Interactive  CPU       ROOT    1.00      Completed        0:0      2024-07-23 14:42:23 +0800 CST
30665 Interactive  CPU       ROOT    1.00      Completed        0:0      2024-07-23 14:42:15 +0800 CST
```

```SQL
```SQL
cacct -j=30618,30619,30620
```

```
JOBID JOBNAME  PARTITION ACCOUNT ALLOCCPUS STATE            EXITCODE
30620 Test_Job CPU       ROOT    0.00      Cancelled        0:0
30619 Test_Job CPU       ROOT    2.00      ExceedTimeLimit  0:15
30618 Test_Job CPU       ROOT    2.00      Cancelled        0:15
```

```SQL
```SQL
cacct -u=cranetest
```

```
JOBID JOBNAME      PARTITION ACCOUNT    ALLOCCPUS STATE            EXITCODE
30689 Interactive  CPU       CraneTest  2.00      Completed        0:0
30688 Interactive  CPU       CraneTest  2.00      Completed        0:64
30687 Test_Job     CPU       CraneTest  0.00      Cancelled        0:0
30686 Test_Job     CPU       CraneTest  2.00      Cancelled        0:15
30685 Test_Job     CPU       CraneTest  2.00      ExceedTimeLimit  0:15
30684 Test_Job     CPU       CraneTest  2.00      Cancelled        0:15
30683 Test_Job     CPU       CraneTest  2.00      Cancelled        0:15
```

```SQL
```SQL
cacct -A=CraneTest
```

```
JOBID JOBNAME      PARTITION ACCOUNT    ALLOCCPUS STATE            EXITCODE
30689 Interactive  CPU       CraneTest  2.00      Completed        0:0
30688 Interactive  CPU       CraneTest  2.00      Completed        0:64
30687 Test_Job     CPU       CraneTest  0.00      Cancelled        0:0
30686 Test_Job     CPU       CraneTest  2.00      Cancelled        0:15
30685 Test_Job     CPU       CraneTest  2.00      ExceedTimeLimit  0:15
30684 Test_Job     CPU       CraneTest  2.00      Cancelled        0:15
30683 Test_Job     CPU       CraneTest  2.00      Cancelled        0:15
```

```SQL
```SQL
cacct -m=10
```

```
JOBID JOBNAME      PARTITION ACCOUNT ALLOCCPUS STATE            EXITCODE STARTTIME
30689 Interactive  CPU       ROOT    2.00      Completed        0:0      2024-07-24 10:48:27 +0800 CST
30688 Interactive  CPU       ROOT    2.00      Completed        0:64     2024-07-24 10:47:51 +0800 CST
30687 Test_Job     CPU       ROOT    0.00      Cancelled        0:0      2024-07-24 10:45:23 +0800 CST
30686 Test_Job     CPU       ROOT    2.00      Cancelled        0:15     2024-07-24 10:44:03 +0800 CST
30685 Test_Job     CPU       ROOT    2.00      ExceedTimeLimit  0:15     2024-07-24 10:38:30 +0800 CST
30684 Test_Job     CPU       ROOT    2.00      Cancelled        0:15     2024-07-24 10:38:29 +0800 CST
30683 Test_Job     CPU       ROOT    2.00      Cancelled        0:15     2024-07-24 10:38:29 +0800 CST
30682 Test_Job     CPU       ROOT    2.00      Failed           0:66     2024-07-24 10:24:56 +0800 CST
30681 Test_Job     CPU       ROOT    2.00      Cancelled        0:15     2024-07-24 10:24:54 +0800 CST
30680 Interactive  CPU       ROOT    1.00      Completed        0:0      2024-07-24 10:43:20 +0800 CST
```

```C
```C
cacct -p GPU
```

```
JOBID JOBNAME      PARTITION ACCOUNT ALLOCCPUS STATE      EXITCODE STARTTIME
30690 GPU_Job      GPU       test    4.00      Running    0:0      2024-07-24 11:20:15 +0800 CST
30689 ML_Training  GPU       test    8.00      Completed  0:0      2024-07-24 10:45:30 +0800 CST
30688 Deep_Learn   GPU       test    2.00      Completed  0:0      2024-07-24 09:30:22 +0800 CST
```

```C
```C
cacct -n=Test_Job
```

```
JOBID JOBNAME  PARTITION ACCOUNT ALLOCCPUS STATE            EXITCODE STARTTIME
30685 Test_Job CPU       ROOT    2.00      ExceedTimeLimit  0:15     2024-07-24 10:38:30 +0800 CST
30684 Test_Job CPU       ROOT    2.00      Cancelled        0:15     2024-07-24 10:38:29 +0800 CST
30683 Test_Job CPU       ROOT    2.00      Cancelled        0:15     2024-07-24 10:38:29 +0800 CST
30682 Test_Job CPU       ROOT    2.00      Failed           0:66     2024-07-24 10:24:56 +0800 CST
30681 Test_Job CPU       ROOT    2.00      Cancelled        0:15     2024-07-24 10:24:54 +0800 CST
```

```SQL
```SQL
cacct -o="%j %.10n %P %a %t"
```

```
JOBID NAME       PARTITION ACCOUNT    STATE
30689 Interactive CPU      CraneTest   Completed
30688 Interactive CPU      CraneTest   Completed
30687 Test_Job    CPU      CraneTest   Cancelled
30686 Test_Job    CPU      CraneTest   Cancelled
30685 Test_Job    CPU      CraneTest   ExceedTimeLimit
```
```Bash
```Bash
cacct -A ROOT -m 10
```

```
JOBID JOBNAME      PARTITION ACCOUNT ALLOCCPUS STATE      EXITCODE STARTTIME
30695 Interactive  CPU       ROOT    1.00      Completed  0:0      2024-07-24 12:43:20 +0800 CST
30694 Interactive  CPU       ROOT    2.00      Completed  0:64     2024-07-24 12:43:12 +0800 CST
30693 Interactive  CPU       ROOT    1.00      Completed  0:0      2024-07-24 12:42:26 +0800 CST
30692 Interactive  CPU       ROOT    1.00      Completed  0:0      2024-07-24 12:42:22 +0800 CST
30691 Interactive  CPU       ROOT    1.00      Completed  0:0      2024-07-24 12:42:14 +0800 CST
30690 Interactive  CPU       ROOT    2.00      Completed  0:64     2024-07-24 12:42:06 +0800 CST
30689 Interactive  CPU       ROOT    1.00      Completed  0:0      2024-07-24 12:41:48 +0800 CST
30688 Interactive  CPU       ROOT    1.00      Completed  0:0      2024-07-24 12:41:27 +0800 CST
30687 Interactive  CPU       ROOT    1.00      Completed  0:0      2024-07-24 12:41:19 +0800 CST
30686 Test_Job     CPU       ROOT    2.00      Cancelled  0:15     2024-07-24 12:38:30 +0800 CST
```

```Bash
```Bash
cacct -m 10 -j 783925,783889 -t=c -F
```

```
JOBID JOBNAME      PARTITION ACCOUNT ALLOCCPUS STATE     EXITCODE SUBMITTIME              STARTTIME               ENDTIME                 NODELIST  PRIORITY QOS
783925 Test_Job     CPU       test    2.00      Completed 0:0      2024-07-20 09:30:15    2024-07-20 09:30:20    2024-07-20 09:35:45    crane01   1000     normal
783889 Batch_Job    CPU       test    4.00      Completed 0:0      2024-07-20 08:15:30    2024-07-20 08:15:35    2024-07-20 08:45:22    crane02   1000     normal
```

```Bash
```Bash
cacct -n test
```

```
JOBID JOBNAME PARTITION ACCOUNT ALLOCCPUS STATE     EXITCODE STARTTIME
30700 test    CPU       user1   1.00      Completed 0:0      2024-07-24 13:15:30 +0800 CST
30699 test    CPU       user1   2.00      Running   0:0      2024-07-24 13:10:15 +0800 CST
30698 test    CPU       user2   1.00      Completed 0:0      2024-07-24 12:55:45 +0800 CST
```


```Bash
```Bash
cacct -q test_qos
```

```
JOBID JOBNAME      PARTITION ACCOUNT ALLOCCPUS STATE     EXITCODE STARTTIME                QOS
30702 ML_Job       GPU       test    4.00      Running   0:0      2024-07-24 13:30:15     test_qos
30701 Data_Process CPU       test    2.00      Completed 0:0      2024-07-24 13:25:30     test_qos
30700 Analysis     CPU       test    1.00      Completed 0:0      2024-07-24 13:20:45     test_qos
```
```Bash
```Bash
cacct -m 10 -E=2024-10-08T10:00:00~2024-10-10T110:00:00 -p CPU -t c
```

```
JOBID JOBNAME      PARTITION ACCOUNT ALLOCCPUS STATE     EXITCODE ENDTIME
30705 Process_Job  CPU       test    2.00      Completed 0:0      2024-10-09 14:30:25 +0800 CST
30704 Analysis     CPU       test    1.00      Completed 0:0      2024-10-09 13:45:10 +0800 CST
30703 Batch_Work   CPU       test    4.00      Completed 0:0      2024-10-09 12:15:30 +0800 CST
30702 Data_Clean   CPU       test    1.00      Completed 0:0      2024-10-09 11:30:45 +0800 CST
30701 Test_Run     CPU       test    2.00      Completed 0:0      2024-10-09 10:45:15 +0800 CST
30700 Simple_Job   CPU       test    1.00      Completed 0:0      2024-10-08 15:20:30 +0800 CST
30699 Quick_Task   CPU       test    1.00      Completed 0:0      2024-10-08 14:15:45 +0800 CST
30698 Basic_Work   CPU       test    2.00      Completed 0:0      2024-10-08 13:30:20 +0800 CST
30697 Standard_Job CPU       test    1.00      Completed 0:0      2024-10-08 12:45:10 +0800 CST
30696 Regular_Task CPU       test    2.00      Completed 0:0      2024-10-08 11:15:30 +0800 CST
```