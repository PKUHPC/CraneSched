
# ccontrol 查看分区和节点状态 #

**ccontrol可以查看分区和节点的状态。**

**主要命令**

- **help**：显示帮助
- **show**：显示实体的状态，默认为所有记录
- **update**：修改作业/分区/节点信息
- **hold**：暂停作业调度
- **release**：继续作业调度
- **completion**：为指定的shell生成自动补全脚本
- **create：**创建一个新实体
- **delete：**删除指定的实体

**支持的命令行选项：**

- **-h/--help**: 显示帮助
- **--json**：json格式输出命令执行结果
- **-v/--version：**查询版本号
- **-C/--config string**：配置文件路径(默认 "/etc/crane/config.yaml")

# **1. 查看**

支持的命令行选项：

- **config:** 查询配置信息
- **job：**查询作业信息
- **node：**查询节点信息
- **partition**：查看集群分区情况

```Bash
ccontrol show -h
```

```
Usage: ccontrol show [OPTIONS] [ENTITY]

Show information about various entities

Available entities:
  config      Show configuration information
  job         Show job information  
  node        Show node information
  partition   Show partition information

Options:
  -h, --help               Show this help message
  -C, --config string      Config file path (default "/etc/crane/config.yaml")
      --json               Output results in JSON format
```

## **1. 1 查看分区状态**

```Plaintext
ccontrol show partition
```

**ccontrol show partition运行结果展示**

```
PARTITION   STATE   TOTAL_NODES   ALIVE_NODES   TOTAL_CPUS   AVAIL_CPUS   ALLOC_CPUS   FREE_CPUS   TOTAL_MEM   AVAIL_MEM   ALLOC_MEM   FREE_MEM    HOSTLIST
CPU         UP      3             3             24           20           4            16          96GB        80GB        16GB        64GB        crane01,crane02,crane03
GPU         UP      2             2             16           12           4            8           64GB        48GB        16GB        32GB        gpu01,gpu02
```
#### **主要输出项**

- **PartitionName**：分区名
- **State**：分区状态
- **TotalNodes**：分区节点数目
- **AliveNodes**：分区中可运行的节点数目
- **TotalCpus**：分区中所有节点总CPU数目
- **AvailCpus**：分区中所有可以使用的CPU数目
- **AllocCpus**：分区中已经被分配的CPU数目
- **FreeCpus**：分区中空闲的CPU数目
- **TotalMem**：分区节点的总内存
- **AvailMem**：分区中当前可以使用的内存大小
- **AllocMem**：分区中已分配的内存大小
- **FreeMem**：分区中空闲的内存大小
- **HostList**：分区中所有节点的节点名列表

## **1.2 查看节点状态**

```Plaintext
ccontrol show node
```

**ccontrol show node运行结果展示**

```
NODENAME   STATE   CPUS   ALLOC_CPUS   FREE_CPUS   REAL_MEMORY   ALLOC_MEM   FREE_MEM   PARTITION   RUNNING_TASK
crane01    IDLE    8      0            8           32GB          0GB         32GB       CPU         0
crane02    IDLE    8      4            4           32GB          16GB        16GB       CPU         2
crane03    DOWN    8      0            8           32GB          0GB         32GB       CPU         0
gpu01      IDLE    8      0            8           32GB          0GB         32GB       GPU         0  
gpu02      IDLE    8      4            4           32GB          16GB        16GB       GPU         1
```

#### **主要输出项**

- **NodeName**：节点名
- **State**：节点状态
  - **IDLE**： 节点空闲，可使用
  - **DOWN**： 节点宕机，不可用
- **CPUs**：节点CPU数目
- **AllocCpus**：节点已分配的CPU数目
- **FreeCpus**：节点空闲的CPU数目
- **RealMemory**：节点的实际内存大小
- **AllocMem**：节点已经分配的内存大小
- **FreeMem**：节点空闲的内存大小
- **Patition**：节点所属分区
- **RunningTask**：节点上正在运行的作业数量

## **1.3 查看作业状态**

```Plain
ccontrol show job
```

ccontrol show job 运行结果展示

```
JOBID   JOBNAME      USERID   GROUPID   ACCOUNT    JOB_STATE   RUN_TIME    TIME_LIMIT   SUBMIT_TIME             START_TIME              END_TIME                PARTITION   NODELIST   NUM_NODES
30705   ML_Training  1001     1001      test       RUNNING     00:25:30    02:00:00     2024-07-24 14:30:15     2024-07-24 14:35:20     -                       GPU         gpu01      1
30704   Data_Process 1002     1002      analysis   COMPLETED   01:15:45    01:30:00     2024-07-24 13:20:10     2024-07-24 13:25:15     2024-07-24 14:41:00     CPU         crane02    1
30703   Batch_Job    1001     1001      test       PENDING     -           02:00:00     2024-07-24 14:45:30     -                       -                       CPU         -          1
30702   Quick_Task   1003     1003      user       COMPLETED   00:05:20    00:10:00     2024-07-24 14:10:05     2024-07-24 14:10:10     2024-07-24 14:15:30     CPU         crane01    1
```

主要输出项

- **JobId：**作业号
- **JobName：**作业名
- **UserId：**作业所属用户
- **GroupId：**分组id
- **Account：**作业所属账户
- **JobState：**作业状态
- **RunTime：**作业运行时间
- **TimeLimit：**作业运行时间限制
- **SubmitTime：**作业提交时间
- **StartTime：**作业开始时间
- **EndTime：**作业结束时间
- **Partition：**作业所属分区
- **Nodelist：**作业运行的节点
- **NumNodes：**节点数量

# 2. 修改

支持的命令行选项：

- **job：**查询作业信息
- **node：**查询节点信息

```Bash
ccontrol update -h
```

```
Usage: ccontrol update [OPTIONS] ENTITY

Update information for various entities

Available entities:
  job         Update job information
  node        Update node information  
  partition   Update partition information

Options:
  -h, --help               Show this help message
  -C, --config string      Config file path (default "/etc/crane/config.yaml")
      --json               Output results in JSON format
```

## 2.1 **修改作业信息**

支持的命令行选项：

- **-h/--help**: 显示帮助
- **-J/--job-name string：**作业名
- **-P/--priority float**： 作业优先级
- **-T/--time-limit string：**作业超时时长

```Bash
ccontrol update job -h
```

```
Usage: ccontrol update job [OPTIONS] JOB_ID

Update job information

Options:
  -h, --help                Show this help message
  -J, --job-name string     Job name
  -P, --priority float      Job priority
  -T, --time-limit string   Job time limit
  -C, --config string       Config file path (default "/etc/crane/config.yaml")
      --json                Output results in JSON format
```

```Go
ccontrol update job -J 30685 -T 0:25:25
```

**命令执行前的作业状态:**
```
JOBID   JOBNAME   TIME_LIMIT   STATE     PRIORITY
30685   Test_Job  01:00:00     PENDING   1000
```

**命令执行:**
```
Job 30685 time limit updated successfully from 01:00:00 to 00:25:25
```

**命令执行后的作业状态:**
```
JOBID   JOBNAME   TIME_LIMIT   STATE     PRIORITY  
30685   Test_Job  00:25:25     PENDING   1000
```

```Bash
ccontrol update job -J 191 -P 2.0
```

```
Job 191 priority updated successfully from 1000 to 2.0
```

## 2**.2 修改节点信息**

支持的命令行选项：

- **-h/--help**: 显示帮助
- **-n/--name string：**节点名
- **-r/--reason string**： 设置修改原因
- **-t/--state string：**修改节点状态

```Bash
ccontrol update node -h
```

```
Usage: ccontrol update node [OPTIONS]

Update node information

Options:
  -h, --help               Show this help message
  -n, --name string        Node name
  -r, --reason string      Reason for state change
  -t, --state string       Node state (drain, idle, down)
  -C, --config string      Config file path (default "/etc/crane/config.yaml")
      --json               Output results in JSON format
```
```Go
ccontrol update node -n crane01 -t drain -r improving performance
```

**命令执行前的节点状态:**
```
NODENAME   STATE   REASON
crane01    IDLE    -
```

**命令执行:**
```
Node crane01 state updated successfully to drain with reason: improving performance
```

**命令执行后的节点状态:**
```
NODENAME   STATE   REASON
crane01    DRAIN   improving performance
```

主要参数：

- **-c/--cpu**：节点的核心数**（-h列表无该参数）**
- **-M/--memory**：节点的内存大小，默认是MB**（-h列表无该参数）**
- **-n/--name**：节点名称
- **-P/--partition**：节点所属的分区**（-h列表无该参数）**

以下参数和上面参数不能一起设置，下面参数用于修改节点状态

- **-r/--reason**：设置状态改变原因
- **-t/--state**：设置节点状态

# 3. 暂停/恢复

## 3.1 **暂停作业调度**

```
Usage: ccontrol hold [OPTIONS] JOB_ID

Hold (suspend) job scheduling

Options:
  -h, --help               Show this help message
  -T, --time-limit string  Time limit for automatic release
  -C, --config string      Config file path (default "/etc/crane/config.yaml")
      --json               Output results in JSON format

Examples:
  ccontrol hold 30751                    # Hold job 30751 indefinitely
  ccontrol hold 30751 -T 0:25:25        # Hold job 30751 for 25 minutes 25 seconds
```

主要参数：

- **--time-limit/-T：**修改时间限制

```Plain
ccontrol hold 30751           #暂停调度编号为30751的任务
ccontrol hold 30751 -t 0:25:25  #暂停调度编号为30751的任务25分钟25秒钟（随后解除暂停）
```

- hold 接受 job_id 的方式与 ccancel 相同，要求为逗号分隔的任务编号。
- 只能 hold pending 任务
- 如果此前有设置解除暂停的定时器，该操作会取消原有的定时器。
- 使用 cqueue 查询时，如果任务被 hold，Node(Reason) 一列会显示 "Held"。

**执行 hold 命令前的作业状态:**
```
JOBID   JOBNAME   STATE     NODE(REASON)
30751   Test_Job  PENDING   -
```

**执行 hold 命令:**
```
Job 30751 held successfully
```

**执行 hold 命令后的作业状态:**
```
JOBID   JOBNAME   STATE     NODE(REASON)
30751   Test_Job  PENDING   Held
```

## 3.2 **继续作业调度**

```
Usage: ccontrol release [OPTIONS] JOB_ID

Release (resume) job scheduling

Options:
  -h, --help               Show this help message
  -C, --config string      Config file path (default "/etc/crane/config.yaml")
      --json               Output results in JSON format

Examples:
  ccontrol release 30751   # Release job 30751 from hold
```

```Plain
ccontrol release 30751
```

- 如果此前有设置解除暂停的定时器，该操作会取消原有的定时器。
- 只能 release pending 任务1

**执行 release 命令前的作业状态:**
```
JOBID   JOBNAME   STATE     NODE(REASON)
30751   Test_Job  PENDING   Held
```

**执行 release 命令后的作业状态:**
```
JOBID   JOBNAME   STATE     NODE(REASON)
30751   Test_Job  PENDING   -
```

# 4**. completion**

**主要命令**：

- **bash**：为bash生成自动补全脚本
- **fish**：为fish生成自动补全脚本
- **powershell**：为powershell生成自动补全脚本
- **zsh**：为zsh生成自动补全脚本

**支持的命令行选项：**

- **-h/--help:** 显示帮助
- **-C/--config string：**配置文件路径(默认 "/etc/crane/config.yaml")
  - **--json：**json格式输出命令执行结果
