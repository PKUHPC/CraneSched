# crun 提交交互式任务

crun使用命令行指定的参数申请资源并在计算节点启动指定的任务，用户的输入将被转发到计算节点上对应的任务，任务的输出将被转发回用户终端。crun需要在有cfored运行的节点上启动。

crun可以运行在两种模式下：

- **作业模式**：在现有作业之外执行时创建新的作业分配
- **作业步模式**：在作业分配内执行时（例如在calloc中）创建作业步

系统通过检查`CRANE_JOB_ID`环境变量自动检测运行模式。

## 选项

**-h, --help**

:   **适用于：** `作业`, `作业步`
显示crun命令的帮助信息。

**-v, --version**

:   **适用于：** `作业`, `作业步`
显示crun版本信息。

**-C, --config=\<path\>**

:   **适用于：** `作业`, `作业步`
配置文件路径。默认值："/etc/crane/config.yaml"。

**--debug-level=\<level\>**

:   **适用于：** `作业`, `作业步`
设置调试输出级别。可用级别：trace、debug、info。默认值："info"。

**-N, --nodes=\<minnodes[-maxnodes]\>**

:   **适用于：** `作业`, `作业步`
运行所需的节点数量。可以指定最小和最大节点数量，用破折号分隔（如2-4）。如果只指定一个数字，则该数字同时用作最小和最大节点数量。默认值：1。

**-c, --cpus-per-task=\<ncpus\>**

:   **适用于：** `作业`, `作业步`
每个任务所需的CPU数量。如果任务是多线程的并且需要多个CPU以获得最佳性能，此选项可能很有用。默认值：1。

**--ntasks-per-node=\<ntasks\>**

:   **适用于：** `作业`, `作业步`
请求在每个节点上调用ntasks个任务。如果与--nodes选项一起使用，--ntasks选项将被视为作业的最大任务数。这意味着将至少调用ntasks-per-node *
nodes个任务，或ntasks个任务，以较小者为准。默认值：1。

**--mem=\<size\>**

:   **适用于：** `作业`, `作业步`
每个节点所需的最大实际内存量。支持不同单位：GB(G, g)、MB(M, m)、KB(K, k)和Bytes(B)。默认单位是MB。

**-t, --time=\<time\>**

:   **适用于：** `作业`, `作业步`
设置作业分配的总运行时间限制。时间格式为"[day-]hours:minutes:seconds"。例如，"5-0:0:1"表示5天1秒，或"10:1:2"表示10小时1分钟2秒。

**--gres=\<list\>**

:   **适用于：** `作业`, `作业步`
指定每个任务所需的通用资源。格式："gpu:type:number"（如"gpu:a100:1"）或"gpu:number"（如"gpu:1"）。

**-w, --nodelist=\<host1,host2,...\>**

:   **适用于：** `作业`, `作业步`
请求特定的节点列表。列表可以指定为以逗号分隔的节点名称列表。

**-x, --exclude=\<host1,host2,...\>**

:   **适用于：** `作业`, `作业步`
明确从授予作业的资源中排除某些节点。列表可以指定为以逗号分隔的节点名称列表。

**-p, --partition=\<partition_name\>**

:   **适用于：** `作业`
为资源分配请求特定分区。如果未指定，将使用默认分区。

**-A, --account=\<account\>**

:   **适用于：** `作业`
将此作业使用的资源计入指定账户。账户是一个任意字符串。

**-q, --qos=\<qos\>**

:   **适用于：** `作业`
为作业请求特定的服务质量（QoS）。QoS值由系统管理员定义。

**--exclusive**

:   **适用于：** `作业`
作业分配不能与其他正在运行的作业共享节点。

**-H, --hold**

:   **适用于：** `作业`
以暂挂状态提交作业。暂挂的作业在用户或管理员使用ccontrol release命令明确释放之前不会被调度执行。

**-r, --reservation=\<reservation_name\>**

:   **适用于：** `作业`
从指定的预留中为作业分配资源。

**-J, --job-name=\<jobname\>**

:   **适用于：** `作业`, `作业步`
为作业分配指定名称。指定的名称将与作业ID一起出现在cqueue的输出中。

**-L, --licenses=\<license\>**

:   **适用于：** `作业`, `作业步`
指定作业所需的许可证。格式："license1:count1,license2:count2"或"license1:count1|license2:count2"。例如："matlab:2,ansys:1"。

**--wckey=\<wckey\>**

:   **适用于：** `作业`, `作业步`
为作业指定工作负载特征键。这是一个任意字符串，可用于跟踪作业组。

**--comment=\<string\>**

:   **适用于：** `作业`, `作业步`
作业的任意备注字符串。

**-D, --chdir=\<directory\>**

:   **适用于：** `作业`, `作业步`
在执行开始之前将任务的工作目录设置为directory。路径可以是绝对路径或相对路径。

**--export=\<environment\>**

:   **适用于：** `作业`, `作业步`
指定要导出到任务环境的环境变量。选项包括"ALL"（导出所有环境变量）、"NONE"（不导出环境变量）或以逗号分隔的变量名称列表。

**--get-user-env**

:   **适用于：** `作业`, `作业步`
将用户的登录环境变量加载到任务环境中。

**--extra-attr=\<json\>**

:   **适用于：** `作业`, `作业步`
以JSON格式指定作业的额外属性。

**-i, --input=\<mode\>**

:   **适用于：** `作业`, `作业步`
指定如何重定向标准输入。选项为"all"（将标准输入重定向到所有任务）或"none"（不重定向标准输入）。默认值："all"。

**--pty**

:   **适用于：** `作业`, `作业步`
在伪终端模式下执行任务。这允许正确处理交互式程序。

**--x11**

:   **适用于：** `作业`, `作业步`
为作业启用X11支持。如果未与--x11-forwarding一起使用，则使用直接X11转发（不安全）。默认值为false。

**--x11-forwarding**

:   **适用于：** `作业`, `作业步`
通过CraneSched启用安全X11转发。默认值为false。

**--mail-type=\<type\>**

:   **适用于：** `作业`, `作业步`
当发生某些事件类型时通过电子邮件通知用户。有效的类型值为NONE、BEGIN、END、FAIL、TIMELIMIT和ALL（等同于BEGIN、END、FAIL和TIMELIMIT）。可以在以逗号分隔的列表中指定多个类型值。默认值：NONE。

**--mail-user=\<email\>**

:   **适用于：** `作业`, `作业步`
如果指定了--mail-type，则接收作业状态通知的电子邮件地址。

## 作业模式 vs 作业步模式

### 作业模式

当`crun`在现有作业分配**之外**执行时（环境中没有`CRANE_JOB_ID`），它会创建新作业：

- 所有选项都可用
- 创建新的资源分配
- 需要分区、账户和QoS规范（或默认值）

### 作业步模式

当`crun`在现有作业分配**内**执行时（例如在calloc内），它会自动作为作业步运行：

**自动检测：**

- 如果设置了`CRANE_JOB_ID` → 作为该作业内的作业步运行

- 如果未设置`CRANE_JOB_ID` → 作为新的独立作业运行

**资源行为：**

- 作业步使用父作业分配的资源
- 可以指定节点子集、不同的CPU数量等
- 资源必须在父作业的分配范围内可用

**继承的属性：**

作业步自动从父作业继承：

- 分区（`-p/--partition`）
- 账户（`-A/--account`）
- QoS（`-q/--qos`）
- 用户/组

标记为"适用于：`作业`"的选项在作业步模式下不能被覆盖，如果指定将被忽略。

## 使用示例

### 作业模式示例

**分配资源并运行bash：**

在CPU分区，申请两个节点，一个CPU核心，200M内存，并运行bash程序：

```bash
crun -c 1 --mem 200M -p CPU -N 2 /usr/bin/bash
```

![crun](../../images/crun/crun_c.png)

**排除特定节点：**

申请一个节点，且节点不能是crane01、crane02，任务名称为testjob，运行时间限制为0:25:25，并运行bash程序：

```bash
crun -N 1 -x crane01,crane02 -J testjob -t 0:25:25 /usr/bin/bash
```

![crun](../../images/crun/crun_N1.png)

**指定节点列表：**

在GPU分区申请一个节点和200M运行内存，节点只能在crane02、crane03中选择，并运行bash程序：

```bash
crun -p GPU --mem 200M -w crane02,crane03 /usr/bin/bash
```

![crun](../../images/crun/crun_N2.png)

**带账户、QoS和环境设置：**

```bash
crun -A ROOT -J test_crun -x cranetest03 --get-user-env --ntasks-per-node 2 -q test_qos -t 00:20:00 /usr/bin/bash
```

![crun](../../images/crun/crun_A.png)

**带工作目录和调试级别：**

```bash
crun -D /path --debug-level trace --export ALL /usr/bin/bash
```

![crun](../../images/crun/crun_D.png)

**在特定节点上运行：**

```bash
crun -w cranetest04 /usr/bin/bash
```

![crun](../../images/crun/crun_w.png)

**X11转发：**

```bash
# 运行X11应用程序
crun --x11 xclock
```

![crun](../../images/crun/crun_clock.png)

**独占模式：**

请求对分配节点的独占访问，防止其他作业共享：

```bash
crun --exclusive -N 2 /usr/bin/bash
```

**暂挂模式：**

以暂挂状态提交作业，防止其在手动释放前启动：

```bash
crun --hold -c 4 /usr/bin/bash
# 稍后使用以下命令释放: ccontrol release <job_id>
```

**预留资源：**

使用预留资源运行作业：

```bash
crun -r my_reservation /usr/bin/bash
```

**邮件通知：**

接收作业事件的邮件通知：

```bash
crun --mail-type=END --mail-user=user@example.com -c 4 /usr/bin/bash
```

**作业备注：**

为作业添加描述性备注：

```bash
crun --comment "测试新算法" -c 8 /usr/bin/python script.py
```

### 作业步模式示例

**在calloc任务内嵌套启动：**

crun可以在calloc任务内嵌套启动，将自动继承calloc任务的所有资源。不需要指定分区、账户或QoS：

![crun](../../images/crun/crun_N3.png)

**基本作业步执行：**

```bash
# 首先分配资源
calloc -N 2 -c 8 -p CPU -A myaccount

# 在分配内运行作业步（无需指定分区/账户）
crun -N 1 -c 4 ./task1
crun -N 1 -c 4 ./task2
crun -N 2 -c 2 ./task3
```

**多个并发作业步：**

```bash
# 在calloc分配中
crun -N 1 ./long_running_task &
crun -N 1 ./another_task &
wait
```

**具有特定资源的作业步：**

```bash
# 在具有4个节点的calloc分配内
crun -N 2 -c 8 --mem 4G ./memory_intensive_task
crun -w crane01,crane02 ./specific_node_task
```

**监控作业步：**

```bash
# 在另一个终端中
cqueue --step -j $CRANE_JOB_ID
ccontrol show step $CRANE_JOB_ID.2
```

## 向程序传递参数

向crun启动的程序传递参数：

```bash
# 使用双破折号
crun -c 1 -- your_program --your_args

# 使用引号
crun -c 1 "your_program --your_args"
```

## 相关命令

- [calloc](calloc.md) - 分配资源供交互式使用
- [cbatch](cbatch.md) - 提交批处理作业
- [ccancel](ccancel.md) - 取消作业和作业步
- [cqueue](cqueue.md) - 查看作业队列和作业步
- [ccontrol](ccontrol.md) - 控制和查询作业/作业步
