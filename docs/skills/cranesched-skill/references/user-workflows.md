# 普通用户工作流

## 目录

- [开始前确认](#开始前确认)
- [选择运行方式](#选择运行方式)
- [提交批处理作业](#提交批处理作业)
- [申请交互式资源](#申请交互式资源)
- [监控作业](#监控作业)
- [查看结果与历史](#查看结果与历史)
- [处理登录环境与批作业差异](#处理登录环境与批作业差异)
- [安全取消作业](#安全取消作业)
- [管理自己的活动作业](#管理自己的活动作业)
- [调整后重新提交](#调整后重新提交)
- [形成有效答复](#形成有效答复)

## 开始前确认

先收集会改变命令的最少信息：

- 要运行的程序或脚本，以及是否需要交互。
- 节点数、每节点任务数、每任务 CPU 数、每节点内存和最长运行时间。
- 是否需要 GPU/其他加速器、许可证、多节点通信或容器。
- 站点规定的分区、账户和 QoS；不知道时不要杜撰。
- 工作目录、输入数据位置、输出文件和错误文件。

资源含义不要混用：

- `--nodes`：节点数。
- `--ntasks-per-node`：每个节点启动的任务数。
- `--cpus-per-task`：每个任务需要的 CPU 核数，适合多线程程序。
- `--mem`：每个节点申请的内存；显式写单位，如 `4G` 或 `512M`。
- `--gres`：每个任务的通用资源，如 `gpu:1` 或站点支持的 `gpu:<type>:1`。
- `--time`：作业时限，格式为 `[day-]hours:minutes:seconds`。

从已验证的小规模作业开始，再扩大资源。不要为了缩短排队时间盲目申请更大时限、更多节点或独占节点。

## 选择运行方式

| 需求 | 首选 | 特点 |
|---|---|---|
| 无人值守、可复现、运行较久 | `cbatch` | 提交脚本后排队执行 |
| 获得计算节点 shell 做短时检查 | `calloc` | 退出 shell 后释放分配 |
| 交互运行一条程序或命令 | `crun` | 终端输入输出转发到任务 |
| 在已有 `calloc` 分配中运行子任务 | `crun` | 自动作为作业步复用资源 |

`calloc` 和 `crun` 需要从站点配置了前端转发服务的提交节点启动。若报命令环境或连接错误，先核对站点入口，不要改系统配置。

## 提交批处理作业

### 使用最小脚本

将所有尖括号占位符替换为站点或应用的真实值：

```bash
#!/bin/bash
#CBATCH --job-name=<job_name>
#CBATCH --partition=<partition>
#CBATCH --nodes=1
#CBATCH --ntasks-per-node=1
#CBATCH --cpus-per-task=1
#CBATCH --mem=1G
#CBATCH --time=00:30:00
#CBATCH --output=<job_name>_%j.out
#CBATCH --error=<job_name>_%j.err

echo "job_id=${CRANE_JOB_ID}"
echo "nodes=${CRANE_JOB_NODELIST}"
echo "started=$(date --iso-8601=seconds)"

<application_command>

status=$?
echo "finished=$(date --iso-8601=seconds) exit=${status}"
exit "${status}"
```

仅当站点要求时添加：

```bash
#CBATCH --account=<account>
#CBATCH --qos=<qos>
```

不要在不知道站点默认值时随意添加账户或 QoS。提交前检查：

```bash
bash -n job.sh
cbatch --help
```

`bash -n` 只检查 shell 语法，不验证 `#CBATCH` 参数、模块、路径或应用是否可用。

### 提交并保存作业 ID

```bash
cbatch job.sh
```

从返回信息中记录作业 ID，然后立即验证：

```bash
cqueue -j <job_id> -F
ccontrol show job <job_id>
```

提交成功只表示调度系统接受作业，不代表它已经运行或应用会成功。

### 提交单条短命令

只对简单命令使用 `--wrap`：

```bash
cbatch --partition=<partition> --time=00:05:00 --wrap "hostname"
```

包含复杂引号、管道、环境初始化或多步错误处理时，改用脚本以保证可复现。

## 申请交互式资源

### 获取 shell

```bash
calloc --partition=<partition> --nodes=1 --cpus-per-task=2 --mem=2G --time=00:30:00
```

进入后先核对分配：

```bash
echo "${CRANE_JOB_ID}"
echo "${CRANE_JOB_NODELIST}"
hostname
```

完成后执行 `exit`。退出 `calloc` shell 会终止该分配并释放资源，不要把仍需运行的前台任务遗留在会话中。

### 交互执行程序

```bash
crun --partition=<partition> --nodes=1 --cpus-per-task=2 --mem=2G --time=00:30:00 -- <program> <args>
```

在已有 `calloc` 分配内使用 `crun` 时，不要重复指定分区、账户或 QoS；它会自动创建作业步并复用分配资源。

## 监控作业

查看自己的活动作业：

```bash
cqueue --self -F
```

查看指定作业及详情：

```bash
cqueue -j <job_id> -F
ccontrol show job <job_id>
```

周期刷新时使用内置选项，避免自己编写无限循环：

```bash
cqueue -j <job_id> --iterate=5
```

状态判断：

- `Pending`：读取 `NODELIST/REASON` 或详情中的 Reason，不要仅凭等待时长判断异常。
- `Running`：检查节点、运行时间和实时输出；`ceff` 对运行中作业的效率统计可能误导。
- `Configuring`、`Starting`、`Completing`：短时间过渡通常正常；长时间停留再按排障流程收集证据。
- 作业从 `cqueue` 消失：转到 `cacct`，不要直接认定作业丢失。

查看作业步：

```bash
cqueue --step -j <job_id>
```

## 查看结果与历史

活动队列只覆盖当前作业。作业结束后查询历史：

```bash
cacct -j <job_id> -F
```

重点查看 `State` 和 `ExitCode`。`0:0` 通常表示应用成功退出；非零主代码表示程序返回失败，非零次代码表示收到信号或由 CraneSched 终止。

再查看输出、错误和资源效率：

```bash
sed -n '1,200p' <job_name>_<job_id>.out
sed -n '1,200p' <job_name>_<job_id>.err
ceff <job_id>
```

不要只看最后一行日志。保留首次错误、应用退出码、作业状态和时间线。

使用 `ceff` 调整后续资源：

- 多次已完成作业的内存使用都远低于申请量时，逐步降低 `--mem`，保留合理余量。
- CPU 效率低可能来自 I/O、通信、串行阶段或线程配置，不等同于“CPU 申请过多”。先结合应用行为判断。
- `OutOfMemory` 时增加内存前，先排除内存泄漏、输入规模变化和并发任务数错误。

## 处理登录环境与批作业差异

“登录节点能运行、批作业不能运行”通常先检查工作目录、环境初始化和路径可见性，不要让用户读取系统配置或服务日志。

优先在脚本内显式加载模块、激活虚拟环境并使用共享存储上的绝对路径。需要固定工作目录时添加：

```bash
#CBATCH --chdir=<shared_workdir>
```

只在站点文档明确推荐时选择下列一种环境传播方式：

```bash
#CBATCH --get-user-env
```

或：

```bash
#CBATCH --export=ALL
```

`--get-user-env` 加载登录环境，`--export=ALL` 传播当前环境。不要无条件同时使用，也不要粘贴完整 `env` 输出；环境中可能含令牌等敏感值。

在作业输出中仅记录必要的用户级证据：

```bash
pwd
command -v <program>
ls -ld <shared_workdir> <input_path>
```

若路径仅在登录节点存在、计算节点不可见，改用站点提供的共享路径。若程序、模块或共享路径策略不清楚，请用户咨询管理员，并提供作业 ID、工作目录和首次报错。

## 安全取消作业

取消单个作业前先确认所有者、状态和 ID：

```bash
cqueue -j <job_id> -F
ccancel <job_id>
cacct -j <job_id> -F
```

`ccancel` 没有确认提示且立即生效。普通用户只能取消自己的作业。

取消单个作业步而保留父作业的其他步骤：

```bash
cqueue --step -j <job_id>
ccancel <job_id>.<step_id>
cacct -j <job_id>.<step_id> -F
```

批量取消时先预览自己的 Pending 作业，再从输出中复制并复核需要取消的明确 ID：

```bash
cqueue --self --state=PENDING -F
ccancel <job_id_1>,<job_id_2>
cacct -j <job_id_1>,<job_id_2> -F
```

只有用户逐项确认这些 ID 后才执行。每次取消后确认所有目标在历史中进入 `Cancelled`；若历史尚未更新，短暂等待后只重试只读查询，不要重复取消。不要提供按用户、状态、分区、账户或名称过滤的批量取消命令，因为查询与执行之间可能出现新的未预览作业。

## 管理自己的活动作业

`ccontrol` 文档允许作业所有者 hold、release 或更新自己的作业。只在当前 `--help` 和站点政策允许、用户明确要求且已确认作业所有权时使用。

执行任何变更前后都查询同一作业：

```bash
cqueue -j <job_id> -F
ccontrol show job <job_id>
```

暂挂或释放自己的 Pending 作业：

```bash
ccontrol hold <job_id>
ccontrol release <job_id>
```

更新自己的活动作业时只修改用户明确指定的字段，例如：

```bash
ccontrol update jobid=<job_id> timelimit=<HH:MM:SS|D-HH:MM:SS>
ccontrol update jobid=<job_id> comment="<comment>"
ccontrol update jobid=<job_id> mailuser=<email> mailtype=<NONE|BEGIN|END|FAIL|TIMELIMIT|ALL>
```

不要替用户猜测新值，不要修改其他用户作业，也不要默认更新优先级或 deadline。命令失败时保留错误并咨询管理员，不要提权重试。

## 调整后重新提交

节点、CPU、内存、GRES、应用命令或环境等不适合在线更新的内容，通常通过修改脚本后提交新作业：

1. 保留旧作业 ID、日志和 `cacct` 结果。
2. 每次只改变能验证假设的参数，例如内存、时限或任务数。
3. 在脚本或记录中注明变更。
4. 提交新作业并保存新 ID，不要混淆两次运行。

长作业优先让应用产生 checkpoint。调度器的取消、超时或节点异常不会自动保证应用数据可恢复。

## 形成有效答复

简单请求给出“命令 + 替换项 + 成功判据”。例如：

```text
使用 cbatch，因为任务无需交互。把 <partition> 和 <application_command>
替换为站点值；提交后记录作业 ID，并用 cqueue -j <job_id> -F 验证。
```

脚本审查至少检查：

- 资源数量是否与程序并行模型一致。
- 分区、账户、QoS 是否为已知站点值。
- 内存和时限是否显式且有单位。
- stdout/stderr 是否可定位到作业 ID。
- 工作目录、模块、虚拟环境、输入和输出路径是否存在。
- 是否需要 `--chdir`，以及环境是否应在脚本内显式初始化。
- 应用退出码是否被保留，而不是被后续 `echo` 覆盖。

不要把站点示例中的 `CPU`、`GPU`、`ROOT`、模块版本或节点名直接写进用户的最终脚本，除非用户已确认这些值。
