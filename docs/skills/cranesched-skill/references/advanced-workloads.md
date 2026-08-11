# 进阶工作负载

## 目录

- [资源模型](#资源模型)
- [GPU 与加速器作业](#gpu-与加速器作业)
- [多节点与 MPI](#多节点与-mpi)
- [数组作业](#数组作业)
- [作业依赖](#作业依赖)
- [作业步与重新附加](#作业步与重新附加)
- [容器作业](#容器作业)
- [进阶场景的回答边界](#进阶场景的回答边界)

## 资源模型

在生成命令前写清应用的并行模型：

```text
总任务数 = 节点数 × 每节点任务数
总 CPU 需求 = 总任务数 × 每任务 CPU 数
总内存上界 = 节点数 × 每节点 --mem
```

仅在应用确实使用多线程时提高 `--cpus-per-task`。仅在应用支持分布式运行且站点网络、软件栈已准备好时申请多节点。

`--gres` 是每任务通用资源请求。先确认站点暴露的资源名称和粒度，再选择：

```bash
--gres=gpu:1
--gres=gpu:<type>:1
```

不要假设所有站点都把 GPU 型号编码进 GRES，也不要同时混用含义不清的 GPU 参数。若当前版本 `--help` 提供站点推荐的 `--gpus-per-node`，按现场文档使用。

## GPU 与加速器作业

### 生成脚本

在基础批处理脚本上增加已确认的加速器请求：

```bash
#CBATCH --partition=<accelerator_partition>
#CBATCH --gres=gpu:1
#CBATCH --cpus-per-task=4
#CBATCH --mem=16G
#CBATCH --output=gpu_%j.out
#CBATCH --error=gpu_%j.err
```

在应用启动前记录调度器给出的可见设备变量：

```bash
env | sed -n '/^CUDA_VISIBLE_DEVICES=/p; /^NVIDIA_VISIBLE_DEVICES=/p; /^HIP_VISIBLE_DEVICES=/p; /^ASCEND.*VISIBLE_DEVICES=/p'
```

CraneSched 会按加速器类型设置相应可见性变量。不要无条件执行 `export CUDA_VISIBLE_DEVICES=0`，这可能覆盖调度器分配并让多个作业争用同一设备。

默认只记录调度器设置的设备可见性变量和应用报错。只有站点文档明确提供不会显示其他用户设备或进程的分配内诊断时才使用该命令；不要默认让普通用户运行整机 GPU 进程查看工具。模块名、驱动版本、虚拟环境和镜像由站点决定。

### 排查 GPU 问题

按顺序区分：

1. 作业未提交：核对 GRES 格式、分区访问权限和单作业资源限制。
2. 作业 Pending：查看 Reason；`Resource` 可能只是当前没有满足型号和数量的资源。
3. 作业 Running 但应用看不到设备：记录可见性变量、分配节点、应用框架报错和设备检查输出。
4. 显存不足：区分调度器的 `OutOfMemory`（主机内存）与应用自己的 GPU OOM 日志。

## 多节点与 MPI

先确认应用期望的进程数、每进程线程数和 MPI 启动方式。示例资源关系：

```bash
#CBATCH --nodes=3
#CBATCH --ntasks-per-node=4
#CBATCH --cpus-per-task=1
```

这表示最多按 3 个节点、每节点 4 个任务组织 12 个任务；不要把 `--cpus-per-task=4` 误当成 4 个 MPI 进程。

CraneSched 提供两类常见启动路径：

- 站点启用 PMIx 时，可使用 `crun --mpi=pmix` 作为进程管理方式。
- 站点提供传统 MPI 模块时，按站点文档加载匹配版本，并用其推荐的 `mpirun`/`mpiexec` 参数消费分配节点。

不要把一种路径硬套到另一站点。先询问“集群推荐 PMIx 还是模块化 MPI”，并核对当前 `crun --help`。

作业内可记录：

```bash
printf '%s\n' "${CRANE_JOB_NODELIST}" | tr ';' '\n' > crane.hosts
cat crane.hosts
```

`CRANE_JOB_NODELIST` 使用分号分隔节点。生成 hostfile 后确认总进程数与资源请求一致。

多节点失败时收集：MPI 实现和版本、启动命令、节点列表、总 rank 数、每 rank 线程数、首次错误和是否单节点可复现。不要先调整网络或系统服务。

## 数组作业

数组适合相同脚本配不同参数的独立任务。当前文档化的范围语法是 `start-end`：

```bash
cbatch --array 0-3 array_job.sh
```

不要使用 Slurm 风格的 `%并发数`，CraneSched 的该数组语法不支持并发门禁。

脚本示例：

```bash
#!/bin/bash
#CBATCH --job-name=sweep
#CBATCH --partition=<partition>
#CBATCH --nodes=1
#CBATCH --ntasks-per-node=1
#CBATCH --cpus-per-task=1
#CBATCH --mem=1G
#CBATCH --time=00:20:00
#CBATCH --output=sweep_%A_%a.out
#CBATCH --error=sweep_%A_%a.err

params=(0.1 0.2 0.5 1.0)
index="${CRANE_ARRAY_TASK_ID}"

if (( index < 0 || index >= ${#params[@]} )); then
  echo "array index out of range: ${index}" >&2
  exit 2
fi

<program> --parameter "${params[index]}"
```

可用变量与文件占位符：

- `CRANE_ARRAY_JOB_ID`：数组父/锚点作业 ID。
- `CRANE_ARRAY_TASK_ID`：当前子任务索引。
- `%A`：数组父/锚点作业 ID。
- `%a`：数组子任务索引。
- `%j`：当前作业 ID。

监控时注意聚合行和 `jobid_arraytaskid` 子任务标识：

```bash
cqueue -j <anchor_job_id> -F
cacct -j <anchor_job_id> -F
```

## 作业依赖

依赖可用于构建简单流水线：

依赖 ID 必须来自用户本人刚提交并记录的上游作业。不要接受来源不明或其他用户的作业 ID；站点若允许跨用户依赖，仍需明确的站点政策和授权。

| 类型 | 启动条件 |
|---|---|
| `after` | 依赖作业开始或取消，离开 Pending |
| `afterok` | 依赖作业以 0 成功完成 |
| `afternotok` | 依赖作业非 0 失败，包括超时或节点错误 |
| `afterany` | 依赖作业结束，不论成功失败 |

基本示例：

```bash
cbatch --dependency afterok:<own_upstream_job_id> next.sh
```

延迟必须使用带单位格式：

```bash
cbatch --dependency afterok:<own_upstream_job_id>+10s next.sh
cbatch --dependency afterok:<own_upstream_job_id>+90m next.sh
```

不要使用 `HH:MM:SS` 表示依赖延迟，因为冒号用于分隔作业 ID，可能得到错误依赖。

组合规则：

- 使用 `,` 表示所有条件都满足（AND）。
- 使用 `?` 表示任一条件满足（OR）。
- 不要在同一依赖字符串中混用 `,` 和 `?`。

查看未满足依赖：

```bash
ccontrol show job <dependent_job_id>
```

`DependencyNeverSatisfied` 表示成功条件等已经不可能满足；不要仅继续等待，应核对上游状态并决定是否取消后重新提交。

## 作业步与重新附加

在一个 `calloc` 分配内使用 `crun` 创建作业步：

```bash
calloc --nodes=2 --cpus-per-task=4 --partition=<partition>
crun --nodes=1 --cpus-per-task=2 -- <program_a>
crun --nodes=1 --cpus-per-task=2 -- <program_b>
```

作业步请求不能超过父作业分配。查看详情：

```bash
cqueue --step -j "${CRANE_JOB_ID}"
ccontrol show step <job_id>.<step_id>
```

仅在用户明确请求、确认步骤属于自己并理解终端输入会转发给任务后，附加到正在运行的交互式作业步：

```bash
cattach <job_id>.<step_id>
```

在非 PTY 模式下，`Ctrl+C` 断开 `cattach` 而不终止作业步。`cattach` 必须在站点支持的提交节点使用；前后用 `cqueue --step -j <job_id>` 确认自己的目标状态。

取消单个步骤：

```bash
cqueue --step -j <job_id>
ccancel <job_id>.<step_id>
cacct -j <job_id>.<step_id> -F
```

这会改变运行状态，执行前确认步骤 ID，执行后确认该步骤在历史中进入 `Cancelled`；取消父作业会结束其全部步骤。

## 容器作业

只有集群已启用容器运行时、用户有分区权限且计算节点能访问镜像时才建议使用。

一次性容器作业：

```bash
ccon --partition=<partition> run <image> <command> <args>
```

需要交互式 shell 时，确认镜像与目标分区后使用：

```bash
ccon --partition=<partition> run -it <image> -- /bin/bash
```

在 TTY 会话中用 `Ctrl-P`、`Ctrl-Q` 脱离而不停止容器。挂载卷时只使用用户有权访问且计算节点可见的路径；不要挂载系统目录。使用 `--env` 时不要把密码或令牌直接写入命令行。

批处理编排：

```bash
#!/bin/bash
#CBATCH --job-name=container-job
#CBATCH --partition=<partition>
#CBATCH --nodes=1
#CBATCH --time=00:30:00
#CBATCH --pod
#CBATCH --output=container_%j.out
#CBATCH --error=container_%j.err

if ! container_id=$(ccon run -d <image> -- <command> <args>); then
  echo "container launch failed" >&2
  exit 1
fi
printf 'container_id=%s\n' "$container_id"
ccon wait
```

`-d` 让容器步骤后台启动并返回属于该作业的步骤 ID；保存它供后续定向检查。启动失败立即退出，`ccon wait` 作为脚本最后一条命令把等待结果传递为脚本退出状态。

容器只读检查：

```bash
ccon inspectp <job_id>
ccon inspect <job_id>.<step_id>
ccon logs --tail 100 <job_id>.<step_id>
```

这里的作业和步骤 ID 必须已确认属于当前用户。通用文档没有说明 `ccon pods` 或 `ccon ps` 会按当前用户过滤，也没有提供用户过滤参数；除非现场帮助或站点文档明确保证本人范围，否则不要让普通用户运行这两个无过滤列表命令。

多节点容器步骤的日志必须指定该作业分配中的目标节点：

```bash
ccon logs -n <node> --tail 100 <job_id>.<step_id>
```

仅在用户明确请求、确认容器属于自己并理解影响后提供交互或停止命令：

```bash
ccon attach <job_id>.<step_id>
ccon exec -it <job_id>.<step_id> /bin/bash
ccon stop <job_id>.<step_id>
```

多节点容器步骤的交互也必须指定目标节点：

```bash
ccon attach -n <node> <job_id>.<step_id>
ccon exec -it -n <node> <job_id>.<step_id> /bin/bash
```

`attach` 和 `exec` 会与运行中容器交互；`stop` 会终止容器。执行后用 `ccon inspectp <job_id>`、`ccon inspect <job_id>.<step_id>` 和 `cqueue -j <job_id>` 验证，不要对其他用户或来源不明的 ID 操作。

若命令返回 `ERR_CRI_DISABLED`，不要让普通用户尝试启用服务；升级给管理员。若容器未就绪，先同时查看父作业状态与容器日志。多节点操作报不支持时，先核对当前 `--help`：`logs`、`attach` 和 `exec` 使用 `-n <node>` 选择目标节点；其他确实不支持的操作再缩小为单节点或改用站点支持的运行方式。

不要默认使用 `latest` 镜像标签；推荐用户固定已验证的标签或摘要。不要在回答中要求粘贴镜像仓库密码。

## 进阶场景的回答边界

生成最终方案前至少确认：

- 当前 CraneSched 版本或相关命令的 `--help`。
- 站点支持的分区、资源名和启动器。
- 应用的并行模型，而不只是“想跑得更快”。
- 成功判据和输出位置。

不确定时提供两个明确分支，例如“若站点启用 PMIx，使用 A；否则按站点 MPI 模块使用 B”，不要混成一个无法执行的脚本。
