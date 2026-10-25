# 作业环境变量

CraneSched 在执行作业时会自动设置一系列环境变量，供作业脚本和程序使用。这些环境变量包含了作业的各种信息，如作业 ID、资源分配、时间限制等。

本页说明计算节点上任务的环境、原生 PMIx 读取的配置，以及钩子专用变量。`calloc` 打开的本地交互 Shell 继承提交进程的环境并添加 `CRANE_JOB_ID`，不会收到本页列出的完整计算节点环境。

---

## 作业信息环境变量

### CRANE_JOB_ID
- **说明**：作业的唯一标识符
- **类型**：整数
- **示例**：`12345`

### CRANE_JOB_NAME
- **说明**：作业名称
- **类型**：字符串
- **示例**：`my_simulation`

### CRANE_JOB_ACCOUNT
- **说明**：提交作业所使用的账户
- **类型**：字符串
- **示例**：`research_group`

### CRANE_JOB_PARTITION
- **说明**：作业运行的分区名称
- **类型**：字符串
- **示例**：`compute`

### CRANE_JOB_QOS
- **说明**：作业的服务质量等级
- **类型**：字符串
- **示例**：`normal`

### CRANE_JOB_UID
- **说明**：作业用户的 UID
- **类型**：整数
- **示例**：`1000`

### CRANE_JOB_GID
- **说明**：提交时校验后用于执行作业的有效 GID，可能与用户的主组不同
- **类型**：整数
- **示例**：`1000`

---

## 数组作业环境变量

这些变量仅会在展开后的数组子任务中设置。

### CRANE_ARRAY_JOB_ID
- **说明**：整个数组的父/锚点作业 ID
- **类型**：整数
- **示例**：`1000`

### CRANE_ARRAY_TASK_ID
- **说明**：当前数组任务索引
- **类型**：整数
- **示例**：`7`

---

## 作业步环境变量

CraneSched 为执行单元设置任务数量和任务标识。`CRANE_STEP_ID`、`CRANE_STEP_NODELIST` 和 `CRANE_STEP_NUM_NODES` 用于非 DAEMON、非批处理式 PRIMARY 的执行上下文，包括直接提交的 `crun` 和作业内的 `crun` 作业步。批处理脚本与 `calloc` 分配的 PRIMARY 不设置这三个变量；它们在内部仍由作业步承载。

### CRANE_STEP_ID
- **说明**：父作业内的作业步ID（仅数字部分，不包含作业ID）
- **类型**：整数
- **示例**：`1`（对于作业步123.1，此值为`1`）
- **可用性**：仅在作业步执行上下文中设置
- **注意**：不能仅凭此变量判断是否存在父作业；`crun` 使用 `CRANE_JOB_ID` 判断是否在现有分配中创建作业步。

### CRANE_STEP_NODELIST
- **说明**：分配给当前作业步的节点列表，节点名称以分号分隔
- **类型**：字符串
- **示例**：`node01;node02`
- **可用性**：仅在作业步执行上下文中设置

### CRANE_STEP_NUM_NODES
- **说明**：分配给当前作业步的节点数量
- **类型**：整数
- **示例**：`2`
- **可用性**：仅在作业步执行上下文中设置

### CRANE_STEP_NUM_TASKS
- **说明**：当前执行单元的任务总数
- **类型**：整数
- **示例**：`4`

### CRANE_GTIDS
- **说明**：当前节点承载的全局任务 ID，以逗号分隔
- **类型**：整数列表
- **示例**：`0,2`

### CRANE_LOCALID
- **说明**：当前任务在所在节点上的本地序号，从 0 开始
- **类型**：整数
- **示例**：`1`
- **可用性**：仅在任务进程及其 task epilog 中设置

### CRANE_PROCID / CRANE_PROC_ID
- **说明**：当前执行单元内从 0 开始的全局任务 ID，两个变量互为别名
- **类型**：整数
- **示例**：`2`
- **可用性**：在原生任务进程及 task epilog 中设置；容器任务初始化路径不会额外生成这两个别名。

---

## 节点和资源环境变量

### CRANE_JOB_NODELIST
- **说明**：分配给作业的节点列表，节点名称以分号分隔
- **类型**：字符串
- **示例**：`node01;node02;node03`

### CRANE_JOB_NUM_NODES
- **说明**：分配给作业的节点数量
- **类型**：整数
- **示例**：`3`

### CRANE_JOB_CPUS_PER_NODE
- **说明**：当前节点上分配给作业的 CPU 数量
- **类型**：数值
- **示例**：`4`

### CRANE_TASKS_PER_NODE
- **说明**：按节点列表顺序排列的任务数。在作业上下文中表示作业任务分布，在作业步上下文中表示当前作业步的任务分布
- **类型**：逗号分隔的整数列表
- **示例**：`2,1,1`

### CRANED_NODENAME
- **说明**：当前执行节点的 Craned 节点名称
- **类型**：字符串
- **示例**：`node01`

### CRANE_WORKING_DIR
- **说明**：当前作业或作业步请求的工作目录
- **类型**：字符串
- **示例**：`/home/user/project`

### CRANE_MEM_PER_NODE
- **说明**：当前节点分配给作业的内存，按 1024² 字节换算并带 `M` 后缀
- **类型**：字符串
- **示例**：`4096M`

### 其他分配与提交信息

| 变量 | 含义及格式 |
| --- | --- |
| `CRANE_CLUSTER_NAME` | 管理员配置的集群名称 |
| `CRANE_NODELIST` | `CRANE_JOB_NODELIST` 的别名，节点名称以分号分隔 |
| `CRANE_NODEID` | 当前节点在作业分配节点列表中的序号，从 0 开始 |
| `CRANE_CPUS_PER_TASK` | 作业分配请求的每任务 CPU 数，保留两位小数，如 `2.00` |
| `CRANE_CPUS_ON_NODE` | 当前节点分配给作业的 CPU 数，保留两位小数 |
| `CRANE_MEM_PER_CPU` | 当前节点分配内存除以分配 CPU 数，单位为 MiB/CPU，保留两位小数且无后缀；CPU 数为零时为 `0.00` |
| `CRANE_NTASKS` | 作业分配请求的任务总数 |
| `CRANE_NTASKS_PER_NODE` | 作业分配请求的每节点任务数，不同于 `CRANE_TASKS_PER_NODE` 的实际分布 |
| `CRANE_GPUS` | 当前节点分配给作业的 GPU 数，包含 `0` |
| `CRANE_SUBMIT_DIR` | 提交作业时所在的目录 |
| `CRANE_SUBMIT_HOST` | 提交作业的主机 |

以上变量最初由作业分配生成。普通作业步继承 `CRANE_NTASKS`、`CRANE_NTASKS_PER_NODE`、`CRANE_CPUS_PER_TASK`、`CRANE_CPUS_ON_NODE` 和 `CRANE_NODEID`，不会将其改写为作业步的值。查询当前作业步时，应使用 `CRANE_STEP_*`、`CRANE_GTIDS` 及任务 ID；原生 PMIx 任务的节点列表有下文说明的例外。

---

## 时间相关环境变量

### CRANE_TIMELIMIT
- **说明**：当前执行单元的时间限制，格式为 HH:MM:SS；仅在合并后的环境尚无 `CRANE_TIMELIMIT` 时填入
- **类型**：字符串
- **示例**：`02:30:00` 表示 2 小时 30 分钟

### CRANE_JOB_END_TIME
- **说明**：环境初始化时刻加上作业时间限制，以自 Unix 纪元以来的 `system_clock` 时钟计数表示（当前 Linux 构建中为纳秒）。这是一个快照，不是持续更新的截止时间。
- **类型**：整数
- **示例**：`1699876543000000000`

### CRANE_JOB_START_TIME
- **说明**：作业开始时间的 Unix 时间戳，单位为秒
- **类型**：整数
- **示例**：`1699876543`

---

## GPU 和加速器环境变量

当作业被分配了 GPU 或其他加速器设备时，CraneSched 会设置相应的设备可见性环境变量。

变量由设备配置的 `EnvInjector` 选择，根据当前节点上的作业分配生成。列表是从 `0` 到分配设备数减一的逻辑序号，不是物理设备 UUID 列表；某类注入器没有分配设备时，不生成对应变量。这些作业级列表会被作业步继承，不应据此推定存在单独的每任务 GPU 绑定。

### CUDA_VISIBLE_DEVICES
- **说明**：NVIDIA GPU 设备的可见列表，用于 CUDA 应用程序
- **类型**：逗号分隔的整数列表
- **示例**：`0,1,2`
- **适用于**：NVIDIA GPU

### NVIDIA_VISIBLE_DEVICES
- **说明**：NVIDIA GPU 设备的可见列表，用于容器运行时
- **类型**：逗号分隔的整数列表
- **示例**：`0,1,2`
- **适用于**：NVIDIA GPU

### HIP_VISIBLE_DEVICES
- **说明**：AMD GPU 设备的可见列表，用于 HIP/ROCm 应用程序
- **类型**：逗号分隔的整数列表
- **示例**：`0,1`
- **适用于**：AMD GPU

### ASCEND_RT_VISIBLE_DEVICES
- **说明**：华为昇腾 NPU 设备的可见列表
- **类型**：逗号分隔的整数列表
- **示例**：`0,1,2,3`
- **适用于**：华为昇腾 NPU

### ASCEND_VISIBLE_DEVICES
- **说明**：华为昇腾 NPU 设备的可见列表（别名）
- **类型**：逗号分隔的整数列表
- **示例**：`0,1,2,3`
- **适用于**：华为昇腾 NPU

---

## 其他环境变量

### HOME
- **说明**：用户的主目录路径
- **类型**：字符串
- **示例**：`/home/username`
- **备注**：可以从提交环境传播而来。`--get-user-env` 仅在缺少此变量时，根据执行用户的账户信息补充。

### SHELL
- **说明**：用户的默认 Shell
- **类型**：字符串
- **示例**：`/bin/bash`
- **备注**：可以从提交环境传播而来。`--get-user-env` 仅在缺少此变量时，根据执行用户的账户信息补充。

### TERM
- **说明**：终端类型
- **类型**：字符串
- **示例**：`xterm-256color`
- **备注**：可以从提交环境传播而来。对于 `crun`，如果变量缺失且终端元数据非空，则使用该元数据补充。

### DISPLAY / XAUTHORITY

启用 X11 的原生 `crun` 任务会将 `DISPLAY` 设置为配置的 X11 端点，将 `XAUTHORITY` 设置为生成的认证文件路径，覆盖传播的同名值。未启用 X11 时，这些变量仍可能通过普通环境传播存在。

## 钩子环境

| 变量 | CraneSched 设置的位置 |
| --- | --- |
| `CRANE_SCRIPT_CONTEXT=epilog` | 计算节点上的作业 epilog 环境 |
| `CRANE_SCRIPT_CONTEXT=task_epilog` | 任务 epilog 环境 |
| `USER`、`LOGNAME` | 账户查询成功时，task epilog 中设置为作业用户的用户名 |

Task epilog 接收作业步环境，并补充 `CRANE_PROCID`、`CRANE_PROC_ID`、`CRANE_LOCALID`；启用 Slurm 兼容时也包含对应的 Slurm ID。该环境由 supervisor 重新构建，不会复制用户进程或 task prolog 后续做出的环境修改。作业 epilog 接收作业级环境，不应假定其中存在任务 ID。CraneSched 不会为 task prolog 或普通用户任务自动添加 `CRANE_SCRIPT_CONTEXT` 标记。

Task prolog 使用准备好的任务环境，并可通过输出 `export name=value`、`unset name` 修改随后执行的任务环境。参见 [Prolog/Epilog 指南](../deployment/configuration/prolog_epilog_guide.md)。

## 原生 PMIx 环境

### 用户提供的配置

以下设置从 `crun --mpi=pmix` 提交的作业步环境读取。请在调用 `crun` 前导出，或通过 `--export` 传入。它们是输入配置，不是 CraneSched 自动生成的分配信息。

| 变量 | 可用值及默认行为 |
| --- | --- |
| `CRANE_PMIX_TIMEOUT` | PMIx 操作及集合通信清理的超时秒数，默认 `5`。应使用正整数；空值或转换失败时保留默认值。 |
| `CRANE_PMIX_FENCE` | `tree` 或 `ring`。未设置或无法识别时，回调在收集非空数据时选择 `ring`，其他情况选择 `tree`，并非始终默认 tree。 |
| `CRANE_PMIX_DIRECT_CONN_UCX` | 仅精确值 `true` 启用 UCX 传输 PMIx 元数据，其他值使用 gRPC。需要 CraneSched 编译时启用 UCX，否则启动失败。 |
| `PMIXP_PMIXLIB_TMPDIR` | 可选的 PMIx 客户端文件基础目录，须对作业用户可访问。CraneSched 在其后添加 `pmix.crane.cli/spmix_appdir_<uid>.<job>.<step>`；未设置或为空时使用 PMIx 默认目录。 |

### 注入 PMIx 任务的变量

- PMIx 库通过 `PMIx_server_setup_fork` 提供客户端连接、rank 等环境信息；具体变量名取决于所安装的 PMIx 版本，并非 CraneSched 固定清单。
- `OMPI_MCA_orte_precondition_transports` 用于 Open MPI 兼容。如果 PMIx 生成的环境中没有它，CraneSched 会根据作业 ID 和作业步 ID 生成十六进制键。
- `CRANE_JOB_NODELIST` 和 `CRANE_NODELIST` 会被替换为**当前作业步**的分号分隔节点列表，其他 `CRANE_JOB_*` 值仍描述作业分配。
- `EnableSlurmCompatibleEnv: false` 时，PMIx 任务仍会收到 `SLURM_JOBID`、`SLURM_JOB_NODELIST`、`SLURM_NODELIST` 和 `SLURM_STEP_ID`。其中两个节点列表均描述**作业步**，并在可能时采用压缩主机列表格式。
- `EnableSlurmCompatibleEnv: true` 时，Slurm 作业节点列表仍是完整作业分配，作业步节点列表及 rank 变量描述 PMIx 作业步，与下文表格一致。

这些 PMIx 注入值会覆盖原生任务已准备环境中的同名值。Task epilog 使用前述独立重建的环境，而不是 PMIx 子进程环境的副本。部署与启动示例见 [PMIx 使用指南](../deployment/configuration/pmix.md)。

---

## Slurm 兼容环境变量

在集群 `config.yaml` 顶层设置 `EnableSlurmCompatibleEnv: true` 后，CraneSched 会额外生成下列变量；默认值为 `false`。参见[集群配置](../deployment/configuration/config.md)。这些变量用于脚本和启动器兼容，不表示所有 Slurm 命令选项都已实现。

下表描述开关启用后的自动生成行为。关闭开关不会清除用户传播的 `SLURM_*` 变量，原生 PMIx 也仍会生成前述四个变量。

### 作业信息

| 变量 | 当前含义 |
| --- | --- |
| `SLURM_JOB_ID`、`SLURM_JOBID` | 作业 ID |
| `SLURM_JOB_NAME`、`SLURM_JOB_ACCOUNT`、`SLURM_JOB_PARTITION`、`SLURM_JOB_QOS` | 作业名称、账户、分区及 QoS |
| `SLURM_JOB_UID`、`SLURM_JOB_GID` | 作业用户 UID 和有效 GID |
| `SLURM_CLUSTER_NAME`、`SLURMD_NODENAME` | 集群名和当前 Craned 节点名 |
| `SLURM_JOB_NODELIST`、`SLURM_NODELIST` | 完整作业分配的节点名，逗号分隔，不使用 `node[01-03]` 压缩格式 |
| `SLURM_JOB_NUM_NODES`、`SLURM_JOB_NODES` | 作业分配的节点数量 |
| `SLURM_JOB_CPUS_PER_NODE` | 当前节点上作业分配的 CPU 数，是单个数值，不是所有节点的压缩列表 |
| `SLURM_SUBMIT_DIR`、`SLURM_SUBMIT_HOST` | 提交目录和提交主机 |
| `SLURM_WORKING_DIR` | 当前执行单元的工作目录，可能与提交目录不同 |
| `SLURM_JOB_START_TIME` | 作业开始时间，Unix 秒级时间戳 |
| `SLURM_ARRAY_JOB_ID`、`SLURM_ARRAY_TASK_ID` | 数组父作业 ID 和任务索引，仅在展开的数组子任务中设置 |

### 作业步与任务信息

| 变量 | 当前含义 |
| --- | --- |
| `SLURM_NNODES` | 作业上下文中的作业节点数；`crun` 作业步中为该步的节点数 |
| `SLURM_NTASKS`、`SLURM_NPROCS` | 作业上下文中的作业任务总数；`crun` 作业步中为该步实际启动的任务数 |
| `SLURM_NTASKS_PER_NODE` | 当前作业或作业步请求的每节点任务数 |
| `SLURM_TASKS_PER_NODE` | 按当前节点列表顺序排列的实际任务数，如 `2,1,1`，不使用 Slurm 的 `2(x3)` 压缩形式 |
| `SLURM_STEP_ID`、`SLURM_STEPID` | 数字作业步 ID；与 `CRANE_STEP_ID` 使用相同的上下文规则 |
| `SLURM_STEP_NODELIST`、`SLURM_STEP_NUM_NODES` | 当前作业步的逗号分隔节点列表及节点数；与 `CRANE_STEP_NODELIST` 使用相同的上下文规则 |
| `SLURM_STEP_NUM_TASKS` | 当前执行单元实际启动的任务数；批处理 PRIMARY 为脚本任务数，不是整份分配可容纳的 MPI rank 数 |
| `SLURM_STEP_TASKS_PER_NODE` | `crun` 作业步内按节点顺序排列的实际任务数 |
| `SLURM_NODEID`、`SLURM_STEP_NODEID` | 从 0 开始的节点序号；`crun` 作业步中相对于该步的节点列表。`SLURM_STEP_NODEID` 仅用于作业步上下文 |
| `SLURM_PROCID`、`SLURM_LOCALID` | 任务进程及 task epilog 中的全局任务编号、节点内任务编号，从 0 开始 |
| `SLURM_STEP_PROCID`、`SLURM_STEP_LOCALID` | `crun` 作业步任务进程及 task epilog 中对应的任务编号；PMIx 任务按 rank 设置 |
| `SLURM_GTIDS` | 当前节点承载的全局任务编号，逗号分隔 |
| `SLURM_CPUS_ON_NODE`、`SLURM_CPUS_PER_TASK` | 当前执行单元在本节点的 CPU 分配量，以及每任务请求的 CPU 数 |
| `SLURM_MEM_PER_NODE` | 当前执行单元在本节点的内存，按 1024² 字节换算的整数，不带 `M` 后缀 |
| `SLURM_GPUS_ON_NODE` | 本节点上非零的作业 GPU 数；作业步 GPU 数大于零时会替换该值。零 GPU 的作业步不会清除继承值，因此不能仅凭此变量判断该步已分配 GPU。 |
| `SLURM_CPU_BIND_TYPE` | 当前设置为 `none`；不表示已实现 Slurm 的 `--cpu-bind` 选项 |
| `SLURM_EXTERNAL_LAUNCHER` | 外部启动器步骤设置为 `1`；`crun` 也会读取此变量以启用该模式，见[crun](../command/crun.md) |

批处理脚本中的 `SLURM_NTASKS` 描述整份作业分配，脚本本身的 `SLURM_STEP_NUM_TASKS` 可以为 `1`。进入 `crun` 作业步后，`SLURM_NTASKS`、`SLURM_NNODES` 和任务分布改为该步的值，`SLURM_JOB_*` 仍描述父作业。外部启动器步骤中的任务数是启动器进程数，实际 MPI rank 数由启动器决定。

## 使用示例

### 在批处理脚本中使用

```bash
#!/bin/bash
#SBATCH --job-name=test_job
#SBATCH --nodes=2
#SBATCH --time=01:00:00

echo "作业 ID: $CRANE_JOB_ID"
echo "数组作业 ID: ${CRANE_ARRAY_JOB_ID:-N/A}"
echo "数组任务索引: ${CRANE_ARRAY_TASK_ID:-N/A}"
echo "作业名称: $CRANE_JOB_NAME"
echo "节点列表: $CRANE_JOB_NODELIST"
echo "节点数量: $CRANE_JOB_NUM_NODES"
echo "时间限制: $CRANE_TIMELIMIT"
echo "每节点内存: ${CRANE_MEM_PER_NODE}"

# 如果有 GPU 分配
if [ ! -z "$CUDA_VISIBLE_DEVICES" ]; then
    echo "可用 GPU: $CUDA_VISIBLE_DEVICES"
fi

# 运行程序
./my_program
```

### 在 Python 程序中使用

```python
import os

# 获取作业信息
job_id = os.environ.get('CRANE_JOB_ID', 'unknown')
job_name = os.environ.get('CRANE_JOB_NAME', 'unknown')
num_nodes = int(os.environ.get('CRANE_JOB_NUM_NODES', '1'))

print(f"作业 ID: {job_id}")
print(f"作业名称: {job_name}")
print(f"节点数量: {num_nodes}")

# 检查 GPU 可用性
cuda_devices = os.environ.get('CUDA_VISIBLE_DEVICES')
if cuda_devices:
    print(f"可用 GPU: {cuda_devices}")
    gpu_count = len(cuda_devices.split(','))
    print(f"GPU 数量: {gpu_count}")
```

---

## 注意事项

1. **传播选项**：`cbatch`、`calloc`、`crun` 使用 `--export`，默认 `ALL`。`NIL` 不添加提交端的普通环境变量；`NONE` 还会启用 `--get-user-env`。列表支持 `NAME`、`NAME=value`、`ALL`，也会启用 `--get-user-env`。此传播逻辑会保留已有的 `CRANE_*` 变量。`CRANE_EXPORT_ENV` 是前端内部传递选项值的变量，不是应在作业脚本中设置的分配变量。

2. **合并顺序**：作业自动生成值优先于提交环境中的同名值，然后由作业步、任务初始化代码设置对应上下文字段。原生 PMIx 随后可能覆盖部分值，task prolog 输出也可在执行前导出或删除变量；不存在所有 `CRANE_*` 名称均不可修改的统一规则。容器使用 `ccon -e/--env` 提供容器专用值，但不会覆盖后端已准备环境中的同名变量。

3. **登录环境**：`--get-user-env` 补充缺失的 `HOME`、`SHELL`，并通过 `/bin/bash --login -c` 执行原生命令。登录启动文件可能继续修改环境；除非用户启动文件自行加载，否则不保证执行 `.bashrc`。任务不会直接继承守护进程的全部环境。

4. **前端读取**：`crun` 使用 `CRANE_JOB_ID` 识别已有分配，并读取 `CRANE_NTASKS`、`CRANE_JOB_NUM_NODES`、`CRANE_NTASKS_PER_NODE`、`CRANE_MEM_PER_NODE` 作为作业步默认值；也会独立于后端兼容开关读取 `SLURM_EXTERNAL_LAUNCHER`。`ccontrol show hostnames` 省略主机列表参数时，依次检查 `SLURM_JOB_NODELIST`、`SLURM_NODELIST`、`CRANE_JOB_NODELIST`、`CRANE_NODELIST`。

5. **本页范围**：构建变量及依赖库专用设置见对应部署指南。`CRANE_CGROUP_OP_CONCURRENCY`、`CRANE_CGROUP_OP_SEM_NAME`、`CRANE_CGROUP_V2_FAST_PATH`、`CRANE_CGROUP_V2_CLEANUP_MODE`、`CRANE_TRACE_HOOK_MAX_REQUEST_BYTES` 用于守护进程与 supervisor 内部配置传递，不是自动生成的作业变量，也不是建议使用的作业提交控制项。UCX 服务端在自身进程中将 `UCX_MEM_MMAP_RELOC`、`UCX_MEM_MALLOC_HOOKS`、`UCX_MEM_MALLOC_RELOC`、`UCX_MEM_EVENTS` 设置为 `no`，不保证用户任务中存在这些变量。
