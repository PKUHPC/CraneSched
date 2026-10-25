# 作业环境变量

CraneSched 在执行作业时会自动设置一系列环境变量，供作业脚本和程序使用。这些环境变量包含了作业的各种信息，如作业 ID、资源分配、时间限制等。

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
- **说明**：作业用户的主 GID
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

---

## 时间相关环境变量

### CRANE_TIMELIMIT
- **说明**：作业的时间限制，格式为 HH:MM:SS
- **类型**：字符串
- **示例**：`02:30:00` 表示 2 小时 30 分钟

### CRANE_JOB_END_TIME
- **说明**：作业预计结束时间的时间戳（自 Unix 纪元以来的纳秒数）
- **类型**：整数
- **示例**：`1699876543000000000`

### CRANE_JOB_START_TIME
- **说明**：作业开始时间的 Unix 时间戳，单位为秒
- **类型**：整数
- **示例**：`1699876543`

---

## GPU 和加速器环境变量

当作业被分配了 GPU 或其他加速器设备时，CraneSched 会设置相应的设备可见性环境变量。

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
- **备注**：仅当使用 `--get-user-env` 选项时设置

### SHELL
- **说明**：用户的默认 Shell
- **类型**：字符串
- **示例**：`/bin/bash`
- **备注**：仅当使用 `--get-user-env` 选项时设置

### TERM
- **说明**：终端类型
- **类型**：字符串
- **示例**：`xterm-256color`
- **备注**：仅在交互式作业中设置

---

## Slurm 兼容环境变量

在集群 `config.yaml` 顶层设置 `EnableSlurmCompatibleEnv: true` 后，CraneSched 会额外生成下列变量；默认值为 `false`。参见[集群配置](../deployment/configuration/config.md)。这些变量用于脚本和启动器兼容，不表示所有 Slurm 命令选项都已实现。

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
| `SLURM_GPUS_ON_NODE` | 当前执行单元在本节点的 GPU 数，仅在数量大于 0 时生成 |
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

1. **环境变量优先级**：用户在提交作业时通过 `-E` 或 `--env` 选项指定的环境变量会覆盖默认值，但 CRANE 系统环境变量的优先级最高，会覆盖用户设置的同名变量。

2. **节点列表格式**：`CRANE_JOB_NODELIST` 使用分号（`;`）分隔节点名称，与某些其他调度系统使用的格式可能不同。

3. **设备环境变量**：GPU 和加速器相关的环境变量仅在作业实际分配了相应设备时才会设置。

4. **时间戳格式**：`CRANE_JOB_END_TIME` 使用纳秒级时间戳，在使用时可能需要进行单位转换。

5. **`--get-user-env` 选项**：使用该选项时，作业环境会从执行节点的用户环境初始化，包括加载 `.bashrc`、`/etc/profile` 等配置文件。
