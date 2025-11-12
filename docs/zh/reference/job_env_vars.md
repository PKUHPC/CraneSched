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

### CRANE_MEM_PER_NODE
- **说明**：每个节点分配的内存大小，单位为 MB
- **类型**：整数
- **示例**：`4096`

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

## 使用示例

### 在批处理脚本中使用

```bash
#!/bin/bash
#SBATCH --job-name=test_job
#SBATCH --nodes=2
#SBATCH --time=01:00:00

echo "作业 ID: $CRANE_JOB_ID"
echo "作业名称: $CRANE_JOB_NAME"
echo "节点列表: $CRANE_JOB_NODELIST"
echo "节点数量: $CRANE_JOB_NUM_NODES"
echo "时间限制: $CRANE_TIMELIMIT"
echo "每节点内存: ${CRANE_MEM_PER_NODE}MB"

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
