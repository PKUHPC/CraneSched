# 使用案例

本指南提供使用 CraneSched 的常见工作流程的实际示例。每个用例都包含从作业提交到结果检索的完整演练。

---

## 用例 1：基础批处理作业

**场景**：在单个节点上运行简单的计算任务。

### 任务描述

您有一个处理数据并将结果输出到文件的脚本。这是批处理最常见的用例。

### 步骤 1：准备脚本

创建名为 `simple_job.sh` 的文件：

```bash
#!/bin/bash
#CBATCH --job-name=simple_test
#CBATCH --partition=CPU
#CBATCH --nodes=1
#CBATCH --ntasks-per-node=1
#CBATCH --cpus-per-task=1
#CBATCH --mem=100M
#CBATCH --time=0:05:00
#CBATCH --output=job_%j.out

# 打印作业信息
echo "作业开始时间: $(date)"
echo "运行节点: $(hostname)"
echo "作业 ID: $CRANE_JOB_ID"

# 模拟一些计算工作
echo "正在处理数据..."
sleep 5
echo "数据处理完成!"

# 打印作业完成信息
echo "作业结束时间: $(date)"
```

### 步骤 2：提交作业

```bash
cbatch simple_job.sh
```

预期输出：
```
Job <12345> is submitted to partition <CPU>
```

### 步骤 3：检查作业状态

```bash
cqueue -j 12345
```

或检查您的所有作业：

```bash
cqueue --self
```

### 步骤 4：查看结果

作业完成后，查看输出文件：

```bash
cat job_12345.out
```

---

## 用例 2：交互式会话

**场景**：您需要在计算节点上交互式地测试代码或调试应用程序。

### 任务描述

与其提交批处理作业，您希望直接通过命令行访问具有特定资源的计算节点。

### 步骤 1：请求交互式会话

```bash
crun -p CPU -N 1 -c 2 --mem 500M -t 1:00:00 /bin/bash
```

这将分配：
- CPU 分区的 1 个节点
- 2 个 CPU 核心
- 500 MB 内存
- 1 小时时间限制

### 步骤 2：交互式工作

连接后，您将在计算节点上看到 shell 提示符：

```bash
# 检查当前位置
hostname

# 检查分配的资源
echo $CRANE_JOB_NODELIST

# 运行您的命令
python test_script.py
./my_program --input data.txt

# 完成后退出
exit
```

退出时资源将自动释放。

---

## 用例 3：多节点并行作业

**场景**：在多个节点上运行 MPI 应用程序进行并行计算。

### 任务描述

您有一个需要在多个计算节点上运行的 MPI 程序，以并行方式处理数据。

### 步骤 1：准备 MPI 作业脚本

创建 `mpi_job.sh`：

```bash
#!/bin/bash
#CBATCH --job-name=mpi_test
#CBATCH --partition=CPU
#CBATCH --nodes=3
#CBATCH --ntasks-per-node=4
#CBATCH --cpus-per-task=1
#CBATCH --mem=2G
#CBATCH --time=1:00:00
#CBATCH --output=mpi_job_%j.out

# 加载 MPI 模块（根据您的环境调整）
module load mpich/4.0

# 从分配的节点生成机器文件
echo "$CRANE_JOB_NODELIST" | tr ";" "\n" > machinefile

# 显示分配的节点
echo "运行在以下节点上:"
cat machinefile

# 运行 MPI 应用程序
# 总共使用 12 个进程（3 个节点 × 每个节点 4 个任务）
mpirun -n 12 -machinefile machinefile ./my_mpi_program

echo "MPI 作业完成时间: $(date)"
```

### 步骤 2：编译 MPI 程序（如需要）

```bash
mpicc -o my_mpi_program my_mpi_program.c
```

### 步骤 3：提交作业

```bash
cbatch mpi_job.sh
```

### 步骤 4：监控进度

```bash
# 检查作业状态
cqueue -j <job_id>

# 实时查看输出（作业运行时）
tail -f mpi_job_<job_id>.out
```

---

## 用例 4：GPU 加速作业

**场景**：使用 GPU 资源训练机器学习模型。

### 任务描述

您想运行需要 GPU 加速的深度学习训练脚本。

### 步骤 1：准备 GPU 作业脚本

创建 `gpu_training.sh`：

```bash
#!/bin/bash
#CBATCH --job-name=gpu_training
#CBATCH --partition=GPU
#CBATCH --nodes=1
#CBATCH --ntasks-per-node=1
#CBATCH --cpus-per-task=4
#CBATCH --mem=16G
#CBATCH --gres=gpu:1
#CBATCH --time=2:00:00
#CBATCH --output=training_%j.out
#CBATCH --error=training_%j.err

# 加载必要的模块
module load cuda/11.8
module load python/3.9

# 激活虚拟环境（如果使用）
source ~/venv/bin/activate

# 显示 GPU 信息
echo "分配的 GPU:"
nvidia-smi

# 设置环境变量
export CUDA_VISIBLE_DEVICES=0

# 运行训练脚本
echo "训练开始时间: $(date)"
python train_model.py \
    --data-path /path/to/dataset \
    --batch-size 32 \
    --epochs 100 \
    --output-dir ./results

echo "训练完成时间: $(date)"
```

### 步骤 2：提交 GPU 作业

```bash
cbatch gpu_training.sh
```

### 步骤 3：检查 GPU 作业状态

```bash
# 检查作业是否正在运行以及分配了哪个 GPU
cqueue -j <job_id> --start

# 检查详细的作业信息
cinfo -j <job_id>
```

### 步骤 4：监控 GPU 使用情况（作业运行时）

如果您可以交互式访问计算节点：

```bash
# 在作业运行的计算节点上
watch -n 1 nvidia-smi
```

---

## 用例 5：参数扫描的数组作业

**场景**：使用不同参数运行相同的分析（例如，超参数调优）。

### 任务描述

您想用 10 个不同的学习率测试模型，而不想提交 10 个单独的作业。

### 步骤 1：创建数组作业脚本

创建 `param_sweep.sh`：

```bash
#!/bin/bash
#CBATCH --job-name=param_sweep
#CBATCH --partition=CPU
#CBATCH --nodes=1
#CBATCH --cpus-per-task=2
#CBATCH --mem=4G
#CBATCH --time=0:30:00
#CBATCH --output=sweep_%j.out
#CBATCH --repeat=10

# 定义参数数组
learning_rates=(0.001 0.005 0.01 0.05 0.1 0.5 1.0 2.0 5.0 10.0)

# 获取此任务的参数
# 注意：作业数组实现可能有所不同
param_index=$(( ($CRANE_JOB_ID % 10) ))
learning_rate=${learning_rates[$param_index]}

echo "测试学习率=$learning_rate"

# 使用参数运行程序
python train.py --learning-rate $learning_rate --output results_lr_${learning_rate}.txt

echo "学习率=$learning_rate 的测试完成"
```

### 步骤 2：提交数组作业

```bash
cbatch --repeat 10 param_sweep.sh
```

这将提交 10 个作业，每个作业具有不同的数组索引。

### 步骤 3：监控所有数组作业

```bash
# 查看数组中的所有作业
cqueue --self -n param_sweep
```

---

## 用例 6：作业依赖链

**场景**：运行一个流水线，其中每个步骤都依赖于前一步骤成功完成。

### 任务描述

您有一个三阶段数据处理流水线：预处理 → 训练 → 评估。

### 步骤 1：提交第一个作业

```bash
# 预处理作业
job1=$(cbatch preprocess.sh | grep -oP '\d+')
echo "预处理作业: $job1"
```

### 步骤 2：提交依赖作业

```bash
# 训练作业（等待预处理）
# 注意：依赖实现可能因版本而异
job2=$(cbatch --dependency=afterok:$job1 train.sh | grep -oP '\d+')
echo "训练作业: $job2"

# 评估作业（等待训练）
job3=$(cbatch --dependency=afterok:$job2 evaluate.sh | grep -oP '\d+')
echo "评估作业: $job3"
```

### 步骤 3：监控流水线

```bash
# 查看所有三个作业
cqueue -j $job1,$job2,$job3
```

---

## 常用模式

### 检查作业历史

作业完成后，使用 `cacct` 查看作业统计信息：

```bash
# 查看您最近的作业
cacct --user $USER --starttime $(date -d "7 days ago" +%Y-%m-%d)

# 查看特定作业详情
cacct --job <job_id>
```

### 取消作业

```bash
# 取消特定作业
ccancel <job_id>

# 取消所有挂起的作业
ccancel --state=PENDING --user=$USER
```

### 资源效率

检查作业对分配资源的使用效率：

```bash
ceff <job_id>
```

---

## 提示和最佳实践

1. **从小开始**：首先用最小资源测试作业，然后再扩大规模
2. **设置准确的时间限制**：高估会浪费资源，低估会导致作业被终止
3. **使用作业名称**：便于在队列中识别作业
4. **监控资源使用**：作业完成后使用 `ceff` 优化未来的提交
5. **错误处理**：始终指定单独的 `--error` 文件以捕获 stderr 消息
6. **检查点**：对于长时间运行的作业，实现检查点以从故障中恢复

---

## 下一步

- 探索[命令参考](../command/cbatch.md)了解详细选项
- 了解[集群配置](../deployment/configuration/config.md)
- 调试作业失败时查看[退出代码](exit_code.md)
