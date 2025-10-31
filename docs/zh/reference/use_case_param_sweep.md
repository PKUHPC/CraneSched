# 用例：使用作业数组进行参数扫描

**场景**：使用不同参数多次运行相同的分析（例如超参数调优）。

作业数组可以避免重复提交大量单独的作业，让您轻松探索多种配置。

## 步骤 1：创建数组作业脚本

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

# 获取当前任务的参数
# 注意：作业数组的实现可能有所不同
param_index=$(( ($CRANE_JOB_ID % 10) ))
learning_rate=${learning_rates[$param_index]}

echo "Testing with learning_rate=$learning_rate"

# 使用当前参数运行程序
python train.py --learning-rate $learning_rate --output results_lr_${learning_rate}.txt

echo "Completed test for learning_rate=$learning_rate"
```

## 步骤 2：提交数组作业

```bash
cbatch --repeat 10 param_sweep.sh
```

每个数组索引都会获得对应的学习率配置。

## 步骤 3：监控所有数组作业

```bash
# 查看数组中的全部作业
cqueue --self -n param_sweep
```

## 提示

- 在 `--output` 和 `--error` 中包含 `%j`（作业 ID）可避免文件冲突。
- 将输出结果集中保存到共享目录，并写入本次使用的参数值，便于后续整理。
- 对于大规模扫描，可分批提交或设置依赖以控制并发度。
