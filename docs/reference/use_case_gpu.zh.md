# 用例：GPU 加速训练

**场景**：使用 GPU 资源训练机器学习模型。

此流程演示如何申请 GPU 资源、配置软件环境并监控训练作业。

## 步骤 1：准备 GPU 作业脚本

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

# 加载所需模块
module load cuda/11.8
module load python/3.9

# 如果使用虚拟环境则激活
source ~/venv/bin/activate

# 显示 GPU 信息
echo "Allocated GPU:"
nvidia-smi

# 设置环境
export CUDA_VISIBLE_DEVICES=0

# 运行训练脚本
echo "Training started at $(date)"
python train_model.py \
    --data-path /path/to/dataset \
    --batch-size 32 \
    --epochs 100 \
    --output-dir ./results

echo "Training completed at $(date)"
```

## 步骤 2：提交 GPU 作业

```bash
cbatch gpu_training.sh
```

## 步骤 3：检查作业状态

```bash
# 查看作业是否正在运行以及分配的 GPU
cqueue -j <job_id> --start

# 查看详细作业信息
cinfo -j <job_id>
```

## 步骤 4：监控 GPU 使用率（作业运行时）

如果可以交互式登录到作业所在的计算节点：

```bash
# 在作业运行的计算节点上
watch -n 1 nvidia-smi
```

## 提示

- 将标准输出与标准错误分开记录，便于排查长时间训练作业的问题。
- 如果集群提供节点本地高速存储，可将临时数据写入该目录以提升 I/O 性能。
- 考虑开启混合精度（如 `torch.cuda.amp`），以在相同显存中容纳更大的模型。
