# Use Case: GPU-Accelerated Training

**Scenario**: Train a machine learning model using GPU resources.

This workflow demonstrates how to request GPU resources, set up the software environment, and monitor a training job.

## Step 1: Prepare the GPU Job Script

Create `gpu_training.sh`:

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

# Load necessary modules
module load cuda/11.8
module load python/3.9

# Activate virtual environment (if using)
source ~/venv/bin/activate

# Display GPU information
echo "Allocated GPU:"
nvidia-smi

# Set up environment
export CUDA_VISIBLE_DEVICES=0

# Run training script
echo "Starting training at $(date)"
python train_model.py \
    --data-path /path/to/dataset \
    --batch-size 32 \
    --epochs 100 \
    --output-dir ./results

echo "Training completed at $(date)"
```

## Step 2: Submit the GPU Job

```bash
cbatch gpu_training.sh
```

## Step 3: Check Job Status

```bash
# Check if job is running and which GPU is allocated
cqueue -j <job_id> --start

# Check detailed job information
cinfo -j <job_id>
```

## Step 4: Monitor GPU Usage (While Running)

If you have interactive access to the compute node:

```bash
# On the compute node where job is running
watch -n 1 nvidia-smi
```

## Tips

- Keep stdout and stderr separated for easier troubleshooting on long training runs.
- If the cluster provides node-local scratch, direct temporary data there for better I/O throughput.
- Consider enabling mixed precision (e.g., `torch.cuda.amp`) to fit larger models in memory.
