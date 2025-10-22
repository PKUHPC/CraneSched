# Use Cases

This guide provides practical examples of common workflows using CraneSched. Each use case includes a complete walkthrough from job submission to result retrieval.

---

## Use Case 1: Basic Batch Job

**Scenario**: Run a simple computational task on a single node.

### Task Description

You have a script that processes data and outputs results to a file. This is the most common use case for batch processing.

### Step 1: Prepare Your Script

Create a file named `simple_job.sh`:

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

# Print job information
echo "Job started at: $(date)"
echo "Running on node: $(hostname)"
echo "Job ID: $CRANE_JOB_ID"

# Simulate some computational work
echo "Processing data..."
sleep 5
echo "Data processing complete!"

# Print job completion
echo "Job finished at: $(date)"
```

### Step 2: Submit the Job

```bash
cbatch simple_job.sh
```

Expected output:
```
Job <12345> is submitted to partition <CPU>
```

### Step 3: Check Job Status

```bash
cqueue -j 12345
```

or check all your jobs:

```bash
cqueue --self
```

### Step 4: View Results

After the job completes, check the output file:

```bash
cat job_12345.out
```

---

## Use Case 2: Interactive Session

**Scenario**: You need to interactively test code or debug an application on a compute node.

### Task Description

Instead of submitting a batch job, you want direct command-line access to a compute node with specific resources.

### Step 1: Request an Interactive Session

```bash
crun -p CPU -N 1 -c 2 --mem 500M -t 1:00:00 /bin/bash
```

This allocates:
- 1 node from the CPU partition
- 2 CPU cores
- 500 MB memory
- 1 hour time limit

### Step 2: Work Interactively

Once connected, you'll see a shell prompt on the compute node:

```bash
# Check where you are
hostname

# Check allocated resources
echo $CRANE_JOB_NODELIST

# Run your commands
python test_script.py
./my_program --input data.txt

# When done, exit
exit
```

The resources will be released automatically when you exit.

---

## Use Case 3: Multi-Node Parallel Job

**Scenario**: Run an MPI application across multiple nodes for parallel computation.

### Task Description

You have an MPI program that needs to run across multiple compute nodes to process data in parallel.

### Step 1: Prepare the MPI Job Script

Create `mpi_job.sh`:

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

# Load MPI module (adjust according to your environment)
module load mpich/4.0

# Generate machine file from allocated nodes
echo "$CRANE_JOB_NODELIST" | tr ";" "\n" > machinefile

# Display allocated nodes
echo "Running on nodes:"
cat machinefile

# Run MPI application
# This will use 12 processes total (3 nodes × 4 tasks per node)
mpirun -n 12 -machinefile machinefile ./my_mpi_program

echo "MPI job completed at $(date)"
```

### Step 2: Compile Your MPI Program (if needed)

```bash
mpicc -o my_mpi_program my_mpi_program.c
```

### Step 3: Submit the Job

```bash
cbatch mpi_job.sh
```

### Step 4: Monitor Progress

```bash
# Check job status
cqueue -j <job_id>

# View real-time output (while job is running)
tail -f mpi_job_<job_id>.out
```

---

## Use Case 4: GPU-Accelerated Job

**Scenario**: Train a machine learning model using GPU resources.

### Task Description

You want to run a deep learning training script that requires GPU acceleration.

### Step 1: Prepare GPU Job Script

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

### Step 2: Submit the GPU Job

```bash
cbatch gpu_training.sh
```

### Step 3: Check GPU Job Status

```bash
# Check if job is running and which GPU is allocated
cqueue -j <job_id> --start

# Check detailed job information
cinfo -j <job_id>
```

### Step 4: Monitor GPU Usage (while job runs)

If you have interactive access to the compute node:

```bash
# On the compute node where job is running
watch -n 1 nvidia-smi
```

---

## Use Case 5: Array Jobs for Parameter Sweep

**Scenario**: Run the same analysis with different parameters (e.g., hyperparameter tuning).

### Task Description

You want to test your model with 10 different learning rates without submitting 10 separate jobs.

### Step 1: Create Array Job Script

Create `param_sweep.sh`:

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

# Define parameter array
learning_rates=(0.001 0.005 0.01 0.05 0.1 0.5 1.0 2.0 5.0 10.0)

# Get parameter for this task
# Note: Job array implementation may vary
param_index=$(( ($CRANE_JOB_ID % 10) ))
learning_rate=${learning_rates[$param_index]}

echo "Testing with learning_rate=$learning_rate"

# Run your program with the parameter
python train.py --learning-rate $learning_rate --output results_lr_${learning_rate}.txt

echo "Completed test for learning_rate=$learning_rate"
```

### Step 2: Submit Array Job

```bash
cbatch --repeat 10 param_sweep.sh
```

This submits 10 jobs, each with a different array index.

### Step 3: Monitor All Array Jobs

```bash
# View all jobs in the array
cqueue --self -n param_sweep
```

---

## Use Case 6: Job Dependency Chain

**Scenario**: Run a pipeline where each step depends on the previous step completing successfully.

### Task Description

You have a three-stage data processing pipeline: preprocessing → training → evaluation.

### Step 1: Submit First Job

```bash
# Preprocessing job
job1=$(cbatch preprocess.sh | grep -oP '\d+')
echo "Preprocessing job: $job1"
```

### Step 2: Submit Dependent Jobs

```bash
# Training job (waits for preprocessing)
# Note: Dependency implementation may vary by version
job2=$(cbatch --dependency=afterok:$job1 train.sh | grep -oP '\d+')
echo "Training job: $job2"

# Evaluation job (waits for training)
job3=$(cbatch --dependency=afterok:$job2 evaluate.sh | grep -oP '\d+')
echo "Evaluation job: $job3"
```

### Step 3: Monitor the Pipeline

```bash
# Watch all three jobs
cqueue -j $job1,$job2,$job3
```

---

## Common Patterns

### Check Job History

After jobs complete, use `cacct` to view job statistics:

```bash
# View your recent jobs
cacct --user $USER --starttime $(date -d "7 days ago" +%Y-%m-%d)

# View specific job details
cacct --job <job_id>
```

### Cancel Jobs

```bash
# Cancel a specific job
ccancel <job_id>

# Cancel all your pending jobs
ccancel --state=PENDING --user=$USER
```

### Resource Efficiency

Check how efficiently your job used allocated resources:

```bash
ceff <job_id>
```

---

## Tips and Best Practices

1. **Start Small**: Test your job with minimal resources first, then scale up
2. **Set Accurate Time Limits**: Overestimating wastes resources, underestimating kills jobs
3. **Use Job Names**: Makes it easier to identify jobs in the queue
4. **Monitor Resource Usage**: Use `ceff` after job completion to optimize future submissions
5. **Error Handling**: Always specify separate `--error` file to catch stderr messages
6. **Checkpointing**: For long jobs, implement checkpointing to resume from failures

---

## Next Steps

- Explore [command reference](../command/cbatch.md) for detailed options
- Learn about [cluster configuration](../deployment/configuration/config.md)
- Check [exit codes](exit_code.md) when debugging job failures
