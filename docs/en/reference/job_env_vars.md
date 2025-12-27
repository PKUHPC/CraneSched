# Job Environment Variables

CraneSched automatically sets a series of environment variables when executing jobs, which can be used by job scripts and programs. These environment variables contain various job information such as job ID, resource allocation, time limits, etc.

---

## Job Information Environment Variables

### CRANE_JOB_ID
- **Description**: Unique identifier for the job
- **Type**: Integer
- **Example**: `12345`

### CRANE_JOB_NAME
- **Description**: Job name
- **Type**: String
- **Example**: `my_simulation`

### CRANE_JOB_ACCOUNT
- **Description**: Account used to submit the job
- **Type**: String
- **Example**: `research_group`

### CRANE_JOB_PARTITION
- **Description**: Partition name where the job runs
- **Type**: String
- **Example**: `compute`

### CRANE_JOB_QOS
- **Description**: Quality of Service level for the job
- **Type**: String
- **Example**: `normal`

---

## Step Environment Variables

When executing within a job step, CraneSched sets additional step-specific environment variables. These variables help scripts and programs distinguish between running as a standalone job versus running as a step within a job.

### CRANE_STEP_ID
- **Description**: Step ID within the parent job (numeric part only, without the job ID)
- **Type**: Integer
- **Example**: `1` (for step 123.1, this value would be `1`)
- **Available**: Only set in step execution context
- **Note**: If this variable is not set, the process is running as a standalone job, not as a step

---

## Node and Resource Environment Variables

### CRANE_JOB_NODELIST
- **Description**: List of nodes allocated to the job, node names separated by semicolons
- **Type**: String
- **Example**: `node01;node02;node03`

### CRANE_JOB_NUM_NODES
- **Description**: Number of nodes allocated to the job
- **Type**: Integer
- **Example**: `3`

### CRANE_MEM_PER_NODE
- **Description**: Amount of memory allocated per node in MB
- **Type**: Integer
- **Example**: `4096`

---

## Time-Related Environment Variables

### CRANE_TIMELIMIT
- **Description**: Time limit for the job in HH:MM:SS format
- **Type**: String
- **Example**: `02:30:00` represents 2 hours and 30 minutes

### CRANE_JOB_END_TIME
- **Description**: Timestamp of the expected job end time (nanoseconds since Unix epoch)
- **Type**: Integer
- **Example**: `1699876543000000000`

---

## GPU and Accelerator Environment Variables

When a job is allocated GPU or other accelerator devices, CraneSched sets the corresponding device visibility environment variables.

### CUDA_VISIBLE_DEVICES
- **Description**: List of visible NVIDIA GPU devices for CUDA applications
- **Type**: Comma-separated list of integers
- **Example**: `0,1,2`
- **Applies to**: NVIDIA GPUs

### NVIDIA_VISIBLE_DEVICES
- **Description**: List of visible NVIDIA GPU devices for container runtimes
- **Type**: Comma-separated list of integers
- **Example**: `0,1,2`
- **Applies to**: NVIDIA GPUs

### HIP_VISIBLE_DEVICES
- **Description**: List of visible AMD GPU devices for HIP/ROCm applications
- **Type**: Comma-separated list of integers
- **Example**: `0,1`
- **Applies to**: AMD GPUs

### ASCEND_RT_VISIBLE_DEVICES
- **Description**: List of visible Huawei Ascend NPU devices
- **Type**: Comma-separated list of integers
- **Example**: `0,1,2,3`
- **Applies to**: Huawei Ascend NPUs

### ASCEND_VISIBLE_DEVICES
- **Description**: List of visible Huawei Ascend NPU devices (alias)
- **Type**: Comma-separated list of integers
- **Example**: `0,1,2,3`
- **Applies to**: Huawei Ascend NPUs

---

## Other Environment Variables

### HOME
- **Description**: User's home directory path
- **Type**: String
- **Example**: `/home/username`
- **Note**: Only set when using the `--get-user-env` option

### SHELL
- **Description**: User's default shell
- **Type**: String
- **Example**: `/bin/bash`
- **Note**: Only set when using the `--get-user-env` option

### TERM
- **Description**: Terminal type
- **Type**: String
- **Example**: `xterm-256color`
- **Note**: Only set in interactive jobs

---

## Usage Examples

### Using in Batch Scripts

```bash
#!/bin/bash
#SBATCH --job-name=test_job
#SBATCH --nodes=2
#SBATCH --time=01:00:00

echo "Job ID: $CRANE_JOB_ID"
echo "Job Name: $CRANE_JOB_NAME"
echo "Node List: $CRANE_JOB_NODELIST"
echo "Number of Nodes: $CRANE_JOB_NUM_NODES"
echo "Time Limit: $CRANE_TIMELIMIT"
echo "Memory per Node: ${CRANE_MEM_PER_NODE}MB"

# If GPUs are allocated
if [ ! -z "$CUDA_VISIBLE_DEVICES" ]; then
    echo "Available GPUs: $CUDA_VISIBLE_DEVICES"
fi

# Run program
./my_program
```

### Using in Python Programs

```python
import os

# Get job information
job_id = os.environ.get('CRANE_JOB_ID', 'unknown')
job_name = os.environ.get('CRANE_JOB_NAME', 'unknown')
num_nodes = int(os.environ.get('CRANE_JOB_NUM_NODES', '1'))

print(f"Job ID: {job_id}")
print(f"Job Name: {job_name}")
print(f"Number of Nodes: {num_nodes}")

# Check GPU availability
cuda_devices = os.environ.get('CUDA_VISIBLE_DEVICES')
if cuda_devices:
    print(f"Available GPUs: {cuda_devices}")
    gpu_count = len(cuda_devices.split(','))
    print(f"GPU Count: {gpu_count}")
```

---

## Important Notes

1. **Environment Variable Priority**: User-specified environment variables via `-E` or `--env` options will override defaults, but CRANE system environment variables have the highest priority and will override user-set variables with the same name.

2. **Node List Format**: `CRANE_JOB_NODELIST` uses semicolons (`;`) to separate node names, which may differ from formats used by other scheduling systems.

3. **Device Environment Variables**: GPU and accelerator-related environment variables are only set when the job is actually allocated corresponding devices.

4. **Timestamp Format**: `CRANE_JOB_END_TIME` uses nanosecond-level timestamps, which may require unit conversion when used.

5. **`--get-user-env` Option**: When this option is used, the job environment is initialized from the user environment on the execution node, including loading configuration files like `.bashrc`, `/etc/profile`, etc.
