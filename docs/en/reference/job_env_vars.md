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

### CRANE_JOB_UID
- **Description**: UID of the job user
- **Type**: Integer
- **Example**: `1000`

### CRANE_JOB_GID
- **Description**: Primary GID of the job user
- **Type**: Integer
- **Example**: `1000`

---

## Array Job Environment Variables

These variables are set only for expanded array tasks.

### CRANE_ARRAY_JOB_ID
- **Description**: Parent/anchor job ID of the whole array
- **Type**: Integer
- **Example**: `1000`

### CRANE_ARRAY_TASK_ID
- **Description**: Current array task index
- **Type**: Integer
- **Example**: `7`

---

## Step Environment Variables

CraneSched sets task counts and identifiers for execution units. `CRANE_STEP_ID`, `CRANE_STEP_NODELIST`, and `CRANE_STEP_NUM_NODES` apply to execution contexts other than DAEMON and batch-like PRIMARY steps, including directly submitted `crun` jobs and `crun` steps inside allocations. Batch scripts and calloc allocation PRIMARY steps do not set these three variables, although they are still implemented as steps internally.

### CRANE_STEP_ID
- **Description**: Step ID within the parent job (numeric part only, without the job ID)
- **Type**: Integer
- **Example**: `1` (for step 123.1, this value would be `1`)
- **Available**: Only set in step execution context
- **Note**: This variable alone does not determine whether a parent allocation exists; `crun` uses `CRANE_JOB_ID` to decide whether to create a step in an existing allocation.

### CRANE_STEP_NODELIST
- **Description**: List of nodes allocated to the current step, separated by semicolons
- **Type**: String
- **Example**: `node01;node02`
- **Available**: Only set in step execution context

### CRANE_STEP_NUM_NODES
- **Description**: Number of nodes allocated to the current step
- **Type**: Integer
- **Example**: `2`
- **Available**: Only set in step execution context

### CRANE_STEP_NUM_TASKS
- **Description**: Total number of tasks in the current execution unit
- **Type**: Integer
- **Example**: `4`

### CRANE_GTIDS
- **Description**: Comma-separated global task IDs assigned to the current node
- **Type**: Integer list
- **Example**: `0,2`

### CRANE_LOCALID
- **Description**: Zero-based local index of the current task on its node
- **Type**: Integer
- **Example**: `1`
- **Available**: Only set in task processes and their task epilogs

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

### CRANE_JOB_CPUS_PER_NODE
- **Description**: Number of CPUs allocated to the job on the current node
- **Type**: Number
- **Example**: `4`

### CRANE_TASKS_PER_NODE
- **Description**: Task counts ordered by the node list. It describes the job distribution in a job context and the current step distribution in a step context
- **Type**: Comma-separated integer list
- **Example**: `2,1,1`

### CRANED_NODENAME
- **Description**: Craned node name of the current execution node
- **Type**: String
- **Example**: `node01`

### CRANE_WORKING_DIR
- **Description**: Requested working directory of the current job or step
- **Type**: String
- **Example**: `/home/user/project`

### CRANE_MEM_PER_NODE
- **Description**: Memory allocated to the job on the current node, in units of 1024² bytes with an `M` suffix
- **Type**: String
- **Example**: `4096M`

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

### CRANE_JOB_START_TIME
- **Description**: Unix timestamp when the job started, in seconds
- **Type**: Integer
- **Example**: `1699876543`

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

## Slurm-Compatible Environment Variables

Set the top-level `EnableSlurmCompatibleEnv: true` in the cluster's `config.yaml` to generate the following additional variables; the default is `false`. See [Cluster Configuration](../deployment/configuration/config.md). These variables support scripts and launchers and do not imply support for every Slurm command option.

### Job Information

| Variable | Current meaning |
| --- | --- |
| `SLURM_JOB_ID`, `SLURM_JOBID` | Job ID |
| `SLURM_JOB_NAME`, `SLURM_JOB_ACCOUNT`, `SLURM_JOB_PARTITION`, `SLURM_JOB_QOS` | Job name, account, partition, and QoS |
| `SLURM_JOB_UID`, `SLURM_JOB_GID` | Job user's UID and effective GID |
| `SLURM_CLUSTER_NAME`, `SLURMD_NODENAME` | Cluster name and current Craned node name |
| `SLURM_JOB_NODELIST`, `SLURM_NODELIST` | Full job allocation's node names, comma-separated, without compression such as `node[01-03]` |
| `SLURM_JOB_NUM_NODES`, `SLURM_JOB_NODES` | Number of nodes in the job allocation |
| `SLURM_JOB_CPUS_PER_NODE` | CPUs allocated to the job on this node: a single value, not a compressed list for all nodes |
| `SLURM_SUBMIT_DIR`, `SLURM_SUBMIT_HOST` | Submission directory and host |
| `SLURM_WORKING_DIR` | Current execution unit's working directory, which can differ from the submission directory |
| `SLURM_JOB_START_TIME` | Job start time as a Unix timestamp in seconds |
| `SLURM_ARRAY_JOB_ID`, `SLURM_ARRAY_TASK_ID` | Parent array job ID and task index, set only for expanded array tasks |

### Step and Task Information

| Variable | Current meaning |
| --- | --- |
| `SLURM_NNODES` | Job node count in the job context; current step's node count inside a `crun` step |
| `SLURM_NTASKS`, `SLURM_NPROCS` | Job task count in the job context; number of tasks actually launched in a `crun` step |
| `SLURM_NTASKS_PER_NODE` | Requested tasks per node for the current job or step |
| `SLURM_TASKS_PER_NODE` | Actual task counts in node-list order, such as `2,1,1`, without Slurm compression such as `2(x3)` |
| `SLURM_STEP_ID`, `SLURM_STEPID` | Numeric step ID, with the same context rules as `CRANE_STEP_ID` |
| `SLURM_STEP_NODELIST`, `SLURM_STEP_NUM_NODES` | Current step's comma-separated node list and node count, with the same context rules as `CRANE_STEP_NODELIST` |
| `SLURM_STEP_NUM_TASKS` | Tasks actually launched for the current execution unit; for a batch PRIMARY this counts script tasks, not the MPI ranks the full allocation can accommodate |
| `SLURM_STEP_TASKS_PER_NODE` | Actual task counts in node-list order inside a `crun` step |
| `SLURM_NODEID`, `SLURM_STEP_NODEID` | Zero-based node index, relative to the step's node list inside a `crun` step. `SLURM_STEP_NODEID` applies only to step contexts |
| `SLURM_PROCID`, `SLURM_LOCALID` | Zero-based global and node-local task IDs in task processes and task epilogs |
| `SLURM_STEP_PROCID`, `SLURM_STEP_LOCALID` | Corresponding task IDs in `crun` step processes and task epilogs; PMIx tasks use their ranks |
| `SLURM_GTIDS` | Comma-separated global task IDs hosted on this node |
| `SLURM_CPUS_ON_NODE`, `SLURM_CPUS_PER_TASK` | Current execution unit's CPU allocation on this node, and requested CPUs per task |
| `SLURM_MEM_PER_NODE` | Current execution unit's memory on this node, as an integer in units of 1024² bytes without an `M` suffix |
| `SLURM_GPUS_ON_NODE` | Current execution unit's GPU count on this node, generated only when greater than zero |
| `SLURM_CPU_BIND_TYPE` | Currently `none`; this does not imply support for Slurm's `--cpu-bind` option |
| `SLURM_EXTERNAL_LAUNCHER` | Set to `1` for external launcher steps; `crun` also reads it to enable the mode. See [crun](../command/crun.md) |

In a batch script, `SLURM_NTASKS` describes the full job allocation while the script's `SLURM_STEP_NUM_TASKS` can be `1`. Inside a `crun` step, `SLURM_NTASKS`, `SLURM_NNODES`, and task distribution describe that step; `SLURM_JOB_*` still describe the parent job. Task counts in external launcher steps count launcher processes; the launcher determines the actual MPI rank count.

## Usage Examples

### Using in Batch Scripts

```bash
#!/bin/bash
#SBATCH --job-name=test_job
#SBATCH --nodes=2
#SBATCH --time=01:00:00

echo "Job ID: $CRANE_JOB_ID"
echo "Array Job ID: ${CRANE_ARRAY_JOB_ID:-N/A}"
echo "Array Task ID: ${CRANE_ARRAY_TASK_ID:-N/A}"
echo "Job Name: $CRANE_JOB_NAME"
echo "Node List: $CRANE_JOB_NODELIST"
echo "Number of Nodes: $CRANE_JOB_NUM_NODES"
echo "Time Limit: $CRANE_TIMELIMIT"
echo "Memory per Node: ${CRANE_MEM_PER_NODE}"

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
