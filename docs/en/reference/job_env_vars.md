# Job Environment Variables

CraneSched automatically sets a series of environment variables when executing jobs, which can be used by job scripts and programs. These environment variables contain various job information such as job ID, resource allocation, time limits, etc.

This page describes the environment prepared for tasks on compute nodes, the inputs read by native PMIx, and hook-specific variables. The local interactive shell opened by `calloc` inherits the submitting process's environment and adds `CRANE_JOB_ID`; it does not receive the full compute-node environment listed here.

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
- **Description**: Effective GID used to execute the job, after submission-time validation; it can differ from the user's primary group
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

### CRANE_PROCID / CRANE_PROC_ID
- **Description**: Aliases for the zero-based global task ID within the current execution unit
- **Type**: Integer
- **Example**: `2`
- **Available**: Set for native task processes and task epilogs. The container task initialization path does not additionally generate these two aliases.

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

### Additional Allocation and Submission Variables

| Variable | Meaning and format |
| --- | --- |
| `CRANE_CLUSTER_NAME` | Cluster name configured by the administrator |
| `CRANE_NODELIST` | Alias of `CRANE_JOB_NODELIST`, with semicolon-separated node names |
| `CRANE_NODEID` | Zero-based index of this node in the job allocation's node list |
| `CRANE_CPUS_PER_TASK` | Requested CPUs per task for the job allocation, formatted with two decimal places, such as `2.00` |
| `CRANE_CPUS_ON_NODE` | CPUs allocated to the job on this node, formatted with two decimal places |
| `CRANE_MEM_PER_CPU` | Memory allocated on this node divided by its allocated CPUs, in MiB per CPU with two decimal places and no suffix; `0.00` if the CPU count is zero |
| `CRANE_NTASKS` | Requested total task count for the job allocation |
| `CRANE_NTASKS_PER_NODE` | Requested tasks per node for the job allocation, distinct from the actual distribution in `CRANE_TASKS_PER_NODE` |
| `CRANE_GPUS` | GPU count allocated to the job on this node, including `0` |
| `CRANE_SUBMIT_DIR` | Directory from which the job was submitted |
| `CRANE_SUBMIT_HOST` | Host from which the job was submitted |

These values are initially generated from the job allocation. Ordinary steps inherit `CRANE_NTASKS`, `CRANE_NTASKS_PER_NODE`, `CRANE_CPUS_PER_TASK`, `CRANE_CPUS_ON_NODE`, and `CRANE_NODEID` without replacing them with step values. Use `CRANE_STEP_*`, `CRANE_GTIDS`, and task IDs for the current step. Native PMIx tasks have a node-list exception described below.

---

## Time-Related Environment Variables

### CRANE_TIMELIMIT
- **Description**: Time limit for the current execution unit in HH:MM:SS format; filled only if the merged environment does not already contain `CRANE_TIMELIMIT`
- **Type**: String
- **Example**: `02:30:00` represents 2 hours and 30 minutes

### CRANE_JOB_END_TIME
- **Description**: Environment initialization time plus the job time limit, expressed as Unix-epoch `system_clock` ticks (nanoseconds on current Linux builds). This is a snapshot, not a continuously updated deadline.
- **Type**: Integer
- **Example**: `1699876543000000000`

### CRANE_JOB_START_TIME
- **Description**: Unix timestamp when the job started, in seconds
- **Type**: Integer
- **Example**: `1699876543`

---

## GPU and Accelerator Environment Variables

When a job is allocated GPU or other accelerator devices, CraneSched sets the corresponding device visibility environment variables.

The variables are selected by the device's configured `EnvInjector` and generated from the job's allocation on the current node. Each list contains logical indices from `0` to the allocated device count minus one; it is not a list of physical device UUIDs. No variable is generated for an injector with no allocated devices. These job-level lists are inherited by steps; do not assume they provide a separate per-task GPU binding.

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
- **Note**: Can be propagated from the submitting environment. `--get-user-env` fills it from the execution user's account only when it is missing.

### SHELL
- **Description**: User's default shell
- **Type**: String
- **Example**: `/bin/bash`
- **Note**: Can be propagated from the submitting environment. `--get-user-env` fills it from the execution user's account only when it is missing.

### TERM
- **Description**: Terminal type
- **Type**: String
- **Example**: `xterm-256color`
- **Note**: Can be propagated from the submitting environment. For `crun`, nonempty terminal metadata supplies it when it is missing.

### DISPLAY / XAUTHORITY

For native `crun` tasks with X11 enabled, CraneSched sets `DISPLAY` to the configured X11 endpoint and `XAUTHORITY` to the generated authorization file, replacing any propagated values. Without X11, these names may still be present through ordinary environment propagation.

## Hook Environment

| Variable | Where CraneSched supplies it |
| --- | --- |
| `CRANE_SCRIPT_CONTEXT=epilog` | Compute-node job epilog environment |
| `CRANE_SCRIPT_CONTEXT=task_epilog` | Task epilog environment |
| `USER`, `LOGNAME` | Task epilogs receive the job user's name when account lookup succeeds |

Task epilogs receive the step environment plus `CRANE_PROCID`, `CRANE_PROC_ID`, and `CRANE_LOCALID`, and the corresponding Slurm IDs when compatibility is enabled. The environment is rebuilt by the supervisor; changes made by the user process or task prolog are not copied back into it. Job epilogs have a job-level environment and should not assume task IDs exist. CraneSched does not automatically add a `CRANE_SCRIPT_CONTEXT` marker to task prologs or ordinary user tasks.

Task prologs run with the prepared task environment and may change the subsequent task environment through `export name=value` and `unset name` output. See [Prolog/Epilog Guide](../deployment/configuration/prolog_epilog_guide.md).

## Native PMIx Environment

### User-Supplied Settings

These settings are read from the submitted step environment for `crun --mpi=pmix`. Export them before invoking `crun`, or include them in `--export`. They are inputs, not allocation information generated by CraneSched.

| Variable | Accepted value and default behavior |
| --- | --- |
| `CRANE_PMIX_TIMEOUT` | Timeout in seconds for PMIx operation/collective cleanup. Default: `5`. Use a positive whole number; empty values or conversion failures retain the default. |
| `CRANE_PMIX_FENCE` | `tree` or `ring`. If unset or unrecognized, the callback selects `ring` when collecting nonempty data, otherwise `tree`; there is no unconditional tree default. |
| `CRANE_PMIX_DIRECT_CONN_UCX` | The exact value `true` enables UCX for PMIx metadata exchange; other values use gRPC. Requires UCX support in the CraneSched build, otherwise launch fails. |
| `PMIXP_PMIXLIB_TMPDIR` | Optional user-accessible base directory for PMIx client files. CraneSched appends `pmix.crane.cli/spmix_appdir_<uid>.<job>.<step>`. Unset or empty uses PMIx's default directory. |

### Variables Added to PMIx Tasks

- The PMIx library supplies its client connection/rank environment via `PMIx_server_setup_fork`; the exact names depend on the installed PMIx version and are not a fixed CraneSched list.
- `OMPI_MCA_orte_precondition_transports` is supplied for Open MPI compatibility. If the PMIx-generated map lacks it, CraneSched derives a hexadecimal key from the job and step IDs.
- `CRANE_JOB_NODELIST` and `CRANE_NODELIST` are replaced with the **current step's** semicolon-separated node list, even though other `CRANE_JOB_*` values still describe the job allocation.
- With `EnableSlurmCompatibleEnv: false`, PMIx tasks still receive `SLURM_JOBID`, `SLURM_JOB_NODELIST`, `SLURM_NODELIST`, and `SLURM_STEP_ID`. Both node-list variables describe the **step** and use compressed host-list notation where possible.
- With `EnableSlurmCompatibleEnv: true`, the Slurm job node lists remain the full job allocation, while step node lists and rank variables describe the PMIx step, as in the tables below.

These PMIx additions replace same-name values in the prepared native task environment. Task epilogs use the separately rebuilt environment described above, rather than a copy of the PMIx child's environment. See the [PMIx Guide](../deployment/configuration/pmix.md) for deployment and launch examples.

---

## Slurm-Compatible Environment Variables

Set the top-level `EnableSlurmCompatibleEnv: true` in the cluster's `config.yaml` to generate the following additional variables; the default is `false`. See [Cluster Configuration](../deployment/configuration/config.md). These variables support scripts and launchers and do not imply support for every Slurm command option.

The tables describe automatic generation with the switch enabled. Disabling it does not remove user-propagated `SLURM_*` variables, and native PMIx still supplies the four-variable subset described above.

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
| `SLURM_GPUS_ON_NODE` | Positive job GPU count on this node, replaced by a positive step GPU count. A zero-GPU step does not clear an inherited value, so this variable alone is not proof of a step GPU allocation. |
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

1. **Propagation options**: `cbatch`, `calloc`, and `crun` use `--export` (default `ALL`). `NIL` adds no ordinary submitting variables; `NONE` also enables `--get-user-env`. A list accepts `NAME`, `NAME=value`, and `ALL`, and also enables `--get-user-env`. Existing `CRANE_*` variables are retained by this propagation logic. `CRANE_EXPORT_ENV` is an internal frontend carrier for the option, not an allocation variable to set in job scripts.

2. **Merge order**: Generated job values take precedence over same-name submission values; the step/task initialization code then assigns its context-specific fields. Native PMIx can replace values afterward, and task prolog output can export or unset them before execution. There is no universal rule that every `CRANE_*` name is immutable. For containers, `ccon -e/--env` supplies container-specific values but does not replace names already present in the backend's prepared environment.

3. **Login environment**: `--get-user-env` fills missing `HOME`/`SHELL` and runs native commands through `/bin/bash --login -c`. Login startup files may change the environment; `.bashrc` is not guaranteed to be sourced unless the user's startup files arrange it. The daemon's entire ambient environment is not inherited by the task.

4. **Frontend consumers**: `crun` uses `CRANE_JOB_ID` to recognize an existing allocation and reads `CRANE_NTASKS`, `CRANE_JOB_NUM_NODES`, `CRANE_NTASKS_PER_NODE`, and `CRANE_MEM_PER_NODE` as step defaults. It also reads `SLURM_EXTERNAL_LAUNCHER` independently of the backend compatibility switch. When its host-list argument is omitted, `ccontrol show hostnames` checks `SLURM_JOB_NODELIST`, `SLURM_NODELIST`, `CRANE_JOB_NODELIST`, then `CRANE_NODELIST`.

5. **Scope of this reference**: Build-time variables and dependency-specific settings are documented in the relevant deployment guides. `CRANE_CGROUP_OP_CONCURRENCY`, `CRANE_CGROUP_OP_SEM_NAME`, `CRANE_CGROUP_V2_FAST_PATH`, `CRANE_CGROUP_V2_CLEANUP_MODE`, and `CRANE_TRACE_HOOK_MAX_REQUEST_BYTES` carry internal daemon/supervisor settings; they are not generated job variables or recommended submission controls. The UCX server sets `UCX_MEM_MMAP_RELOC`, `UCX_MEM_MALLOC_HOOKS`, `UCX_MEM_MALLOC_RELOC`, and `UCX_MEM_EVENTS` to `no` in its own process, which does not guarantee their presence in user tasks.
