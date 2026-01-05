# crun - Submit Interactive Task

crun allocates resources based on command-line parameters and starts the specified task on compute nodes. User input is
forwarded to the corresponding task on compute nodes, and task output is forwarded back to the user terminal. crun must
be started on nodes where cfored is running.

crun can run in two modes:

- **Job mode**: Creates a new job allocation when executed outside an existing job
- **Step mode**: Creates a step within an existing job when executed inside a job allocation (e.g., within calloc)

The system automatically detects the mode by checking for the `CRANE_JOB_ID` environment variable.

## Options

**-h, --help**
:   **Applies to:** `job`, `step`  
Display help information for crun command.

**-v, --version**

:   **Applies to:** `job`, `step`  
Display crun version information.

**-C, --config=&lt;path&gt;**

:   **Applies to:** `job`, `step`  
Path to configuration file. Default: "/etc/crane/config.yaml".

**-d, --dependency=&lt;string&gt;**

:   **Applies to:** `job`  
Job dependency. Format: `<type>:<job_id>[+<delay>][:<job_id>][,<type>:<job_id>[:<job_id>]]` or `<type>:<job_id>[:<job_id>][?<type>:<job_id>[:<job_id>]]`. Supported types: `after`, `afterok`, `afternotok`, `afterany`. **Note**: For `<delay>`, use time with units (e.g., `10s`, `5m`, `2h`) - do NOT use `HH:MM:SS` format as `:` is the job ID separator. See [Job Dependency](../reference/job_dependency.md) for details.

**--debug-level=&lt;level&gt;**

:   **Applies to:** `job`, `step`  
Set debug output level. Available levels: trace, debug, info. Default: "info".

**-N, --nodes=&lt;num&gt;**

:   **Applies to:** `job`, `step`  
Number of nodes on which to run. Default: 1.

**-c, --cpus-per-task=&lt;ncpus&gt;**

:  **Applies to:** `job`, `step`  
Number of CPUs required per task. This may be useful if the task is multithreaded and requires more than one CPU per
task for optimal performance. Default: 1.

**--ntasks-per-node=&lt;ntasks&gt;**

:   **Applies to:** `job`, `step`  
Request that ntasks be invoked on each node. If used with the --nodes option, the --ntasks option will be treated as the
maximum count of tasks for the job. This means there will be at least ntasks-per-node * nodes tasks invoked, or ntasks
tasks invoked, whichever is smaller. Default: 1.

**--mem=&lt;size&gt;**

:   **Applies to:** `job`, `step`  
Maximum amount of real memory required per node. Supports different units: GB(G, g), MB(M, m), KB(K, k), and Bytes(B).
Default unit is MB.

**-t, --time=&lt;time&gt;**

:   **Applies to:** `job`, `step`  
Set a limit on the total run time of the job allocation. Time format is "[day-]hours:minutes:seconds". For example, "
5-0:0:1" for 5 days and 1 second, or "10:1:2" for 10 hours, 1 minute, and 2 seconds.

**--gres=&lt;list&gt;**

:   **Applies to:** `job`, `step`  
Specify generic resources required per task. Format: "gpu:type:number" (e.g., "gpu:a100:1") or "gpu:number" (e.g., "gpu:
1").

**-w, --nodelist=&lt;host1,host2,...&gt;**

:   **Applies to:** `job`, `step`  
Request a specific list of nodes. The list may be specified as a comma-separated list of node names.

**-x, --exclude=&lt;host1,host2,...&gt;**

:   **Applies to:** `job`, `step`  
Explicitly exclude certain nodes from the resources granted to the job. The list may be specified as a comma-separated
list of node names.

**-p, --partition=&lt;partition_name&gt;**

:   **Applies to:** `job`  
Request a specific partition for the resource allocation. If not specified, the default partition will be used.

**-A, --account=&lt;account&gt;**

:   **Applies to:** `job`  
Charge resources used by this job to specified account. The account is an arbitrary string.

**-q, --qos=&lt;qos&gt;**

:   **Applies to:** `job`  
Request a specific quality of service for the job. QoS values are defined by the system administrator.

**--exclusive**

:   **Applies to:** `job`  
The job allocation cannot share nodes with other running jobs.

**-H, --hold**

:   **Applies to:** `job`  
Submit the job in a held state. A held job will not be scheduled for execution until it is explicitly released by the
user or administrator using the ccontrol release command.

**-r, --reservation=&lt;reservation_name&gt;**

:   **Applies to:** `job`  
Allocate resources for the job from the named reservation.

**-J, --job-name=&lt;jobname&gt;**

:   **Applies to:** `job`, `step`  
Specify a name for the job allocation. The specified name will appear along with the job id in the output of cqueue.

**-L, --licenses=&lt;license&gt;**

:   **Applies to:** `job`, `step`  
Specification of licenses required for the job. Format: "license1:count1,license2:count2" or "license1:count1|license2:
count2". For example: "matlab:2,ansys:1".

**--wckey=&lt;wckey&gt;**

:   **Applies to:** `job`, `step`  
Specify a workload characterization key for the job. This is an arbitrary string that can be used for tracking groups of
jobs.

**--comment=&lt;string&gt;**

:   **Applies to:** `job`, `step`  
An arbitrary comment string for the job.

**-D, --chdir=&lt;directory&gt;**

:   **Applies to:** `job`, `step`  
Set the working directory of the task to directory before execution begins. The path can be absolute or relative.

**--export=&lt;environment&gt;**

:   **Applies to:** `job`, `step`  
Specify environment variables to be exported to the task environment. Options include "ALL" (export all environment
variables), "NONE" (export no environment variables), or a comma-separated list of variable names.

**--get-user-env**

:   **Applies to:** `job`, `step`  
Load the user's login environment variables into the task environment.

**--extra-attr=&lt;json&gt;**

:   **Applies to:** `job`, `step`  
Specify extra attributes for the job in JSON format.

**-i, --input=&lt;mode&gt;**

:   **Applies to:** `job`, `step`  
Specify how standard input is to be redirected. Options are "all" (redirect stdin to all tasks) or "none" (do not
redirect stdin). Default: "all".

**--pty**

:   **Applies to:** `job`, `step`  
Execute task in pseudo terminal mode. This allows for proper handling of interactive programs.

**--x11**

:   **Applies to:** `job`, `step`  
Enable X11 support for the job. If not used with --x11-forwarding, direct X11 forwarding is used (insecure). Default is
false.

**--x11-forwarding**

:   **Applies to:** `job`, `step`  
Enable secure X11 forwarding through CraneSched. Default is false.

**--mail-type=&lt;type&gt;**

:   **Applies to:** `job`, `step`  
Notify user by email when certain event types occur. Valid type values are NONE, BEGIN, END, FAIL, TIMELIMIT, and ALL (
equivalent to BEGIN, END, FAIL, and TIMELIMIT). Multiple type values may be specified in a comma-separated list.
Default: NONE.

**--mail-user=&lt;email&gt;**
:   **Applies to:** `job`, `step`    
Email address to receive job status notifications if --mail-type is specified.

## Job Mode vs Step Mode

### Job Mode

When `crun` is executed **outside** an existing job allocation (no `CRANE_JOB_ID` in environment), it creates a new job:

- All options are available
- Creates a new resource allocation
- Requires partition, account, and QoS specifications (or defaults)

### Step Mode

When `crun` is executed **within** an existing job allocation (e.g., inside calloc), it automatically runs as a step:

**Automatic Detection:**

- If `CRANE_JOB_ID` is set → runs as a step within that job

- If `CRANE_JOB_ID` is not set → runs as a new standalone job

**Resource Behavior:**

- Steps use resources from the parent job's allocation
- Can specify subset of nodes, different CPU counts, etc.
- Resources must be available within the parent job's allocation

**Inherited Properties:**

Steps automatically inherit from parent job:

- Partition (`-p/--partition`)
- Account (`-A/--account`)
- QoS (`-q/--qos`)
- User/Group

Options marked as "Applies to: `job`" cannot be overridden in step mode and will be ignored if specified.

## Usage Examples

### Job Mode Examples

**Allocate resources and run bash:**

Request 2 nodes, 1 CPU core, 200M memory in CPU partition, and run bash:

```bash
$ crun -c 1 --mem 200M -p CPU -N 2 /usr/bin/bash
pwd
/work
hostname
crane01
crane02
exit
```

**Exclude specific nodes:**

Request 1 node, exclude crane01 and crane02, set job name to testjob, time limit 0:25:25, and run bash:

```bash
$ crun -N 1 -x crane01,crane02 -J testjob -t 0:25:25 /usr/bin/bash
pwd
/work
hostname
crane03
exit
```

**Specify node list:**

Request 1 node and 200M memory in GPU partition, nodes must be chosen from crane02 or crane03, and run bash:

```bash
$ crun -p GPU --mem 200M -w crane02,crane03 /usr/bin/bash
pwd 
/work
hostname
crane02
exit
```

**With account, QoS, and environment settings:**

```bash
$ crun -A ROOT -J test_crun -x crane03 --get-user-env --ntasks-per-node 2 -q test_qos -t 00:20:00 /usr/bin/bash
Task id allocated: 188, waiting resources.
Allocated craned nodes: crane02
Task io forward ready, waiting input.
pwd
/work
hostname
crane02
```

**With working directory and debug level:**

```bash
$ crun -D /path --debug-level trace --export ALL /usr/bin/bash
Oct 12 16:08:28.856 [TRAC] Sending Task Req to Cfored
Oct 12 16:08:28.858 [TRAC] Waiting TaskId
Task id allocated: 1, waiting resources.
Oct 12 16:08:29.172 [TRAC] Waiting Res Alloc
Allocated craned nodes: crane02
Task io forward ready, waiting input.
pwd
/path
hostname
crane02
exit
```

**Run on specific node:**

```bash
$ crun -w crane04 /usr/bin/bash
Task id allocated: 1, waiting resources.
Allocated craned nodes: crane04
Task io forward ready, waiting input.
hostname
crane04
```

**X11 forwarding:**

```bash
# Run X11 applications
crun --x11 xclock
```

**Exclusive mode:**

Request exclusive access to allocated nodes, preventing other jobs from sharing:

```bash
crun --exclusive -N 2 /usr/bin/bash
```

**Hold mode:**

Submit the job in held state, preventing it from starting until manually released:

```bash
crun --hold -c 4 /usr/bin/bash
# Release later with: ccontrol release <job_id>
```

**Reservation:**

Use pre-reserved resources for the job:

```bash
crun -r my_reservation /usr/bin/bash
```

**Email notifications:**

Receive email notifications for job events:

```bash
crun --mail-type=END --mail-user=user@example.com -c 4 /usr/bin/bash
```

**Job comments:**

Add descriptive comments to your jobs:

```bash
crun --comment "Testing new algorithm" -c 8 /usr/bin/python script.py
```

### Step Mode Examples

**Nested execution within calloc:**

crun can be started nested within a calloc task and will automatically inherit all resources from the calloc task. No
need to specify partition, account, or QoS:

```bash
$ calloc -N=2 -c=1 --mem 500M --ntasks-per-node 1
Task id allocated: 1
Allocated craned nodes: crane[02-03].
$ crun echo $CRANE_PARTITION
CPU
CPU
$ crun echo $CRANE_MEM_PER_NODE
500
500
$ crun hostname
crane03
crane02
```

**Basic step execution:**

```bash
# First allocate resources
calloc -N 2 -c 8 -p CPU -A myaccount

# Within the allocation, run steps (no need to specify partition/account)
crun -N 1 -c 4 ./task1
crun -N 1 -c 4 ./task2
crun -N 2 -c 2 ./task3
```

**Multiple concurrent steps:**

```bash
# In calloc allocation
crun -N 1 ./long_running_task &
crun -N 1 ./another_task &
wait
```

**Step with specific resources:**

```bash
# Within calloc allocation with 4 nodes
crun -N 2 -c 8 --mem 4G ./memory_intensive_task
crun -w crane01,crane02 ./specific_node_task
```

**Monitoring steps:**

```bash
# In another terminal
cqueue --step -j $CRANE_JOB_ID
ccontrol show step $CRANE_JOB_ID.2
```

## Passing Arguments to Programs

Pass arguments to your program launched by crun:

```bash
# Using double dash
crun -c 1 -- your_program --your_args

# Using quotes
crun -c 1 "your_program --your_args"
```

## Related Commands

- [calloc](calloc.md) - Allocate resources for interactive use
- [cbatch](cbatch.md) - Submit batch jobs
- [ccancel](ccancel.md) - Cancel jobs and steps
- [cqueue](cqueue.md) - View job queue and steps
- [ccontrol](ccontrol.md) - Control and query jobs/steps
