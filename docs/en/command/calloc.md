# calloc - Allocate Resources and Create Interactive Shell

**calloc allocates resources using command-line specified parameters and creates a new interactive shell on the allocated compute nodes.**

calloc must be started on a node where `cfored` is running. When the task starts, it enters a new user terminal where you can directly work on the compute node.

!!! tip
    Exiting the calloc shell will terminate the allocation and release the resources.

## Command Line Options

### Resource Specifications
- **-N, --nodes uint32**: Number of nodes to allocate (default: 1)
- **-c, --cpus-per-task float**: Number of CPU cores per task (default: 1)
- **--ntasks-per-node uint32**: Number of tasks per node (default: 1)
- **--mem string**: Maximum memory per node. Supports GB (G, g), MB (M, m), KB (K, k), and Bytes (B), default unit is MB
- **--gres string**: Generic resources required per task, format: `gpu:a100:1` or `gpu:1`
- **--L, --licenses**: The licenses that the job requires to use, format: `lic1:2,lic2:4` or `lic1:2|lic2:4`

### Job Information
- **-J, --job-name string**: Job name
- **-A, --account string**: Account for job submission
- **-p, --partition string**: Requested partition
- **-q, --qos string**: Quality of Service (QoS) for the job
- **-t, --time string**: Time limit, format: `[day-]hours:minutes:seconds` (e.g., `5-0:0:1` for 5 days and 1 second, or `10:1:2` for 10 hours, 1 minute, 2 seconds)
- **--comment string**: Job comment

### Node Selection
- **-w, --nodelist string**: Nodes to allocate (comma-separated list)
- **-x, --exclude string**: Exclude specific nodes from allocation (comma-separated list)

### Environment Variables
- **--get-user-env**: Load user's login environment variables
- **--export string**: Propagate environment variables

### Scheduling Options
- **-d, --dependency string**: Job dependency. Format: `<type>:<job_id>[+<delay>][:<job_id>][,<type>:<job_id>[:<job_id>]]` or `<type>:<job_id>[:<job_id>][?<type>:<job_id>[:<job_id>]]`. Supported types: `after`, `afterok`, `afternotok`, `afterany`. **Note**: For `<delay>`, use time with units (e.g., `10s`, `5m`, `2h`) - do NOT use `HH:MM:SS` format as `:` is the job ID separator. See [Job Dependency](../reference/job_dependency.md) for details
- **--exclusive**: Request exclusive node resources
- **-H, --hold**: Submit job in held state
- **-r, --reservation string**: Use reserved resources

### Email Notifications
- **--mail-type string**: Notify user by mail when certain events occur. Supported values: `NONE`, `BEGIN`, `END`, `FAIL`, `TIMELIMIT`, `ALL` (default: `NONE`)
- **--mail-user string**: Mail address of notification receiver

### Miscellaneous
- **-D, --chdir string**: Working directory of the job
- **--extra-attr string**: Extra attributes of the job (JSON format)
- **--debug-level string**: Debug level: `trace`, `debug`, `info` (default: `info`)
- **-C, --config string**: Configuration file path (default: `/etc/crane/config.yaml`)
- **-h, --help**: Display help information
- **-v, --version**: Display calloc version
- **--signal**: Send signal to job

## Usage Examples

### Help Information

Display help:
```bash
calloc -h
```
```text
[cranetest@crane01 ~]$ calloc -h
Allocate resource and create terminal

Usage:
  calloc [flags]

Flags:
  -A, --account string           Account used for the job
  -D, --chdir string             Working directory of the job
      --comment string           Comment of the job
  -C, --config string            Path to configuration file (default "/etc/crane/config.yaml")
  -c, --cpus-per-task float      Number of cpus required per task (default 1)
      --debug-level string       Available debug level: trace, debug, info (default "info")
  -d, --dependency string        Conditions for job to execute
  -x, --exclude string           Exclude specific nodes from allocating (commas separated list)
      --exclusive                Exclusive node resources
      --export string            Propagate environment variables
      --extra-attr string        Extra attributes of the job (in JSON format)
      --get-user-env             Load login environment variables of the user
      --gpus-per-node string     Gpus required per node, format: [type:]<number>[,[type:]<number>...]. eg: "4" or "a100:1,volta:1"
      --gres string              Gres required per task,format: "gpu:a100:1" or "gpu:1"
  -h, --help                     help for calloc
  -H, --hold                     Hold the job until it is released
  -J, --job-name string          Name of job
  -L, --licenses string          Licenses used for the job
      --mail-type string         Notify user by mail when certain events occur, supported values: NONE, BEGIN, END, FAIL, TIMELIMIT, ALL (default is NONE)
      --mail-user string         Mail address of the notification receiver
      --mem string               Maximum amount of real memory, support GB(G, g), MB(M, m), KB(K, k) and Bytes(B), default unit is MB
      --mem-per-cpu string       Maximum amount of real memory per CPU, support GB(G, g), MB(M, m), KB(K, k) and Bytes(B), default unit is MB
  -w, --nodelist string          Nodes to be allocated to the job (commas separated list)
  -N, --nodes uint32             Number of nodes on which to run (default 1)
      --ntasks-per-node uint32   Number of tasks to invoke on each node (default 1)
  -p, --partition string         Partition requested
  -q, --qos string               QoS used for the job
  -Q, --quiet                    Quiet mode (suppress informational messages)
  -r, --reservation string       Use reserved resources
  -s, --signal string            Send signal when time limit within time seconds, format: [{R}:]<sig_num>[@sig_time]
  -t, --time string              Time limit, format: "day-hours:minutes:seconds" 5-0:0:1 for 5 days, 1 second or "hours:minutes:seconds" 10:1:2 for 10 hours, 1 minute, 2 seconds
  -v, --version                  version for calloc
      --wckey string             Wckey of the job
```


### Basic Resource Allocation

Allocate 2 nodes with 1 CPU core and 200M memory in CPU partition:
```bash
calloc -c 1 --mem 200M -p CPU -N 2
```

**Result:**

```text
[cranetest@crane01 ~]$ calloc -c 1 --mem 200M -p CPU -N 2
Task id allocated: 30688
Allocated craned nodes: crane[01-02].
```
```text
[cranetest@crane01 ~]$ exit
exit
Task completed.
```

### Specify Account and Node List

Allocate 1 node in GPU partition with 2 tasks per node, using specific account and node list:
```bash
calloc -A acct-test --ntasks-per-node 2 -w crane02,crane03 -p GPU -N 1
```

**Result:**

```text
[cranetest@crane01 ~]$ calloc -A acct-test --ntasks-per-node 2 -w crane02,crane03 -p GPU -N 1
Task id allocated: 30736
Allocated craned nodes: crane03.
```

```text
[cranetest@crane01 ~]$ exit
exit
Task completed.
```

### Time Limit and QoS

Allocate resources with time limit and specific QoS:
```bash
calloc --mem 200M -p CPU -q test-qos -t 00:25:25
```

```text
[cranetest@crane01 ~]$ calloc --mem 200M -p CPU -q test-qos -t 00:25:25
Task id allocated: 30737
Allocated craned nodes: crane02.
```
```text
[cranetest@crane01 ~]$ exit
exit
Task completed.
```

### Working Directory

Specify working directory:
```bash
calloc -D /path
```

```text
[root@cranetest01 zhouhao]# calloc -D /nfs/home/zhouhao/test --debug-level trace --export ALL
Task id allocated: 208
Allocated craned nodes: cranetest02.
Oct 12 15:56:04.488 [TRAC] Pgrp: 6419
Oct 12 15:56:04.488 [TRAC] IsForeground: true
Oct 12 15:56:04.488 [TRAC] Proc.Pid: 6426
Oct 12 15:56:04.489 [TRAC] Proc.Pgid: 6426
[root@cranetest01 zhouhao]# exit
exit
```

### Debug Level

Set debug level to trace:
```bash
calloc --debug-level trace
```

```text
[root@cranetest01 zhouhao]# calloc -D /nfs/home/zhouhao/test --debug-level trace --export ALL
Task id allocated: 208
Allocated craned nodes: cranetest02.
Oct 12 15:56:04.488 [TRAC] Pgrp: 6419
Oct 12 15:56:04.488 [TRAC] IsForeground: true
Oct 12 15:56:04.488 [TRAC] Proc.Pid: 6426
Oct 12 15:56:04.489 [TRAC] Proc.Pgid: 6426
[root@cranetest01 zhouhao]# exit
exit
```

### Exclude Nodes

Exclude specific nodes from allocation:
```bash
calloc -x cranetest02
```

```text
[zhouhao@cranetest02 zhouhao]# calloc -A ROOT -x cranetest02 -J test_calloc -N 2 --ntasks-per-node 2 -t 00:25:00 --get-user-env
Task id allocated: 184
Allocated craned nodes: cranetest[03-04].
```

### User Environment

Load user's login environment:
```bash
calloc --get-user-env
```

```text
[zhouhao@cranetest02 zhouhao]# calloc -A ROOT -x cranetest02 -J test_calloc -N 2 --ntasks-per-node 2 -t 00:25:00 --get-user-env
Task id allocated: 184
Allocated craned nodes: cranetest[03-04].
```

### Job Name

Specify job name:
```bash
calloc -J job_name
```

```text
[root@cranetest01 zhouhao]# calloc -A ROOT -x cranetest02 -J test_calloc -N 2 --ntasks-per-node 2 -t 00:25:00 --get-user-env
Task id allocated: 184
Allocated craned nodes: cranetest[03-04].
```

## Advanced Features

### Exclusive Nodes

Request exclusive access to nodes:
```bash
calloc --exclusive -N 2 -p CPU
```

### Held Job

Submit job in held state (requires manual release):
```bash
calloc --hold -N 1 -p GPU
```

Release the job later using `ccontrol release <job_id>`.

### Use Reservation

Allocate resources from a reservation:
```bash
calloc -r my_reservation -N 2
```

### Email Notifications

Receive email notifications for job events:
```bash
calloc --mail-type=ALL --mail-user=user@example.com -N 1 -p CPU
```

### GPU Allocation

Allocate GPU resources:
```bash
calloc --gres=gpu:a100:2 -N 1 -p GPU
```

### Signal

When a job is within sig_time seconds of its end time, the system will send it the signal sig_num. sig_num can be either a signal number or name (e.g., "10" or "USR1"). sig_time must be an integer between 0 and 65535. By default, no signal is sent before the job's end time. If only sig_num is specified without sig_time, the default lead time is 60 seconds.

```bash
calloc --signal=SIGUSR1@60 -N 1 -p CPU
```

## Interactive Usage

Once calloc allocates resources, you'll be placed in a new shell on the allocated compute node(s). You can:

1. **Run commands directly** on the compute nodes
2. **Access all allocated nodes** via SSH
3. **Use environment variable** `CRANE_JOB_NODELIST` to see allocated nodes
4. **Run parallel tasks** across allocated nodes

**Example session:**
```bash
# Start allocation
$ calloc -N 2 -c 4 -p CPU

# Now in the allocated shell
$ hostname
crane01

$ echo $CRANE_JOB_NODELIST
crane01;crane02

# Run commands on all nodes
$ for node in crane01 crane02; do ssh $node hostname; done
crane01
crane02

# Exit to release resources
$ exit
```

## Important Notes

1. **cfored Requirement**: calloc must be run on a node where `cfored` is running

2. **Resource Release**: Exiting the calloc shell will automatically terminate the job and release all allocated resources

3. **Interactive Nature**: Unlike `cbatch`, calloc provides an interactive shell for immediate work

4. **Node Access**: You have SSH access to all allocated nodes during the allocation

5. **Nested Execution**: You can run `crun` within a calloc allocation to inherit its resources

## Comparison with Other Commands

| Command | Type | Usage |
|---------|------|-------|
| **calloc** | Interactive allocation | Get a shell on compute nodes |
| **crun** | Interactive execution | Run a specific program interactively |
| **cbatch** | Batch submission | Submit a script for later execution |

## See Also

- [crun](crun.md) - Run interactive tasks
- [cbatch](cbatch.md) - Submit batch jobs
- [cqueue](cqueue.md) - View job queue
- [ccancel](ccancel.md) - Cancel jobs
