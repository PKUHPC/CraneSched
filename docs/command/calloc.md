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

### Job Information
- **-J, --job-name string**: Job name
- **-A, --account string**: Account for job submission
- **-p, --partition string**: Requested partition
- **-q, --qos string**: Quality of Service (QoS) for the job
- **-t, --time string**: Time limit, format: `day-hours:minutes:seconds` (e.g., `5-0:0:1` for 5 days, 1 second) or `hours:minutes:seconds` (e.g., `10:1:2` for 10 hours, 1 minute, 2 seconds)
- **--comment string**: Job comment

### Node Selection
- **-w, --nodelist string**: Nodes to allocate (comma-separated list)
- **-x, --exclude string**: Exclude specific nodes from allocation (comma-separated list)

### Environment Variables
- **--get-user-env**: Load user's login environment variables
- **--export string**: Propagate environment variables

### Scheduling Options
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

## Usage Examples

### Help Information

Display help:
```bash
calloc -h
```
![calloc](../images/calloc/calloc_h.png)

### Basic Resource Allocation

Allocate 2 nodes with 1 CPU core and 200M memory in CPU partition:
```bash
calloc -c 1 --mem 200M -p CPU -N 2
```

**Result:**

![calloc](../images/calloc/calloc_c1.png)
![calloc](../images/calloc/calloc_c2.png)

### Specify Account and Node List

Allocate 1 node in GPU partition with 2 tasks per node, using specific account and node list:
```bash
calloc -A acct-test --ntasks-per-node 2 -w crane02,crane03 -p GPU -N 1
```

**Result:**

![calloc](../images/calloc/calloc_A1.png)
![calloc](../images/calloc/calloc_A2.png)

### Time Limit and QoS

Allocate resources with time limit and specific QoS:
```bash
calloc --mem 200M -p CPU -q test-qos -t 00:25:25
```

![calloc](../images/calloc/calloc_mem1.png)
![calloc](../images/calloc/calloc_mem2.png)

### Working Directory

Specify working directory:
```bash
calloc -D /path
```

![calloc](../images/calloc/calloc_D.png)

### Debug Level

Set debug level to trace:
```bash
calloc --debug-level trace
```

![calloc](../images/calloc/calloc_debug.png)

### Exclude Nodes

Exclude specific nodes from allocation:
```bash
calloc -x cranetest02
```

![calloc](../images/calloc/calloc_x.png)

### User Environment

Load user's login environment:
```bash
calloc --get-user-env
```

![calloc](../images/calloc/calloc_get_user.png)

### Job Name

Specify job name:
```bash
calloc -J job_name
```

![calloc](../images/calloc/calloc_j.png)

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
