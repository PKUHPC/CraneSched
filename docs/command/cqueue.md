# cqueue - View Job Queue

**cqueue displays information about jobs in the queue.**

View all jobs in the cluster's queues (including pending, running, and cancelled statuses). By default, displays 100 entries.

~~~bash
cqueue
~~~

**cqueue Output Example**

![cqueue](../images/cqueue/cqueue.png)

## Main Output Fields

- **JobId**: Job number
- **Partition**: Partition where the job is located
- **Name**: Job name
- **User**: Job owner
- **Account**: Job account
- **Status**: Job status
- **Type**: Job type
- **TimeLimit**: Job time limit
- **Nodes**: Number of nodes allocated to the job
- **NodeList**: Names of nodes where the job is running

## Main Options

- **-A/--account string**: Specify accounts to query (comma-separated list for multiple accounts)
- **-C/--config string**: Path to configuration file (default: "/etc/crane/config.yaml")
- **-F/--full**: Display full content. If not specified, only 30 characters per cell are displayed by default
- **-h/--help**: Display help
- **-i/--iterate uint**: Refresh query results at specified intervals (seconds). For example, `-i=3` outputs results every 3 seconds
- **-j/--job string**: Specify job IDs to query (comma-separated list). For example, `-j=2,3,4`
- **--json**: Output command execution results in JSON format
- **-m/--max-lines uint32**: Specify maximum number of output lines. For example, `-m=500` limits output to 500 lines
- **-n/--name string**: Specify job names to query (comma-separated list for multiple names)
- **-N/--noheader**: Hide table headers in output
- **-p/--partition string**: Specify partitions to query (comma-separated list for multiple partitions)
- **-q/--qos string**: Specify QoS to query (comma-separated list for multiple QoS)
- **--self**: View jobs submitted by current user
- **-S/--start**: Display job start time (for pending jobs, shows expected start time)
- **-t/--state string**: Specify job states to query. Valid values are 'pending(p)', 'running(r)' and 'all'. Default is 'all' (both pending and running jobs)
- **-u/--user string**: Specify users to query (comma-separated list for multiple users)
- **-v/--version**: Query version number

### Format Specifiers (-o/--format)

The `--format` option allows customized output formatting. Fields are identified by a percent sign (%) followed by a character or string.

**Format Specification Syntax:**
- `%[.]<size><type>` - Format field with optional width and alignment
  - Without size: Field uses natural width
  - With size only (`%5j`): Minimum width, left-aligned (padding on right)
  - With dot and size (`%.5j`): Minimum width, right-aligned (padding on left)

**Supported Format Identifiers** (case-insensitive):

| Identifier | Full Name | Description |
|------------|-----------|-------------|
| %a | Account | Account associated with the job |
| %c | AllocCpus | CPUs allocated to the job |
| %C | ReqCpus | Total CPUs requested by the job |
| %e | ElapsedTime | Elapsed time since job started |
| %h | Held | Hold state of the job |
| %j | JobID | Job ID |
| %k | Comment | Comment of the job |
| %l | TimeLimit | Time limit for the job |
| %L | NodeList | List of nodes the job is running on (or reason for pending) |
| %m | AllocMemPerNode | Allocated memory per node |
| %M | ReqMemPerNode | Requested memory per node |
| %n | Name | Job name |
| %N | NodeNum | Number of nodes requested by the job |
| %o | Command | Command line of the job |
| %p | Priority | Priority of the job |
| %P | Partition | Partition the job is running in |
| %q | QoS | Quality of Service level for the job |
| %Q | ReqCpuPerNode | Requested CPUs per node |
| %r | ReqNodes | Requested nodes |
| %R | Reason | Reason for pending status |
| %s | SubmitTime | Submission time of the job |
| %S | StartTime | Start time of the job |
| %t | State | Current state of the job |
| %T | JobType | Job type |
| %u | User | User who submitted the job |
| %U | Uid | UID of the job |
| %x | ExcludeNodes | Nodes excluded from the job |
| %X | Exclusive | Exclusive status of the job |

**Format Examples:**

```bash
# Natural width for all fields
cqueue --format "%j %n %t"

# Left-aligned with minimum widths
cqueue --format "%5j %20n %t"

# Right-aligned with minimum widths
cqueue --format "%.5j %.20n %t"

# Mixed formatting with labels
cqueue --format "ID:%8j | Name:%.15n | State:%t"
```

**Note:** If the format is invalid or unrecognized, the program will terminate with an error message.

## Usage Examples

**Display help:**
```bash
cqueue -h
```
![cqueue](../images/cqueue/cqueue_h.png)

**Hide table header:**
```bash
cqueue -N
```
![cqueue](../images/cqueue/cqueue_N.png)

**Show start times:**
```bash
cqueue -S
```
![cqueue](../images/cqueue/cqueue_S.png)

**Query specific jobs:**
```bash
cqueue -j 30674,30675
```
![cqueue](../images/cqueue/cqueue_j.png)

**Filter by state (pending jobs):**
```bash
cqueue -t Pending
```
![cqueue](../images/cqueue/cqueue_t.png)

**Filter by state (running jobs, shorthand):**
```bash
cqueue -t r
```
![cqueue](../images/cqueue/cqueue_tr.png)

**Query jobs for specific user:**
```bash
cqueue -u cranetest
```
![cqueue](../images/cqueue/cqueue_u.png)

**Query jobs for specific account:**
```bash
cqueue -A CraneTest
```
![cqueue](../images/cqueue/cqueue_A.png)

**Auto-refresh every 3 seconds:**
```bash
cqueue -i 3
```
![cqueue](../images/cqueue/cqueue_i.png)

**Filter by partition:**
```bash
cqueue -p CPU
```
![cqueue](../images/cqueue/cqueue_p.png)

**Limit output to 3 lines:**
```bash
cqueue -m 3
```
![cqueue](../images/cqueue/cqueue_m.png)

**Custom format output:**
```bash
cqueue -o="%n %u %.5j %.5t %.3T %.5T"
```
![cqueue](../images/cqueue/cqueue_o.png)

**Filter by job name:**
```bash
cqueue -n test
```
![cqueue](../images/cqueue/cqueue_n1.png)

**Filter by QoS:**
```bash
cqueue -q test_qos
```
![cqueue](../images/cqueue/cqueue_q.png)

**Show only current user's jobs:**
```bash
cqueue --self
```
![cqueue](../images/cqueue/cqueue_self.png)

**JSON output:**
```bash
cqueue --json
```

## Related Commands

- [cbatch](cbatch.md) - Submit batch jobs
- [crun](crun.md) - Run interactive jobs
- [calloc](calloc.md) - Allocate resources
- [ccancel](ccancel.md) - Cancel jobs
- [cacct](cacct.md) - Query completed jobs
