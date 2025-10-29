# ccancel - Cancel Jobs

**ccancel terminates running or pending jobs in the queue.**

You can cancel jobs by job ID or by using filter conditions to cancel multiple jobs at once.

## Command Syntax

```bash
ccancel [job_id[,job_id...]] [OPTIONS]
```

## Command Line Options

### Job Selection
- **job_id[,job_id...]**: Job ID(s) to cancel (comma-separated list). Format: `<job_id>` or `<job_id>,<job_id>,<job_id>...`

### Filter Options
- **-n, --name string**: Cancel jobs with the specified job name
- **-u, --user string**: Cancel jobs submitted by the specified user
- **-A, --account string**: Cancel jobs under the specified account
- **-p, --partition string**: Cancel jobs in the specified partition
- **-t, --state string**: Cancel jobs in the specified state. Valid states: `PENDING` (P), `RUNNING` (R), `ALL` (case-insensitive)
- **-w, --nodes strings**: Cancel jobs running on the specified nodes (comma-separated list)

### Output Options
- **--json**: Output in JSON format

### Miscellaneous
- **-C, --config string**: Configuration file path (default: `/etc/crane/config.yaml`)
- **-h, --help**: Display help information
- **-v, --version**: Display version number

## Filter Rules

!!! important
    At least one condition must be provided: either job ID(s) or at least one filter option.

When using multiple filters, jobs matching **all** specified conditions will be cancelled (AND logic).

## Usage Examples

### Cancel by Job ID

Cancel a single job:
```bash
ccancel 30686
```

**Result:**

![ccancel](../images/ccancel/ccancel_1.png)
![ccancel](../images/ccancel/ccancel_2.png)
![ccancel](../images/ccancel/ccancel_3.png)

Cancel multiple jobs:
```bash
ccancel 30686,30687,30688
```

### Cancel by Job Name

Cancel all jobs named "test1":
```bash
ccancel -n test1
```

**Result:**

![ccancel](../images/ccancel/ccancel_n1.png)
![ccancel](../images/ccancel/ccancel_n2.png)
![ccancel](../images/ccancel/ccancel_n3.png)

### Cancel by Partition

Cancel all jobs in GPU partition:
```bash
ccancel -p GPU
```

**Result:**

![ccancel](../images/ccancel/ccancel_p1.png)
![ccancel](../images/ccancel/ccancel_p2.png)
![ccancel](../images/ccancel/ccancel_p3.png)

### Cancel by Node

Cancel all jobs running on crane02:
```bash
ccancel -w crane02
```

![ccancel](../images/ccancel/ccancel_w.png)

### Cancel by State

Cancel all pending jobs:
```bash
ccancel -t Pending
```

![ccancel](../images/ccancel/ccancel_t.png)

Cancel all running jobs:
```bash
ccancel -t Running
```

Cancel all jobs (pending and running):
```bash
ccancel -t All
```

### Cancel by Account

Cancel all jobs under PKU account:
```bash
ccancel -A PKU
```

![ccancel](../images/ccancel/ccancel_A.png)

### Cancel by User

Cancel all jobs submitted by user ROOT:
```bash
ccancel -u ROOT
```

![ccancel](../images/ccancel/ccancel_u.png)

### Combined Filters

Cancel all pending jobs in CPU partition:
```bash
ccancel -t Pending -p CPU
```

Cancel all jobs by user alice in GPU partition:
```bash
ccancel -u alice -p GPU
```

Cancel running jobs on specific nodes:
```bash
ccancel -t Running -w crane01,crane02
```

### JSON Output

Get cancellation results in JSON format:
```bash
ccancel 30686 --json
```

Cancel with filters and JSON output:
```bash
ccancel -p GPU -t Pending --json
```

## Examples Overview

![ccancel](../images/ccancel/ccancel.png)

## Behavior After Cancellation

After a job is cancelled:

1. **Process Termination**: If there are no other jobs from the user on the allocated nodes, the job scheduling system will terminate all user processes on those nodes

2. **SSH Access Revocation**: SSH access to the allocated nodes will be revoked for the user

3. **Resource Release**: All allocated resources (CPUs, memory, GPUs) are immediately released and become available for other jobs

4. **Job State Update**: The job state changes to `CANCELLED` in the job history

## Permission Requirements

- **Regular Users**: Can only cancel their own jobs
- **Coordinators**: Can cancel jobs within their account
- **Operators/Admins**: Can cancel any job in the system

## Important Notes

1. **Immediate Effect**: Job cancellation takes effect immediately. Running jobs are terminated without grace period by default

2. **Multiple Jobs**: You can cancel multiple jobs at once using comma-separated job IDs or filter conditions

3. **No Confirmation**: There is no confirmation prompt. Jobs are cancelled immediately upon command execution

4. **State Filtering**: Use `-t` to target specific job states to avoid accidentally cancelling jobs in unintended states

5. **Job ID Format**: Job IDs must follow the format `<job_id>` or `<job_id>,<job_id>,<job_id>...` with no spaces

## Error Handling

Common errors:

- **Invalid Job ID**: Returns error if job ID doesn't exist or you don't have permission to cancel it
- **No Matching Jobs**: If filter conditions match no jobs, returns success with zero jobs cancelled
- **Invalid State**: State must be one of: PENDING, RUNNING, ALL (case-insensitive)

## See Also

- [cbatch](cbatch.md) - Submit batch jobs
- [crun](crun.md) - Run interactive tasks
- [calloc](calloc.md) - Allocate interactive resources
- [cqueue](cqueue.md) - View job queue
- [cacct](cacct.md) - View job accounting information
- [ccontrol](ccontrol.md) - Control jobs and system resources
