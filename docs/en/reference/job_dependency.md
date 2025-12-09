# Job Dependency

## Overview

The dependency feature in CraneSched-FrontEnd allows jobs to control their execution timing based on the status of other jobs, enabling job dependency management. Through dependency relationships, you can build complex workflows to ensure jobs execute in the correct order.

## Supported Commands

The dependency feature is available in the following commands:

- `cbatch` - Batch job submission
- `calloc` - Interactive resource allocation
- `crun` - Interactive job execution

## Command Line Parameter

```bash
--dependency, -d <dependency_string>
```

Use the `--dependency` or `-d` parameter when submitting a job to specify dependency relationships.

## Dependency String Format

### Basic Syntax

```
<type>:<job_id>[+<delay>][:<job_id>[+<delay>]]...
```

### Dependency Types

| Type | Description | Trigger Condition |
|------|-------------|-------------------|
| `after` | Start after specified job begins or is cancelled | Dependent job leaves Pending state |
| `afterok` | Start after specified job succeeds | Dependent job completes with exit code 0 |
| `afternotok` | Start after specified job fails | Dependent job completes with non-zero exit code (including timeout, node errors, etc.) |
| `afterany` | Start after specified job completes | Dependent job ends (regardless of success or failure) |

### Delay Time

Optional delay parameter, supporting the following formats:

- **Plain numbers** (default unit is minutes)
  - Example: `10` = 10 minutes

- **Time with units**
  - `s`, `sec`, `second`, `seconds` - seconds
  - `m`, `min`, `minute`, `minutes` - minutes
  - `h`, `hour`, `hours` - hours
  - `d`, `day`, `days` - days
  - `w`, `week`, `weeks` - weeks

- **D-HH:MM:SS format**
  - Example: `1-01:30:00` = 1 day 1 hour 30 minutes

### Multiple Dependency Combinations

#### AND Logic (all conditions must be satisfied)

Use `,` to separate different dependency conditions:

```bash
after:100,afterok:101
```

The job will wait for job 100 to start **and** job 101 to complete successfully.

#### OR Logic (any condition satisfied)

Use `?` to separate different dependency conditions:

```bash
afterok:100?afterok:101
```

The job will start after job 100 **or** job 101 completes successfully.

!!! warning "Note"
    You cannot mix `,` and `?` in the same dependency string. The system will return an error.

## Usage Examples

### 1. Basic Dependencies

```bash
# Wait for job 100 to start before running
cbatch --dependency after:100 my_script.sh

# Wait for job 100 to complete successfully before running
cbatch --dependency afterok:100 my_script.sh

# Wait for job 100 to fail before running
cbatch --dependency afternotok:100 my_script.sh

# Wait for job 100 to complete before running (regardless of success or failure)
cbatch --dependency afterany:100 my_script.sh
```

### 2. Dependencies with Delays

```bash
# Wait for job 100 to complete successfully, then delay 30 minutes before running
cbatch --dependency afterok:100+30 my_script.sh

# Wait for job 100 to complete successfully, then delay 10 seconds before running
cbatch --dependency afterok:100+10s my_script.sh

# Use HH:MM:SS format to specify delay
cbatch --dependency afterok:100+01:30:00 my_script.sh
```

### 3. Multiple Dependencies

```bash
# Wait for job 100 to start AND jobs 101, 102 to both complete successfully
cbatch --dependency after:100,afterok:101:102 my_script.sh

# Wait for job 100 to start for 10 minutes AND job 101 to complete successfully for 30 minutes
cbatch --dependency after:100+10m,afterok:101+30m my_script.sh

# Wait for job 100 to succeed OR job 101 to fail
cbatch --dependency afterok:100?afternotok:101 my_script.sh

# Wait for jobs 100, 101 to both succeed with 2 hour delay, or job 102 to start immediately
cbatch --dependency afterok:100:101+2h?after:102 my_script.sh
```

### 4. Using in Batch Scripts

You can also use the `#CBATCH` directive in batch scripts:

```bash
#!/bin/bash
#CBATCH --dependency afterok:100
#CBATCH --nodes 2
#CBATCH --time 1:00:00
#CBATCH --output job-%j.out

echo "This job starts after job 100 completes successfully"
# Your job code
```

### 5. Using in Interactive Commands

```bash
# Using dependency with calloc
calloc --dependency afterok:100 -n 4 -N 2

# Using dependency with crun
crun --dependency after:100 -n 1 hostname
```

## Viewing Dependency Status

Use the `ccontrol show job <job_id>` command to view job dependency status:

```bash
ccontrol show job 105
```

### Output Example

```
JobId=105
...
Dependency=PendingDependencies=afterok:100+01:00:00 Status=WaitForAll
```

### Dependency Status Field Descriptions

| Field | Description |
|-------|-------------|
| `PendingDependencies` | Dependencies not yet triggered |
| `DependencyStatus` | Dependency satisfaction status (see table below) |

### Dependency Status Values

| Status | Description |
|--------|-------------|
| `WaitForAll` | Waiting for all dependencies to be satisfied (AND logic) |
| `WaitForAny` | Waiting for any dependency to be satisfied (OR logic) |
| `ReadyAfter <time>` | Will be ready after the specified time |
| `SomeFailed` | Some dependencies failed (AND logic, cannot be satisfied) |
| `AllFailed` | All dependencies failed (OR logic, cannot be satisfied) |

## Error Handling

The system will return errors in the following situations:

| Error Condition | Description | Example |
|----------------|-------------|---------|
| Mixed separators | Cannot use `,` and `?` together | `afterok:100,afterok:101?afterok:102` |
| Format error | Dependency string doesn't conform to syntax | `afterok:` or `after100` |
| Invalid delay format | Delay time format is incorrect | `afterok:100+invalid` |
| Duplicate dependency | Same job ID appears multiple times | `afterok:100:100` |
| Job ID doesn't exist or ended | Dependent job doesn't exist (runtime check) | `afterok:99999` |

### Error Examples

```bash
# Error: Mixed AND and OR separators
$ cbatch --dependency afterok:100,afterok:101?afterok:102 job.sh

# Error: Invalid delay format
$ cbatch --dependency afterok:100+invalid job.sh

# Error: Duplicate job ID
$ cbatch --dependency afterok:100,afternotok:100 job.sh
```

## Related Commands

- [cbatch](../command/cbatch.md) - Submit batch jobs
- [crun](../command/crun.md) - Run interactive tasks
- [calloc](../command/calloc.md) - Allocate resources and create interactive shell
- [cqueue](../command/cqueue.md) - View job queue status
- [ccontrol](../command/ccontrol.md) - Control jobs and system resources
- [ccancel](../command/ccancel.md) - Cancel jobs
