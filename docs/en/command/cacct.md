# cacct - View Job Accounting Information

cacct displays accounting information for jobs and steps in the cluster. It queries all job states, including completed, failed, and cancelled jobs. The output automatically includes jobs and their associated steps.

View all jobs and steps in the cluster:

```bash
cacct
```

## Options

**-h, --help**

:   **Applies to:** `Job`, `Step`  
Display help information for the cacct command.

**-v, --version**

:   **Applies to:** `Job`, `Step`  
Display cacct version information.

**-C, --config=&lt;path&gt;**

:   **Applies to:** `Job`, `Step`  
Configuration file path. Default: "/etc/crane/config.yaml".

**-j, --job=&lt;jobid1,jobid2,...&gt;**

:   **Applies to:** `Job`, `Step`  
Specify job IDs to query (comma-separated list). For example, `-j=2,3,4`. When querying jobs, results will be filtered by job ID. The output will include matching jobs and their associated steps. Supports using job step ID format `jobid.stepid` to query specific steps.

**-n, --name=&lt;name1,name2,...&gt;**

:   **Applies to:** `Job`, `Step`  
Specify job names to query (comma-separated list for multiple names).

**-u, --user=&lt;username1,username2,...&gt;**

:   **Applies to:** `Job`, `Step`  
Specify users to query (comma-separated list for multiple users). Filter jobs and steps by the specified usernames.

**-A, --account=&lt;account1,account2,...&gt;**

:   **Applies to:** `Job`, `Step`  
Specify accounts to query (comma-separated list for multiple accounts). Filter jobs and steps by the specified accounts.

**-p, --partition=&lt;partition1,partition2,...&gt;**

:   **Applies to:** `Job`, `Step`  
Specify partitions to view (comma-separated list for multiple partitions). Default: all partitions.

**-q, --qos=&lt;qos1,qos2,...&gt;**

:   **Applies to:** `Job`, `Step`  
Specify QoS to view (comma-separated list for multiple QoS). Default: all QoS levels.

**-t, --state=&lt;state&gt;**

:   **Applies to:** `Job`, `Step`  
Specify job state to view. Supported states: 'pending' or 'p', 'running' or 'r', 'completed' or 'c', 'failed' or 'f', 'cancelled' or 'x', 'time-limit-exceeded' or 't', and 'all'. Default: 'all'. Multiple states can be specified as a comma-separated list.

**-s, --submit-time=&lt;time_range&gt;**

:   **Applies to:** `Job`, `Step`  
Filter jobs by submit time range. Supports closed intervals (format: `2024-01-02T15:04:05~2024-01-11T11:12:41`) or half-open intervals (format: `2024-01-02T15:04:05~` for after a specific time, or `~2024-01-11T11:12:41` for before a specific time).

**-S, --start-time=&lt;time_range&gt;**

:   **Applies to:** `Job`, `Step`  
Filter jobs by start time range. Format same as submit-time.

**-E, --end-time=&lt;time_range&gt;**

:   **Applies to:** `Job`, `Step`  
Filter jobs by end time range. Format same as submit-time. For example, `~2023-03-14T10:00:00` filters jobs that ended before the specified time.

**-w, --nodelist=&lt;node1,node2,...&gt;**

:   **Applies to:** `Job`, `Step`  
Specify node names to view (comma-separated list or pattern such as node[1-10]). Default: all nodes.

**--type=&lt;type1,type2,...&gt;**

:   **Applies to:** `Job`, `Step`  
Specify task types to view (comma-separated list). Valid values: 'Interactive', 'Batch', 'Container'. Default: all types.

**-F, --full**

:   **Applies to:** `Job`, `Step`  
Display full information without truncating fields. By default, each cell displays only 30 characters.

**-N, --noheader**

:   **Applies to:** `Job`, `Step`  
Hide table header in output.

**-m, --max-lines=&lt;number&gt;**

:   **Applies to:** `Job`, `Step`  
Specify the maximum number of output results. For example, `-m=500` limits output to 500 lines. Default: 100 lines.

**--json**

:   **Applies to:** `Job`, `Step`  
Output command execution results in JSON format instead of table format.

**-o, --format=&lt;format_string&gt;**

:   **Applies to:** `Job`, `Step`  
Customize output format using format specifiers. Fields are identified by a percent sign (%) followed by a character or string. Format specification syntax: `%[.]<size><type>`. Without size: field uses natural width. With size only (`%5j`): minimum width, left-aligned. With dot and size (`%.5j`): minimum width, right-aligned. See the Format Specifiers section below for available format identifiers.

## Default Output Fields

When displaying default format, the following fields are shown:

- **JobId**: Job or job step identifier (format: jobid for jobs, jobid.stepid for steps)
- **JobName**: Job or job step name
- **Partition**: Partition where job/job step runs
- **Account**: Account billed for job/job step
- **AllocCPUs**: Number of allocated CPUs
- **State**: Job/job step state (e.g., COMPLETED, FAILED, CANCELLED)
- **ExitCode**: Exit code (format: exitcode:signal, see [Exit Code Reference](../reference/exit_code.md))

## Format Specifiers

The following format identifiers are supported (case-insensitive):

| Identifier            | Description                                        |
|-----------------------|-----------------------------------------------------|
| %a / %Account         | Account associated with job/job step               |
| %C / %ReqCpus         | Number of requested CPUs                            |
| %c / %AllocCpus       | Number of allocated CPUs                            |
| %D / %ElapsedTime     | Elapsed time since job/job step started             |
| %E / %EndTime         | End time of job/job step                            |
| %e / %ExitCode        | Exit code (format: exitcode:signal)                 |
| %h / %Held            | Hold state of job                                   |
| %j / %JobID           | Job ID (or job step ID in format jobid.stepid)      |
| %K / %Wckey           | Workload characterization key                       |
| %k / %Comment         | Comment for job                                     |
| %L / %NodeList        | List of nodes where job/job step runs               |
| %l / %TimeLimit       | Time limit for job/job step                         |
| %M / %ReqMemPerNode   | Memory requested per node                           |
| %m / %AllocMemPerNode | Memory allocated per node                           |
| %N / %NodeNum         | Number of nodes                                     |
| %n / %JobName         | Name of job/job step                                |
| %P / %Partition       | Partition associated with job/job step              |
| %p / %Priority        | Priority of job                                     |
| %q / %Qos             | Quality of service level                            |
| %R / %Reason          | Reason for pending state                            |
| %r / %ReqNodes        | Requested nodes                                     |
| %S / %StartTime       | Start time of job/job step                          |
| %s / %SubmitTime      | Submit time of job                                  |
| %t / %State           | Current state of job/job step                       |
| %T / %JobType         | Job type (e.g., Batch, Interactive)                 |
| %U / %UserName        | Username that submitted the job                     |
| %u / %Uid             | User ID                                             |
| %x / %ExcludeNodes    | Nodes excluded from job                             |
| %X / %Exclusive       | Exclusive state of job                              |

## Usage Examples

### Basic Queries

**View all jobs and steps:**

```bash
cacct
```

![cacct](../../images/cacct/cacct.png)

**Display help:**

```bash
cacct -h
```

![cacct](../../images/cacct/h.png)

**Hide table header:**

```bash
cacct -N
```

![cacct](../../images/cacct/N.png)

### Filter by ID and Name

**Query specific job IDs:**

```bash
cacct -j=30618,30619,30620
```

![cacct](../../images/cacct/j.png)

**Query by job name:**

```bash
cacct -n=Test_Job
```

![cacct](../../images/cacct/nt.png)

**Query by name pattern:**

```bash
cacct -n test
```

![cacct](../../images/cacct/ntest.png)

### Filter by User and Account

**Query jobs by user:**

```bash
cacct -u=cranetest
```

![cacct](../../images/cacct/u.png)

**Query jobs by account:**

```bash
cacct -A=CraneTest
```

![cacct](../../images/cacct/A.png)

**Combine account and max lines:**

```bash
cacct -A ROOT -m 10
```

![cacct](../../images/cacct/am.png)

### Filter by Partition and QoS

**Query jobs in a specific partition:**

```bash
cacct -p GPU
```

![cacct](../../images/cacct/p.png)

**Query by QoS:**

```bash
cacct -q test_qos
```

![cacct](../../images/cacct/qt.png)

### Time Range Filtering

**Filter by start time range:**

```bash
cacct -S=2024-07-22T10:00:00~2024-07-24T10:00:00
```

![cacct](../../images/cacct/S.png)

**Filter by end time range:**

```bash
cacct -E=2024-07-22T10:00:00~2024-07-24T10:00:00
```

![cacct](../../images/cacct/E.png)

**Query jobs submitted in a time range:**

```bash
cacct -s=2024-01-01T00:00:00~2024-01-31T23:59:59
```

**Query jobs that started after a specific time:**

```bash
cacct -S=2024-01-15T00:00:00~
```

**Query jobs that ended before a specific time:**

```bash
cacct -E=~2024-01-31T23:59:59
```

### State Filtering

**View only completed jobs:**

```bash
cacct -t completed
```

**View failed and cancelled jobs:**

```bash
cacct -t failed,cancelled
```

**View timed-out jobs:**

```bash
cacct -t time-limit-exceeded
```

**Filter by job type:**

```bash
# View only container jobs
cacct --type Container

# View batch jobs
cacct --type Batch

# View interactive jobs
cacct --type Interactive
```

!!! tip "Container Job Management"
    In addition to basic queries, see [ccon command manual](ccon.md) for more container-specific operations.

### Output Control

**Limit output to 10 lines:**

```bash
cacct -m=10
```

![cacct](../../images/cacct/m.png)

**JSON output:**

```bash
cacct --json -j 12345
```

### Custom Format Output

**Specify custom output format:**

```bash
cacct -o="%j %.10n %P %a %t"
```

![cacct](../../images/cacct/o.png)

**All fields using natural width:**

```bash
cacct --format "%j %n %t"
```

**Left-aligned with minimum width:**

```bash
cacct --format "%5j %20n %t"
```

**Right-aligned with minimum width:**

```bash
cacct --format "%.5j %.20n %t"
```

**Mixed format with labels:**

```bash
cacct -o="%.8j %20n %-10P %.15U %t"
```

### Combined Filtering

**Multiple filters with full output:**

```bash
cacct -m 10 -j 783925,783889 -t=c -F
```

![cacct](../../images/cacct/mj.png)

**Complex combined query:**

```bash
cacct -m 10 -E=2024-10-08T10:00:00~2024-10-10T10:00:00 -p CPU -t c
```

![cacct](../../images/cacct/me.png)

## Related Commands

- [cqueue](cqueue.md) - View job queue (current/pending jobs and steps)
- [cbatch](cbatch.md) - Submit batch jobs
- [crun](crun.md) - Run interactive jobs and steps
- [ccancel](ccancel.md) - Cancel jobs and steps
- [ceff](ceff.md) - View job efficiency statistics
- [ccon](ccon.md) - Container job management
- [creport](creport.md) - Query job-related statistics