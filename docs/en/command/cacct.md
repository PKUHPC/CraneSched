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

**%deadline/%Deadline**

：Display job deadline
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
Specify the maximum number of output results. For example, `-m=500` limits output to 500 lines. Default: 1000 lines.

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

```text
[cranetest@crane01 ~]$ cacct
JOBID   JOBNAME   PARTITION ACCOUNT ALLOCCPUS STATE     EXITCODE
222112  Test_Job  CPU       PKU     1.00     Completed 0:0
222111  Test_Job  CPU       PKU     1.00     Completed 0:0
222110  Test_Job  CPU       PKU     1.00     Completed 0:0
222109  Test_Job  CPU       PKU     1.00     Completed 0:0
222108  Test_Job  CPU       PKU     1.00     Completed 0:0
222107  Test_Job  CPU       PKU     1.00     Completed 0:0
222106  Test_Job  CPU       PKU     1.00     Completed 0:0
222105  Test_Job  CPU       PKU     1.00     Completed 0:0
222104  Test_Job  CPU       PKU     1.00     Completed 0:0
222103  Test_Job  CPU       PKU     1.00     Completed 0:0
```

**Display help:**

```bash
cacct -h
```
```text
[cranetest@crane01 ~]$ cacct -h
Display the recent job information

Usage:
  cacct [flags]

Flags:
  -A, --account string       Select accounts to view (comma separated list)
  -C, --config string        Path to configuration file (default "/etc/crane/config.yaml")
  -E, --end-time string      Filter jobs with an end time within a certain time period, which can use closed intervals(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41) or semi open intervals(timeFormat: 2024-01-02T15:04:05~ or ~2024-01-11T11:12:41)
  -o, --format string        Specify the output format.
                             
                             Fields are identified by a percent sign (%) followed by a character or string.
                             Format specification: %[[.]size]type
                               - Without size: field uses natural width
                               - With size only (%5j): field uses minimum width, left-aligned (padding on right)
                               - With dot and size (%.5j): field uses minimum width, right-aligned (padding on left)
                             
                             Supported format identifiers or string, string case insensitive:
                                %a/%Account           - Display the account associated with the job.
                                %C/%ReqCpus           - Display the number of requested CPUs, formatted to two decimal places
                                %c/%AllocCpus         - Display the number of allocated CPUs, formatted to two decimal places.
                                %D/%ElapsedTime       - Display the elapsed time from the start of the job.
                                %E/%EndTime           - Display the end time of the job.
                                %e/%ExitCode          - Display the exit code of the job. 
                                                          If the exit code is based on a specific base (e.g., kCraneExitCodeBase),
                                                          it formats as "0:<code>" or "<code>:0" based on the condition.
                                %h/%Held              - Display the hold status of the job.
                                %j/%JobID             - Display the ID of the job.
                                %K/%Wckey             - Display the wckey of the job.
                                %k/%Comment           - Display the comment of the job.
                                %L/%NodeList          - Display the list of nodes the job is running on.
                                %l/%TimeLimit         - Display the time limit of the job.
                                %M/%ReqMemPerNode     - Display the requested mem per node of the job.
                                %m/%AllocMemPerNode   - Display the allocted mem per node of the job.
                                %N/%NodeNum           - Display the node num of the job.
                                %n/%JobName           - Display the name of the job.
                                %P/%Partition         - Display the partition associated with the job.
                                %p/%Priority          - Display the priority of the job.
                                %q/%Qos               - Display the QoS of the job.
                                %R/%Reason            - Display the reason of pending.
                                %r/%ReqNodes          - Display the reqnodes of the job.
                                %S/%StartTime         - Display the start time of the job.
                                %s/%SubmitTime        - Display the submit time num of the job.
                                %t/%State             - Display the state of the job.
                                %T/%JobType           - Display the job type.
                                %U/%UserName          - Display the username of the job.
                                %u/%Uid               - Display the uid of the job.
                                %x/%ExcludeNodes      - Display the excludenodes of the job.
                                %X/%Exclusive         - Display the exclusive status of the job.
                             
                             Examples:
                               --format "%j %n %t"              # Natural width for all fields
                               --format "%5j %20n %t"           # Left-aligned: JobID (min 5), JobName (min 20), State
                               --format "%.5j %.20n %t"         # Right-aligned: JobID (min 5), JobName (min 20), State
                               --format "ID:%8j | Name:%.15n"   # Mixed: left-aligned JobID, right-aligned JobName with prefix
                             
                             Note: If the format is invalid or unrecognized, the program will terminate with an error message.
                             
  -F, --full                 Display full information (If not set, only display 30 characters per cell)
  -h, --help                 help for cacct
  -j, --job string           Select job ids to view (comma separated list), default is all
      --json                 Output in JSON format
  -m, --max-lines uint32     Limit the number of lines in the output, 0 means no limit (default 1000)
  -n, --name string          Select job names to view (comma separated list), default is all
  -w, --nodelist string      Specify node names to view (comma separated list or patterns like node[1-10]), default is all
  -N, --noheader             Do not print header line in the output
  -p, --partition string     Specify partitions to view (comma separated list), default is all
  -q, --qos string           Specify QoS of jobs to view (comma separated list), default is all.
  -S, --start-time string    Filter jobs with a start time within a certain time period, which can use closed intervals(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41) or semi open intervals(timeFormat: 2024-01-02T15:04:05~ or ~2024-01-11T11:12:41)
  -t, --state string         Specify job states to view, supported states: pending(p), running(r), completed(c), failed(f), cancelled(x), time-limit-exceeded(t), all. (default "all")
  -s, --submit-time string   Filter jobs with a submit time within a certain time period, which can use closed intervals(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41) or semi open intervals(timeFormat: 2024-01-02T15:04:05~ or ~2024-01-11T11:12:41)
      --type string          Specify task types to view (comma separated list), 
                             valid values are 'Interactive', 'Batch', 'Container', default is all types
  -u, --user string          Select users to view (comma separated list)
  -v, --version              version for cacct
```


**Hide table header:**

```bash
cacct -N
```

```text
[cranetest@crane01 ~]$ cacct -N
222112 Test_Job  CPU PKU     1.00 Completed   0:0
222111 Test_Job  CPU PKU     1.00 Completed   0:0
222110 Test_Job  CPU PKU     1.00 Completed   0:0
222109 Test_Job  CPU PKU     1.00 Completed   0:0
222108 Test_Job  CPU PKU     1.00 Completed   0:0
222107 Test_Job  CPU PKU     1.00 Completed   0:0
222106 Test_Job  CPU PKU     1.00 Completed   0:0
222105 Test_Job  CPU PKU     1.00 Completed   0:0
222104 Test_Job  CPU PKU     1.00 Completed   0:0
222103 Test_Job  CPU PKU     1.00 Completed   0:0
222102 Test_Job  CPU PKU     1.00 Completed   0:0
222101 Test_Job  CPU PKU     1.00 Completed   0:0
222100 Test_Job  CPU PKU     1.00 Completed   0:0
222099 Test_Job  CPU PKU     1.00 Completed   0:0
```


### Filter by ID and Name

**Query specific job IDs:**

```bash
cacct -j=30618,30619,30620
```

```text
[cranetest@crane01 ~]$ cacct -j=30618,30619,30620
JOBID JOBNAME  PARTITION ACCOUNT ALLOCCPUS STATE          EXITCODE
30620 Test_Job CPU       ROOT    0.00     Cancelled      0:0
30619 Test_Job CPU       ROOT    2.00     ExceedTimeLimit 0:15
30618 Test_Job CPU       ROOT    2.00     Cancelled      0:15
```

**Query by job name:**

```bash
cacct -n=Test_Job
```

```text
[cranetest@crane01 ~]$ cacct -n=Test_Job
JOBID   JOBNAME   PARTITION ACCOUNT ALLOCCPUS STATE     EXITCODE
222112  Test_Job  CPU       PKU     1.00     Completed 0:0
222111  Test_Job  CPU       PKU     1.00     Completed 0:0
222110  Test_Job  CPU       PKU     1.00     Completed 0:0
222109  Test_Job  CPU       PKU     1.00     Completed 0:0
222108  Test_Job  CPU       PKU     1.00     Completed 0:0
222107  Test_Job  CPU       PKU     1.00     Completed 0:0
222106  Test_Job  CPU       PKU     1.00     Completed 0:0
222105  Test_Job  CPU       PKU     1.00     Completed 0:0
222104  Test_Job  CPU       PKU     1.00     Completed 0:0
222103  Test_Job  CPU       PKU     1.00     Completed 0:0
222102  Test_Job  CPU       PKU     1.00     Completed 0:0
222101  Test_Job  CPU       PKU     1.00     Completed 0:0
222100  Test_Job  CPU       PKU     1.00     Completed 0:0
```

**Query by name pattern:**

```bash
cacct -n test
```

```text
[root@cranetest-rocky01 zhouhao]# cacct -n test
JOBID     JOBNAME PARTITION ACCOUNT ALLOCCPUS STATE     EXITCODE
1276686  test    CPU       ROOT    0.00     Cancelled 0:0
```

### Filter by User and Account

**Query jobs by user:**

```bash
cacct -u=cranetest
```

```text
[cranetest@crane01 ~]$ cacct -u=cranetest
JOBID   JOBNAME     PARTITION ACCOUNT   ALLOCCPUS STATE          EXITCODE
30689   Interactive CPU       CraneTest 2.00     Completed      0:0
30688   Interactive CPU       CraneTest 2.00     Completed      0:64
30687   Test_Job    CPU       CraneTest 0.00     Cancelled      0:0
30686   Test_Job    CPU       CraneTest 2.00     Cancelled      0:15
30685   Test_Job    CPU       CraneTest 2.00     ExceedTimeLimit 0:15
30684   Test_Job    CPU       CraneTest 2.00     Cancelled      0:15
30683   Test_Job    CPU       CraneTest 2.00     Cancelled      0:15
```

**Query jobs by account:**

```bash
cacct -A=CraneTest
```

```text
[cranetest@crane01 ~]$ cacct -A=CraneTest
JOBID   JOBNAME     PARTITION ACCOUNT   ALLOCCPUS STATE          EXITCODE
30689   Interactive CPU       CraneTest 2.00     Completed      0:0
30688   Interactive CPU       CraneTest 2.00     Completed      0:64
30687   Test_Job    CPU       CraneTest 0.00     Cancelled      0:0
30686   Test_Job    CPU       CraneTest 2.00     Cancelled      0:15
30685   Test_Job    CPU       CraneTest 2.00     ExceedTimeLimit 0:15
30684   Test_Job    CPU       CraneTest 2.00     Cancelled      0:15
30683   Test_Job    CPU       CraneTest 2.00     Cancelled      0:15
```

**Combine account and max lines:**

```bash
cacct -A ROOT -m 10
```

```text
[zhouhao@cranetest-rocky02 ~]$ cacct -A ROOT -m 10
JOBID     JOBNAME   PARTITION ACCOUNT ALLOCCPUS STATE    EXITCODE
1418574   CPU       CPU       ROOT    0.00     Pending  0:0
1418573   CPU       CPU       ROOT    0.00     Pending  0:0
1418572   CPU       CPU       ROOT    0.00     Pending  0:0
1418571   CPU       CPU       ROOT    0.00     Pending  0:0
1418570   CPU       CPU       ROOT    0.00     Pending  0:0
1418569   CPU       CPU       ROOT    0.00     Pending  0:0
1418568   CPU       CPU       ROOT    0.00     Pending  0:0
1418567   CPU       CPU       ROOT    0.00     Pending  0:0
1418566   CPU       CPU       ROOT    0.00     Pending  0:0
1412536   Test_Job  CPU       ROOT    0.00     Pending  0:0
```


### Filter by Partition and QoS

**Query jobs in a specific partition:**

```bash
cacct -p GPU
```

```text
[cranetest@crane01 ~]$ cacct -p GPU
JOBID   JOBNAME     PARTITION ACCOUNT   ALLOCCPUS STATE          EXITCODE
30736   Interactive GPU       acct-test 2.00     Completed      0:64
30735   Interactive GPU       acct-test 0.00     Cancelled      0:0
30734   Interactive GPU       acct-test 0.00     Cancelled      0:0
30733   Interactive GPU       acct-test 2.00     Completed      0:64
30730   Test_Job    GPU       CraneTest 1.00     ExceedTimeLimit 0:15
30691   Interactive GPU       yanyan    1.00     Completed      0:64
30690   Interactive GPU       yanyan    1.00     Completed      0:64
21670   Test_Job    GPU       ROOT      1.00     Completed      0:0
21669   Test_Job    GPU       ROOT      1.00     Completed      0:0
21668   Test_Job    GPU       ROOT      1.00     Completed      0:0
21667   Test_Job    GPU       ROOT      1.00     Completed      0:0
21666   Test_Job    GPU       ROOT      1.00     Completed      0:0
```


**Query by QoS:**

```bash
cacct -q test_qos
```

```text
[root@cranetest01 zhouhao]# cacct -q test_qos
JOBID JOBNAME     PARTITION ACCOUNT ALLOCCPUS STATE          EXITCODE
231              CPU       ROOT    2.00     ExceedTimeLimit 0:15
230              CPU       ROOT    2.00     Cancelled      0:15
229              CPU       ROOT    2.00     ExceedTimeLimit 0:15
217              CPU       ROOT    2.00     Completed      0:0
216              CPU       ROOT    2.00     Completed      0:0
188  test_crun    computing ROOT    2.00     Completed      0:0
185  test_calloc  CPU       ROOT    8.00     Completed      0:64
183  Interactive  CPU       ROOT    2.00     Completed      0:64
182  Interactive  CPU       ROOT    2.00     Completed      0:64
179  Test_Job     CPU       ROOT    4.00     Completed      0:0
178  Test_Job     CPU       ROOT    4.00     Completed      0:0
177  Test_Job     CPU       ROOT    1.00     Completed      0:0
```

### Time Range Filtering

**Filter by start time range:**

```bash
cacct -S=2024-07-22T10:00:00~2024-07-24T10:00:00
```

```text
[cranetest@crane01 ~]$ cacct -S=2024-07-22T10:00:00~2024-07-24T10:00:00
JOBID   JOBNAME     PARTITION ACCOUNT ALLOCCPUS STATE          EXITCODE STARTTIME
30680   Test_Job    CPU       yanyan  2.00     ExceedTimeLimit 0:15   2024-07-24 09:48:27 +0800 CST
30679   Test_Job    CPU       yanyan  2.00     ExceedTimeLimit 0:15   2024-07-24 09:47:51 +0800 CST
30678   Interactive CPU       yanyan  2.00     Completed      0:0    2024-07-24 09:45:23 +0800 CST
30677   Interactive CPU       yanyan  2.00     Completed      0:64   2024-07-24 09:44:03 +0800 CST
30674   Test_Job    CPU       yanyan  2.00     Cancelled      0:15   2024-07-24 09:38:30 +0800 CST
30673   Test_Job    CPU       yanyan  2.00     Cancelled      0:15   2024-07-24 09:38:29 +0800 CST
30672   Test_Job    CPU       yanyan  2.00     Failed         0:66   2024-07-24 09:38:29 +0800 CST
30671   Test_Job    CPU       yanyan  2.00     Cancelled      0:15   2024-07-24 09:24:56 +0800 CST
30670   Test_Job    CPU       yanyan  2.00     Cancelled      0:15   2024-07-24 09:24:54 +0800 CST
30669   Interactive CPU       ROOT    1.00     Completed      0:0    2024-07-23 14:43:20 +0800 CST
30668   Interactive CPU       ROOT    2.00     Completed      0:64   2024-07-23 14:43:12 +0800 CST
30667   Interactive CPU       ROOT    1.00     Completed      0:0    2024-07-23 14:42:26 +0800 CST
30666   Interactive CPU       ROOT    1.00     Completed      0:0    2024-07-23 14:42:22 +0800 CST
30665   Interactive CPU       ROOT    1.00     Completed      0:0    2024-07-23 14:42:14 +0800 CST
30664   Interactive CPU       ROOT    2.00     Completed      0:64   2024-07-23 14:42:06 +0800 CST
30663   Interactive CPU       ROOT    1.00     Completed      0:0    2024-07-23 14:41:28 +0800 CST
30662   Interactive CPU       ROOT    1.00     Completed      0:0    2024-07-23 14:41:27 +0800 CST
30661   Interactive CPU       ROOT    1.00     Completed      0:0    2024-07-23 14:41:19 +0800 CST
```


**Filter by end time range:**

```bash
cacct -E=2024-07-22T10:00:00~2024-07-24T10:00:00
```

```text
[cranetest@crane01 ~]$ cacct -E=2024-07-22T10:00:00~2024-07-24T10:00:00
JOBID   JOBNAME     PARTITION ACCOUNT ALLOCCPUS STATE     EXITCODE ENDTIME
30678   Interactive CPU       yanyan  2.00     Completed 0:0    2024-07-24 09:45:42 +0800 CST
30677   Interactive CPU       yanyan  2.00     Completed 0:64   2024-07-24 09:44:10 +0800 CST
30676   Interactive CPU       yanyan  0.00     Cancelled 0:0    2024-07-24 09:27:25 +0800 CST
30675   Test_Job    CPU       yanyan  0.00     Cancelled 0:0    2024-07-24 09:38:38 +0800 CST
30674   Test_Job    CPU       yanyan  2.00     Cancelled 0:15   2024-07-24 09:38:11 +0800 CST
30673   Test_Job    CPU       yanyan  2.00     Cancelled 0:15   2024-07-24 09:39:57 +0800 CST
30672   Test_Job    CPU       yanyan  2.00     Failed    0:66   2024-07-24 09:38:29 +0800 CST
30671   Test_Job    CPU       yanyan  2.00     Cancelled 0:15   2024-07-24 09:38:28 +0800 CST
30670   Test_Job    CPU       yanyan  2.00     Cancelled 0:15   2024-07-24 09:38:28 +0800 CST
30669   Interactive CPU       ROOT    1.00     Completed 0:0    2024-07-23 14:43:21 +0800 CST
30668   Interactive CPU       ROOT    2.00     Completed 0:64   2024-07-23 16:54:40 +0800 CST
30667   Interactive CPU       ROOT    1.00     Completed 0:0    2024-07-23 14:42:27 +0800 CST
30666   Interactive CPU       ROOT    1.00     Completed 0:0    2024-07-23 14:42:23 +0800 CST
30665   Interactive CPU       ROOT    1.00     Completed 0:0    2024-07-23 14:42:15 +0800 CST
```


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

```text
[cranetest@crane01 ~]$ cacct -m=10
JOBID   JOBNAME   PARTITION ACCOUNT ALLOCCPUS STATE     EXITCODE
222112  Test_Job  CPU       PKU     1.00     Completed 0:0
222111  Test_Job  CPU       PKU     1.00     Completed 0:0
222110  Test_Job  CPU       PKU     1.00     Completed 0:0
222109  Test_Job  CPU       PKU     1.00     Completed 0:0
222108  Test_Job  CPU       PKU     1.00     Completed 0:0
222107  Test_Job  CPU       PKU     1.00     Completed 0:0
222106  Test_Job  CPU       PKU     1.00     Completed 0:0
222105  Test_Job  CPU       PKU     1.00     Completed 0:0
222104  Test_Job  CPU       PKU     1.00     Completed 0:0
222103  Test_Job  CPU       PKU     1.00     Completed 0:0
```


**JSON output:**

```bash
cacct --json -j 12345
```

### Custom Format Output

**Specify custom output format:**

```bash
cacct -o="%j %.10n %P %a %t"
```

```text
[cranetest@crane01 ~]$ cacct -o="%j %.10n %P %a %t"
JOBID   JOBNAME     PARTITION ACCOUNT STATE
30558   Test_Jo     CPU       PKU     Completed
30571   Test_Jo     CPU       PKU     Completed
30570   Test_Jo     CPU       PKU     Completed
30569   Test_Jo     CPU       PKU     Completed
30568   Test_Jo     CPU       PKU     Completed
30567   Test_Jo     CPU       PKU     Completed
30566   Test_Jo     CPU       PKU     Completed
30565   Test_Jo     CPU       PKU     Completed
30564   Test_Jo     CPU       PKU     Completed
30563   Test_Jo     CPU       PKU     Completed
30562   Test_Jo     CPU       PKU     Completed
```


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

```text
[root@cranetest-rocky01 zhouhao]# cacct -m 10 -j 783925,783889 -t=c -F
JOBID   JOBNAME PARTITION ACCOUNT ALLOCCPUS STATE     EXITCODE
783925          CPU       ROOT    1.00     Completed 0:0
783889          CPU       ROOT    1.00     Completed 0:0
```


**Complex combined query:**

```bash
cacct -m 10 -E=2024-10-08T10:00:00~2024-10-10T10:00:00 -p CPU -t c
```

```text
[root@cranetest-rocky01 zhouhao]# cacct -m 10 -E=2024-10-08T10:00:00~2024-10-10T10:00:00 -p CPU -t=c
JOBID     JOBNAME PARTITION ACCOUNT ALLOCCPUS STATE     EXITCODE ENDTIME
783446            CPU       ROOT    1.00     Completed 0:0    2024-10-10 09:55:47 +0800 CST
783433            CPU       ROOT    1.00     Completed 0:0    2024-10-10 09:57:10 +0800 CST
783309            CPU       ROOT    1.00     Completed 0:0    2024-10-10 09:29:07 +0800 CST
783272            CPU       ROOT    1.00     Completed 0:0    2024-10-10 07:08:05 +0800 CST
783267            CPU       ROOT    1.00     Completed 0:0    2024-10-10 07:55:35 +0800 CST
783249            CPU       ROOT    1.00     Completed 0:0    2024-10-10 09:58:43 +0800 CST
783192            CPU       ROOT    2.00     Completed 0:0    2024-10-10 09:34:04 +0800 CST
783180            CPU       ROOT    1.00     Completed 0:0    2024-10-10 09:34:58 +0800 CST
783177            CPU       ROOT    1.00     Completed 0:0    2024-10-10 09:29:34 +0800 CST
783176            CPU       ROOT    2.00     Completed 0:0    2024-10-10 09:59:51 +0800 CST
```


## Related Commands

- [cqueue](cqueue.md) - View job queue (current/pending jobs and steps)
- [cbatch](cbatch.md) - Submit batch jobs
- [crun](crun.md) - Run interactive jobs and steps
- [ccancel](ccancel.md) - Cancel jobs and steps
- [ceff](ceff.md) - View job efficiency statistics
- [ccon](ccon.md) - Container job management
- [creport](creport.md) - Query job-related statistics