# cqueue - View Job Queue

cqueue displays information about jobs and steps in the queue. By default, it shows job information including pending,
running, and other states. Use the `-s/--step` option to query step information instead.

View all jobs in the cluster's queues:

```bash
cqueue
```

## Options

**-h, --help**

:   **Applies to:** `job`, `step`  
Display help information for cqueue command.

**-v, --version**

:   **Applies to:** `job`, `step`  
Display cqueue version information.

**-C, --config=&lt;path&gt;**

:   **Applies to:** `job`, `step`  
Path to configuration file. Default: "/etc/crane/config.yaml".

**-F, --full**

:   **Applies to:** `job`, `step`  
Display full content without truncation. By default, only 30 characters per cell are displayed.

**-N, --noheader**

:   **Applies to:** `job`, `step`  
Hide table headers in output.

**-m, --max-lines=&lt;number&gt;**

:   **Applies to:** `job`, `step`  
Specify maximum number of output lines. For example, `-m=500` limits output to 500 lines. By default, displays 100
entries.

**-i, --iterate=&lt;seconds&gt;**

:   **Applies to:** `job`, `step`  
Refresh query results at specified intervals (in seconds). For example, `-i=3` outputs results every 3 seconds.

**--json**

:   **Applies to:** `job`, `step`  
Output command execution results in JSON format instead of table format.

**-s, --step[=&lt;stepid1,stepid2,...&gt;]**

:   **Applies to:** `step`  
Query step information instead of job information. Accepts optional comma-separated list of step IDs in format
`jobid.stepid` (e.g., `123.1,123.2,456.3`). If no argument is provided, shows all steps. This option switches the query
mode from jobs to steps.

**-j, --job=&lt;jobid1,jobid2,...&gt;**

:   **Applies to:** `job`, `step`  
Specify job IDs to query (comma-separated list). For example, `-j=2,3,4`. When used with `--step`, filters steps
belonging to the specified jobs.

**-n, --name=&lt;name1,name2,...&gt;**

:   **Applies to:** `job`, `step`  
Specify job names to query (comma-separated list for multiple names).

**-t, --state=&lt;state&gt;**

:   **Applies to:** `job`, `step`  
Specify states to query. Valid values are 'pending(p)', 'running(r)' and 'all'. Default is 'all' (both pending and
running). For steps, valid states include 'running' and other step-specific states.

**-S, --start**

:   **Applies to:** `job`  
Display start time. For pending jobs, shows expected start time. For running jobs, shows actual start time.

**-u, --user=&lt;username1,username2,...&gt;**

:   **Applies to:** `job`, `step`  
Specify users to query (comma-separated list for multiple users). Filters jobs or steps by the specified usernames.

**-A, --account=&lt;account1,account2,...&gt;**

:   **Applies to:** `job`, `step`  
Specify accounts to query (comma-separated list for multiple accounts). Filters jobs or steps by the specified accounts.

**-p, --partition=&lt;partition1,partition2,...&gt;**

:   **Applies to:** `job`, `step`  
Specify partitions to query (comma-separated list for multiple partitions). Filters jobs or steps by the specified
partitions.

**-q, --qos=&lt;qos1,qos2,...&gt;**

:   **Applies to:** `job`, `step`  
Specify QoS to query (comma-separated list for multiple QoS). Filters jobs or steps by the specified Quality of Service
levels.

**--self**

:   **Applies to:** `job`, `step`  
View jobs or steps submitted by current user only.

**-o, --format=&lt;format_string&gt;**

:   **Applies to:** `job`, `step`  
Customize output format using format specifiers. Fields are identified by a percent sign (%) followed by a character.
Format specification syntax: `%[.]<size><type>`. Without size: field uses natural width. With size only (`%5j`): minimum
width, left-aligned. With dot and size (`%.5j`): minimum width, right-aligned. The available format specifiers differ
between job and step queries (see Format Specifiers sections below).

## Default Output Fields

When querying jobs (default mode), the following fields are displayed:

- **JobId**: Job identification number
- **Partition**: Partition where the job is running
- **Name**: Job name
- **User**: Username of job owner
- **Account**: Account charged for the job
- **Status**: Current job state (e.g., RUNNING, PENDING)
- **Type**: Job type (e.g., BATCH, INTERACTIVE)
- **TimeLimit**: Time limit for the job
- **Nodes**: Number of nodes allocated
- **NodeList**: Names of nodes where the job is running

When querying steps (using `--step`), the following fields are displayed:

- **StepId**: Step identification in format jobid.stepid
- **JobId**: Parent job identification number
- **Name**: Step name
- **Partition**: Partition (inherited from parent job)
- **User**: Username (inherited from parent job)
- **State**: Current step state
- **ElapsedTime**: Time elapsed since step started
- **NodeList**: Names of nodes where the step is running

## Job Format Specifiers

When querying jobs (default mode), the following format identifiers are supported (case-insensitive):

| Identifier | Full Name       | Description                                                 |
|------------|-----------------|-------------------------------------------------------------|
| %a         | Account         | Account associated with the job                             |
| %c         | AllocCpus       | CPUs allocated to the job                                   |
| %C         | ReqCpus         | Total CPUs requested by the job                             |
| %e         | ElapsedTime     | Elapsed time since job started                              |
| %h         | Held            | Hold state of the job                                       |
| %j         | JobID           | Job ID                                                      |
| %k         | Comment         | Comment of the job                                          |
| %l         | TimeLimit       | Time limit for the job                                      |
| %L         | NodeList        | List of nodes the job is running on (or reason for pending) |
| %m         | AllocMemPerNode | Allocated memory per node                                   |
| %M         | ReqMemPerNode   | Requested memory per node                                   |
| %n         | Name            | Job name                                                    |
| %N         | NodeNum         | Number of nodes requested by the job                        |
| %o         | Command         | Command line of the job                                     |
| %p         | Priority        | Priority of the job                                         |
| %P         | Partition       | Partition the job is running in                             |
| %q         | QoS             | Quality of Service level for the job                        |
| %Q         | ReqCpuPerNode   | Requested CPUs per node                                     |
| %r         | ReqNodes        | Requested nodes                                             |
| %R         | Reason          | Reason for pending status                                   |
| %s         | SubmitTime      | Submission time of the job                                  |
| %S         | StartTime       | Start time of the job                                       |
| %t         | State           | Current state of the job                                    |
| %T         | JobType         | Job type                                                    |
| %u         | User            | User who submitted the job                                  |
| %U         | Uid             | UID of the job                                              |
| %x         | ExcludeNodes    | Nodes excluded from the job                                 |
| %X         | Exclusive       | Exclusive status of the job                                 |

## Step Format Specifiers

When querying steps (using `--step`), the following format identifiers are supported (case-insensitive):

| Identifier | Full Name   | Description                                    |
|------------|-------------|------------------------------------------------|
| %i         | StepId      | Step ID in format jobid.stepid                 |
| %j         | JobId       | Parent job ID                                  |
| %n         | Name        | Step name                                      |
| %P         | Partition   | Partition (inherited from parent job)          |
| %u         | User        | Username (inherited from parent job)           |
| %U         | Uid         | User ID                                        |
| %e         | ElapsedTime | Elapsed time since step started                |
| %L         | NodeList    | List of nodes the step is running on           |
| %t         | State       | Current state of the step                      |
| %l         | TimeLimit   | Time limit for the step                        |
| %N         | NodeNum     | Number of nodes allocated to the step          |
| %a         | Account     | Account (inherited from parent job)            |
| %q         | QoS         | Quality of Service (inherited from parent job) |
| %o         | Command     | Command line of the step                       |

## Usage Examples

### Basic Job Queries

**View all jobs:**

```bash
cqueue
```

```text
[cranetest@crane01 ~]$ cqueue
JOBID  PARTITION  NAME      USER       ACCOUNT    STATUS   TYPE   TIME      TIMELIMIT  NODES  NODELIST/REASON
30685  CPU        Test_Job  cranetest  CraneTest  Pending  Batch  -         00:30:01   2      Priority
30686  CPU        Test_Job  cranetest  CraneTest  Pending  Batch  -         00:30:01   2      Priority
30687  CPU        Test_Job  cranetest  CraneTest  Pending  Batch  -         00:30:01   2      Priority
30683  CPU        Test_Job  cranetest  CraneTest  Running  Batch  00:01:33   00:30:01   2      crane[02-03]
30684  CPU        Test_Job  cranetest  CraneTest  Running  Batch  00:01:31   00:30:01   2      crane[02-03]
```

**Display help:**

```bash
cqueue -h
```

```text
[cranetest@crane01 ~]$ cqueue -h
Display the job information and queue status

Usage:
  cqueue [flags]

Flags:
  -A, --account string        Specify accounts to view (comma separated list), 
                              default is all accounts
  -C, --config string         Path to configuration file (default "/etc/crane/config.yaml")
  -o, --format string         Specify the output format.
                                Fields are identified by a percent sign (%) followed by a character or string.
                                Format specification: %[[.]size]type
                                  - Without size: field uses natural width
                                  - With size only (%5j): field uses minimum width, left-aligned (padding on right)
                                  - With dot and size (%.5j): field uses minimum width, right-aligned (padding on left)
                              
                              Supported format identifiers or string, string case insensitive:
                                %a/%Account            - Display the account associated with the job/step.
                                %C/%ReqCpus            - Display the cpus requested to the job. (For jobs only)
                                %c/%AllocCpus          - Display the cpus allocated to the job. (For jobs only)
                                %e/%ElapsedTime        - Display the elapsed time from the start of the job/step.
                                %h/%Held               - Display the hold state of the job. (For jobs only)
                                %i/%StepId             - Display the ID of the step (format: jobId.stepId). (For steps only)
                                %j/%JobID              - Display the ID of the job (or parent job ID for steps).
                                %k/%Comment            - Display the comment of the job. (For jobs only)
                                %K/%Wckey              - Display the wckey of the job.
                                %L/%NodeList           - Display the list of nodes the job/step is running on.
                                %l/%TimeLimit          - Display the time limit for the job/step.
                                %M/%ReqMemPerNode      - Display the requested mem per node of the job. (For jobs only)
                                %m/%AllocMemPerNode    - Display the requested mem per node of the job. (For jobs only)
                                %N/%NodeNum            - Display the number of nodes requested by the job/step.
                                %n/%Name               - Display the name of the job/step.
                                %o/%Command            - Display the command line of the job/step.
                                %P/%Partition          - Display the partition the job/step is running in.
                                %p/%Priority           - Display the priority of the job. (For jobs only)
                                %Q/%ReqCpuPerNode      - Display the requested cpu per node of the job. (For jobs only)
                                %q/%QoS                - Display the Quality of Service level for the job/step.
                                %R/%Reason             - Display the reason of pending. (For jobs only)
                                %r/%ReqNodes           - Display the reqnodes of the job. (For jobs only)
                                %S/%StartTime          - Display the start time of the job. (For jobs only)
                                %s/%SubmitTime         - Display the submission time of the job. (For jobs only)
                                %t/%State              - Display the current state of the job/step.
                                %T/%JobType            - Display the job type. (For jobs only)
                                %U/%Uid                - Display the uid of the job/step.
                                %u/%User               - Display the user who submitted the job/step.
                                %X/%Exclusive          - Display the exclusive status of the job. (For jobs only)
                                %x/%ExcludeNodes       - Display the exclude nodes of the job. (For jobs only)
                              
                              Examples:
                                --format "%j %n %t"              # Natural width for all fields
                                --format "%5j %20n %t"           # Left-aligned: JobID (min 5), Name (min 20), State
                                --format "%.5j %.20n %t"         # Right-aligned: JobID (min 5), Name (min 20), State
                                --format "ID:%8j | Name:%.15n"   # Mixed: left-aligned JobID, right-aligned Name with prefix
                              
                              Note: If the format is invalid or unrecognized, the program will terminate with an error message.
                              
  -F, --full                  Display full information (If not set, only display 30 characters per cell)
  -h, --help                  help for cqueue
  -i, --iterate uint          Display at specified intervals (seconds), default is 0 (no iteration)
  -j, --job string            Specify job ids to view (comma separated list), default is all
      --json                  Output in JSON format
  -L, --licenses string       Specify licenses to view (comma separated list), default is all licenses
  -m, --max-lines uint32      Limit the number of lines in the output, 0 means no limit (default 1000)
  -n, --name string           Specify job names to view (comma separated list), default is all
  -w, --nodelist string       Specify node names to view (comma separated list or patterns like node[1-10]), default is all
  -N, --noheader              Do not print header line in the output
  -p, --partition string      Specify partitions to view (comma separated list), 
                              default is all partitions
  -q, --qos string            Specify QoS of jobs to view (comma separated list), 
                              default is all QoS
      --self                  Display only the jobs submitted by current user
  -S, --start                 Display expected start time of pending jobs
  -t, --state string          Specify job states to view. Valid value are 'pending(p)', 'running(r)' and 'all'.
                              By default, 'all' is specified and all pending and running jobs will be reported (default "all")
  -s, --step string[="ALL"]   Specify step ids to view (comma separated list), default is all
      --type string           Specify task types to view (comma separated list), 
                              valid values are 'Interactive', 'Batch', 'Container', default is all types
  -u, --user string           Specify users to view (comma separated list), default is all users
  -v, --version               version for cqueue
  ```


**Hide table header:**

```bash
cqueue -N
```

```text
[cranetest@crane01 ~]$ cqueue -N
30685 CPU Test_Job cranetest CraneTest Pending Batch -         00:30:01 2 Priority
30686 CPU Test_Job cranetest CraneTest Pending Batch -         00:30:01 2 Priority
30687 CPU Test_Job cranetest CraneTest Pending Batch -         00:30:01 2 Priority
30683 CPU Test_Job cranetest CraneTest Running Batch 00:03:33 00:30:01 2 crane[02-03]
30684 CPU Test_Job cranetest CraneTest Running Batch 00:03:31 00:30:01 2 crane[02-03]
```

**Show start times:**

```bash
cqueue -S
```

```text
[cranetest@crane01 ~]$ cqueue -S
JOBID PARTITION NAME     USER       ACCOUNT    STATUS   TYPE   TIME      TIMELIMIT NODES NODELIST/REASON
30685 CPU       Test_Job cranetest CraneTest  Pending  Batch  -         00:30:01   2     Priority
30686 CPU       Test_Job cranetest CraneTest  Pending  Batch  -         00:30:01   2     Priority
30687 CPU       Test_Job cranetest CraneTest  Pending  Batch  -         00:30:01   2     Priority
30683 CPU       Test_Job cranetest CraneTest  Running  Batch  00:03:38   00:30:01   2     crane[02-03]
30684 CPU       Test_Job cranetest CraneTest  Running  Batch  00:03:36   00:30:01   2     crane[02-03]
```

### Filtering Jobs

**Query specific jobs:**

```bash
cqueue -j 30686,30687
```

```text
[cranetest@crane01 ~]$ cqueue -j 30686,30687
JOBID PARTITION NAME     USER       ACCOUNT    STATUS  TYPE  TIME TIMELIMIT NODES NODELIST/REASON
30686 CPU       Test_Job cranetest CraneTest  Pending Batch -    00:30:01   2     Priority
30687 CPU       Test_Job cranetest CraneTest  Pending Batch -    00:30:01   2     Priority
```

**Filter by state (pending jobs):**

```bash
cqueue -t Pending
```

```text
[cranetest@crane01 ~]$ cqueue -t Pending
JOBID PARTITION NAME     USER       ACCOUNT    STATUS  TYPE  TIME TIMELIMIT NODES NODELIST/REASON
30685 CPU       Test_Job cranetest CraneTest  Pending Batch -    00:30:01   2     Priority
30686 CPU       Test_Job cranetest CraneTest  Pending Batch -    00:30:01   2     Priority
30687 CPU       Test_Job cranetest CraneTest  Pending Batch -    00:30:01   2     Priority
```

**Filter by state (running jobs, shorthand):**

```bash
cqueue -t r
```

```text
[zhouhao@cranetest01 ~]$ cqueue -t r
JOBID PARTITION NAME        USER     ACCOUNT         STATUS   TYPE   TIME      TIMELIMIT NODES NODELIST/REASON
246   CPU       Test_Job_003 zhouhao  subAccountTest  Running  Batch  00:01:49   00:03:01   1     cranetest03
```

**Query jobs for specific user:**

```bash
cqueue -u cranetest
```

```text
[cranetest@crane01 ~]$ cqueue -u cranetest
JOBID PARTITION NAME     USER       ACCOUNT    STATUS   TYPE   TIME      TIMELIMIT NODES NODELIST/REASON
30685 CPU       Test_Job cranetest CraneTest  Pending  Batch  -         00:30:01   2     Priority
30686 CPU       Test_Job cranetest CraneTest  Pending  Batch  -         00:30:01   2     Priority
30687 CPU       Test_Job cranetest CraneTest  Pending  Batch  -         00:30:01   2     Priority
30683 CPU       Test_Job cranetest CraneTest  Running  Batch  00:04:11   00:30:01   2     crane[02-03]
30684 CPU       Test_Job cranetest CraneTest  Running  Batch  00:04:09   00:30:01   2     crane[02-03]
```

**Show only current user's jobs:**

```bash
cqueue --self
```

```text
[zhouhao@cranetest01 ~]$ cqueue --self
JOBID PARTITION NAME        USER     ACCOUNT         STATUS   TYPE   TIME      TIMELIMIT NODES NODELIST/REASON
246   CPU       Test_Job_003 zhouhao  subAccountTest  Running  Batch  00:01:12   00:03:01   1     cranetest03
```

**Query jobs for specific account:**

```bash
cqueue -A CraneTest
```

```text
[cranetest@crane01 ~]$ cqueue -A CraneTest
JOBID PARTITION NAME     USER       ACCOUNT    STATUS   TYPE   TIME      TIMELIMIT NODES NODELIST/REASON
30685 CPU       Test_Job cranetest CraneTest  Pending  Batch  -         00:30:01   2     Priority
30686 CPU       Test_Job cranetest CraneTest  Pending  Batch  -         00:30:01   2     Priority
30687 CPU       Test_Job cranetest CraneTest  Pending  Batch  -         00:30:01   2     Priority
30683 CPU       Test_Job cranetest CraneTest  Running  Batch  00:04:32   00:30:01   2     crane[02-03]
30684 CPU       Test_Job cranetest CraneTest  Running  Batch  00:04:30   00:30:01   2     crane[02-03]
```

**Filter by partition:**

```bash
cqueue -p CPU
```

```text
[cranetest@crane01 ~]$ cqueue -p CPU
JOBID PARTITION NAME     USER       ACCOUNT    STATUS   TYPE   TIME      TIMELIMIT NODES NODELIST/REASON
30685 CPU       Test_Job cranetest CraneTest  Pending  Batch  -         00:30:01   2     Priority
30686 CPU       Test_Job cranetest CraneTest  Pending  Batch  -         00:30:01   2     Priority
30687 CPU       Test_Job cranetest CraneTest  Pending  Batch  -         00:30:01   2     Priority
30683 CPU       Test_Job cranetest CraneTest  Running  Batch  00:04:50   00:30:01   2     crane[02-03]
30684 CPU       Test_Job cranetest CraneTest  Running  Batch  00:04:48   00:30:01   2     crane[02-03]
```

**Filter by job name:**

```bash
cqueue -n test
```

```text
[root@cranetest-rocky01 zhouhao]# cqueue -n test
JOBID   PARTITION   NAME   USER   ACCOUNT   STATUS   TYPE   TIME   TIMELIMIT   NODES   NODELIST/REASON
1276686 CPU         test   root   ROOT      Pending  Batch  -      00:03:01    1       Priority
```

**Filter by QoS:**

```bash
cqueue -q test_qos
```

```text
[zhouhao@cranetest01 ~]$ cqueue -q test_qos
JOBID PARTITION NAME        USER     ACCOUNT         STATUS   TYPE   TIME      TIMELIMIT NODES NODELIST/REASON QOS
246   CPU       Test_Job_003 zhouhao  subAccountTest  Running  Batch  00:02:40   00:03:01   1     cranetest03     test_qos
```

### Output Control

**Limit output to 3 lines:**

```bash
cqueue -m 3
```

```text
[cranetest@crane01 ~]$ cqueue -m 3
JOBID PARTITION NAME     USER       ACCOUNT    STATUS  TYPE  TIME TIMELIMIT NODES NODELIST/REASON
30685 CPU       Test_Job cranetest CraneTest  Pending Batch -    00:30:01   2     Priority
30686 CPU       Test_Job cranetest CraneTest  Pending Batch -    00:30:01   2     Priority
30687 CPU       Test_Job cranetest CraneTest  Pending Batch -    00:30:01   2     Priority
```

**Auto-refresh every 3 seconds:**

```bash
cqueue -i 3
```

```text
[cranetest@crane01 ~]$ cqueue -i 3
2024-07-24 11:39:02
JOBID PARTITION NAME     USER       ACCOUNT    STATUS   TYPE   TIME      TIMELIMIT NODES NODELIST/REASON
30685 CPU       Test_Job cranetest CraneTest  Pending  Batch  -         00:30:01   2     Priority
30686 CPU       Test_Job cranetest CraneTest  Pending  Batch  -         00:30:01   2     Priority
30687 CPU       Test_Job cranetest CraneTest  Pending  Batch  -         00:30:01   2     Priority
30683 CPU       Test_Job cranetest CraneTest  Running  Batch  00:04:36   00:30:01   2     crane[02-03]
30684 CPU       Test_Job cranetest CraneTest  Running  Batch  00:04:34   00:30:01   2     crane[02-03]

2024-07-24 11:39:05
JOBID PARTITION NAME     USER       ACCOUNT    STATUS   TYPE   TIME      TIMELIMIT NODES NODELIST/REASON
30685 CPU       Test_Job cranetest CraneTest  Pending  Batch  -         00:30:01   2     Priority
30686 CPU       Test_Job cranetest CraneTest  Pending  Batch  -         00:30:01   2     Priority
30687 CPU       Test_Job cranetest CraneTest  Pending  Batch  -         00:30:01   2     Priority
30683 CPU       Test_Job cranetest CraneTest  Running  Batch  00:04:39   00:30:01   2     crane[02-03]
30684 CPU       Test_Job cranetest CraneTest  Running  Batch  00:04:37   00:30:01   2     crane[02-03]

2024-07-24 11:39:08
JOBID PARTITION NAME     USER       ACCOUNT    STATUS   TYPE   TIME      TIMELIMIT NODES NODELIST/REASON
30685 CPU       Test_Job cranetest CraneTest  Pending  Batch  -         00:30:01   2     Priority
30686 CPU       Test_Job cranetest CraneTest  Pending  Batch  -         00:30:01   2     Priority
30687 CPU       Test_Job cranetest CraneTest  Pending  Batch  -         00:30:01   2     Priority
30683 CPU       Test_Job cranetest CraneTest  Running  Batch  00:04:42   00:30:01   2     crane[02-03]
30684 CPU       Test_Job cranetest CraneTest  Running  Batch  00:04:40   00:30:01   2     crane[02-03]
```

**JSON output:**

```bash
cqueue --json
```

### Custom Format Output

**Natural width for all fields:**

```bash
cqueue --format "%j %n %t"
```

**Left-aligned with minimum widths:**

```bash
cqueue --format "%5j %20n %t"
```

**Right-aligned with minimum widths:**

```bash
cqueue --format "%.5j %.20n %t"
```

**Mixed formatting with labels:**

```bash
cqueue --format "ID:%8j | Name:%.15n | State:%t"
```

**Complex custom format:**

```bash
cqueue -o="%n %u %.5j %.5t %.3T %.5T"
```

```text
[cranetest@crane01 ~]$ cqueue -o="%n %u %.5j %.5t %.3T %.5T"
NAME     USER       JOBID  STATU TYP TYPE
Test_Job cranetest  30685  Pendi Bat Batch
Test_Job cranetest  30686  Pendi Bat Batch
Test_Job cranetest  30687  Pendi Bat Batch
Test_Job cranetest  30683  Runni Bat Batch
Test_Job cranetest  30684  Runni Bat Batch
```

### Step Queries

**Query all steps:**

```bash
cqueue --step
```

**Query specific steps:**

```bash
cqueue --step 100.1,100.2,200.3
```

**Query steps for a specific job:**

```bash
cqueue --step -j 123
```

**Query steps with custom format:**

```bash
cqueue --step --format "%i %n %t %e %L"
```

!!! note "Querying Container Jobs"
    Container jobs (Pods) and container steps are displayed with type `Container`. Use `cacct --type Container` to filter container jobs, or use `cqueue --step -j <pod_job_id>` to view container job steps. For more container operations, see [ccon Command Manual](ccon.md).

**Query steps for specific user:**

```bash
cqueue --step -u username
```

**Right-aligned format for steps:**

```bash
cqueue --step --format "%.10i %.20n %.10t"
```

**Filter steps by state:**

```bash
cqueue --step -t running
```

**Query steps for multiple jobs:**

```bash
cqueue --step -j 100,200,300
```

## Related Commands

- [cbatch](cbatch.md) - Submit batch jobs
- [crun](crun.md) - Run interactive jobs and steps
- [calloc](calloc.md) - Allocate resources for interactive use
- [ccancel](ccancel.md) - Cancel jobs and steps
- [cacct](cacct.md) - Query completed jobs
- [ccon](ccon.md) - Container job management
- [ccontrol](ccontrol.md) - Control and query jobs/steps
- [creport](creport.md) - Query job-related statistics
