# cbatch - Submit Batch Jobs

**cbatch primarily passes a script describing the entire computation process to the job scheduling system, assigns a job ID, and waits for the scheduler to allocate resources and execute it.**

The CraneSched system requires users and accounts before submitting jobs. For adding users and accounts, see the [cacctmgr tutorial](cacctmgr.md).

## Quick Start

First, let's introduce a simple single-node job example:

The following job will request one node, one CPU core, and run `hostname` on the compute node before exiting:

```bash
#!/bin/bash
#CBATCH --ntasks-per-node 1
#CBATCH --nodes 1
#CBATCH -c 1
#CBATCH --mem 20M
#CBATCH --time 00:30:01
#CBATCH -o job.out
#CBATCH -p CPU
#CBATCH -J Test_Job

hostname
```

Assuming the job script is saved as `cbatch_test.sh`, submit it using:

```bash
cbatch cbatch_test.sh
```

**cbatch execution results:**

```bash
[cranetest@crane01 ~]$ cbatch cbatch_test.sh
Job id allocated: 30687
```

```bash hl_lines="5"
[cranetest@crane01 ~]$ cqueue -p CPU
JOBID PARTITION NAME     USER     ACCOUNT   STATUS TYPE TIME    TIMELIMIT NODES NODELIST/REASON
30685 CPU       Test_Job cranetest CraneTest Pending Batch -      00:30:01   1     Priority
30686 CPU       Test_Job cranetest CraneTest Pending Batch -      00:30:01   1     Priority
30687 CPU       Test_Job cranetest CraneTest Pending Batch -      00:30:01   1     Priority
30683 CPU       Test_Job cranetest CraneTest Running Batch 00:00:12 00:30:01   1     crane[02-03]
30684 CPU       Test_Job cranetest CraneTest Running Batch 00:00:10 00:30:01   1     crane[02-03]
```

## Command Line Options

### Resource Specifications
- **-N, --nodes uint32**: Number of nodes to run the job (default: 1)
- **-c, --cpus-per-task float**: Number of CPU cores required per task (default: 1)
- **--ntasks-per-node uint32**: Number of tasks to invoke on each node (default: 1)
- **--mem string**: Maximum amount of real memory. Supports GB (G, g), MB (M, m), KB (K, k) and Bytes (B), default unit is MB
- **--gres string**: Generic resources required per task, format: `gpu:a100:1` or `gpu:1`
- **--L, --licenses**: The licenses that the job requires to use, format: `lic1:2,lic2:4` or `lic1:2|lic2:4`

### Job Information
- **-J, --job-name string**: Name of the job
- **-A, --account string**: Account for job submission
- **-p, --partition string**: Requested partition
- **-q, --qos string**: Quality of Service (QoS) used for the job
- **-t, --time string**: Time limit, format: `[day-]hours:minutes:seconds` (e.g., `5-0:0:1` for 5 days and 1 second, or `10:1:2` for 10 hours, 1 minute, 2 seconds)
- **--deadline string**: Deadline, format:
    - `now + <count> <units>(无时默认为秒数)`
    - `HH:MM[:SS]`
    - `MM/DD[/YY]`
    - `MMDD[YY]`
    - `YYYY-MM-DD[THH:MM[:SS]]`
    - `midnight/noon/teatime/elevenses/fika`
- **--comment string**: Comment for the job

### Node Selection
- **-w, --nodelist string**: Nodes to be allocated to the job (comma separated list)
- **-x, --exclude string**: Exclude specific nodes from allocation (comma separated list)

### I/O Redirection
- **-o, --output string**: Redirect script standard output path
- **-e, --error string**: Redirect script error log path
- **--open-mode string**: Mode for opening output and error files. Supported values: `append`, `truncate` (default)

### Environment Variables
- **--get-user-env**: Load user's login environment variables
- **--export string**: Propagate environment variables

### Scheduling Options
- **--begin string**: Start time for the job. Format: `YYYY-MM-DDTHH:MM:SS`
- **-d, --dependency string**: Job dependency. Format: `<type>:<job_id>[+<delay>][:<job_id>][,<type>:<job_id>[:<job_id>]]` or `<type>:<job_id>[:<job_id>][?<type>:<job_id>[:<job_id>]]`. Supported types: `after`, `afterok`, `afternotok`, `afterany`. **Note**: For `<delay>`, use time with units (e.g., `10s`, `5m`, `2h`) - do NOT use `HH:MM:SS` format as `:` is the job ID separator. See [Job Dependency](../reference/job_dependency.md) for details
- **--exclusive**: Request exclusive node resources
- **-H, --hold**: Submit job in held state
- **-r, --reservation string**: Use reserved resources

### Email Notifications
- **--mail-type string**: Notify user by mail when certain events occur. Supported values: `NONE`, `BEGIN`, `END`, `FAIL`, `TIMELIMIT`, `ALL` (default: `NONE`)
- **--mail-user string**: Mail address of notification receiver

### Container Support

Container-related options are used to create Pod jobs that support container execution. For detailed usage, see [ccon Command Manual](ccon.md) and [Container Quick Start](../reference/container/quickstart.md).

- **--pod**: Enable container mode, creating the job as a Pod job. Once enabled, use `ccon run` within the script to start containers
- **--pod-name string**: Pod name (defaults to job name)
- **--pod-port string**: Pod port mapping, format: `HOST:CONTAINER` or `PORT`. Can be used multiple times
- **--pod-user string**: Run Pod as specified UID[:GID] (default: current user when `--pod-userns=false`)
- **--pod-userns**: Enable Pod user namespace (default: `true`, maps container user to root)
- **--pod-host-network**: Use host network namespace (default: `false`)
- **--pod-dns**: Set DNS servers for the Pod (IPv4 only). User-provided DNS servers are prepended (higher priority) before system defaults (default: `Container.Dns.Servers` in config file)

### Miscellaneous
- **--interpreter string**: Specify script interpreter (e.g., `/bin/bash`, `/usr/bin/python3`)
- **-D, --chdir string**: Working directory of the job
- **--extra-attr string**: Extra attributes of the job (JSON format)
- **--repeat uint32**: Submit job multiple times (default: 1)
- **--wrap string**: Wrap command string in a shell script and submit
- **--json**: Output in JSON format
- **-C, --config string**: Path to configuration file (default: `/etc/crane/config.yaml`)
- **-h, --help**: Display help information
- **-v, --version**: Display cbatch version
- **--signal**: Send signal to job

## Usage Examples

### Basic Job Submission

Submit a batch script:
```bash
cbatch cbatch_test.sh
```

```bash
[cranetest@crane01 ~]$ cbatch cbatch_test.sh
Job id allocated: 30725
```

### Help Information

Display help:
```bash
cbatch -h
```
```bash
[cranetest@crane01 ~]$ cbatch -h
Submit batch job

Usage:
  cbatch [flags] file

Flags:
  -A, --account string           Account used for the job
  -b, --begin string             Defer job until specified time.
  -D, --chdir string             Working directory of the job
      --comment string           Comment of the job
  -C, --config string            Path to configuration file (default "/etc/crane/config.yaml")
  -c, --cpus-per-task float      Number of cpus required per job (default 1)
  -d, --dependency string        Conditions for job to execute
  -e, --error string             Redirection path of standard error of the script
  -x, --exclude string           Exclude specific nodes from allocating (commas separated list)
      --exclusive                Exclusive node resources
      --export string            Propagate environment variables
      --extra-attr string        Extra attributes of the job (in JSON format)
      --get-user-env             Load login environment variables of the user
      --gpus-per-node string     Gpus required per node, format: [type:]<number>[,[type:]<number>...]. eg: "4" or "a100:1,volta:1"
      --gres string              Gres required per task,format: "gpu:a100:1" or "gpu:1"
  -h, --help                     help for cbatch
  -H, --hold                     Hold the job until it is released
      --interpreter string       Interpreter used to run the script
  -J, --job-name string          Name of job
      --json                     Output in JSON format
  -L, --licenses string          Licenses used for the job
      --mail-type string         Notify user by mail when certain events occur, supported values: NONE, BEGIN, END, FAIL, TIMELIMIT, ALL (default is NONE)
      --mail-user string         Mail address of the notification receiver
      --mem string               Maximum amount of real memory, support GB(G, g), MB(M, m), KB(K, k) and Bytes(B), default unit is MB
      --mem-per-cpu string       Maximum amount of real memory per CPU, support GB(G, g), MB(M, m), KB(K, k) and Bytes(B), default unit is MB
  -w, --nodelist string          Nodes to be allocated to the job (commas separated list)
  -N, --nodes uint32             Number of nodes on which to run (N = min[-max]) (default 1)
      --ntasks-per-node uint32   Number of tasks to invoke on each node (default 1)
      --open-mode string         Set the mode for opening output and error files, supported values: append, truncate (default is truncate) 
  -o, --output string            Redirection path of standard output of the script
  -p, --partition string         Partition requested
      --pod                      Submit as container enabled job (pod will be created)
      --pod-dns strings          Configure DNS server(s) for pod (comma-separated or repeated)
      --pod-host-network         Use host network namespace for the pod
      --pod-name string          Name of pod (defaults to job name)
      --pod-port strings         Publish pod port(s) in HOST:CONTAINER or PORT form
      --pod-user string          Run pod as UID[:GID] (default: current user when --pod-userns=false)
      --pod-userns               Enable pod user namespace (default true)
  -q, --qos string               QoS used for the job
      --repeat uint32            Submit the job multiple times (default 1)
  -r, --reservation string       Use reserved resources
  -s, --signal string            Send signal when time limit within time seconds, format: [{R|B}:]<sig_num>[@sig_time]
  -t, --time string              Time limit, format: "day-hours:minutes:seconds" 5-0:0:1 for 5 days, 1 second or "hours:minutes:seconds" 10:1:2 for 10 hours, 1 minute, 2 seconds
  -v, --version                  version for cbatch
      --wckey string             Wckey of the job
      --wrap string              Wrap command string in a sh script and submit
```

### Specify Account

Submit job with a specific account:
```bash
cbatch -A=acct-test cbatch_test.sh
```
```bash
[cranetest@crane01 ~]$ cbatch cbatch_test.sh
Job id allocated: 30726
```
```text
[cranetest@crane01 ~]$ cqueue -j=30726
JOBID PARTITION NAME     USER     ACCOUNT   STATUS TYPE    TIME      TIMELIMIT NODES NODELIST/REASON
30726 CPU       Test_Job cranetest acct-test Running Batch 00:01:23 00:30:01   2     crane[02-03]
```

### Node Exclusion

Exclude nodes from allocation:
```bash
cbatch -x crane01,crane02 cbatch_test.sh
```
```bash
[cranetest@crane01 ~]$ cbatch -x crane01,crane02 cbatch_test.sh
Job id allocated: 30727
```
```text
[cranetest@crane01 ~]$ cqueue -j=30727
JOBID PARTITION NAME     USER     ACCOUNT   STATUS TYPE    TIME      TIMELIMIT NODES NODELIST/REASON
30727 CPU       Test_Job cranetest CraneTest Running Batch 00:00:02 00:30:01   1     crane03
```

### Job Name

Specify job name:
```bash
cbatch -J testjob01 cbatch_test.sh
```
```bash
[cranetest@crane01 ~]$ cbatch -J testjob01 cbatch_test.sh
Job id allocated: 30728
```
```text
[cranetest@crane01 ~]$ cqueue -j=30728
JOBID PARTITION NAME     USER     ACCOUNT   STATUS TYPE    TIME      TIMELIMIT NODES NODELIST/REASON
30728 CPU       testjob01 cranetest CraneTest Running Batch 00:00:10 00:30:01   1     crane02
```

### Node Selection

Request specific nodes:
```bash
cbatch -w crane01,crane03 cbatch_test.sh
```
```bash
[cranetest@crane01 ~]$ cbatch -w crane01,crane03 cbatch_test.sh
Job id allocated: 30729
```
```text
[cranetest@crane01 ~]$ cqueue -j=30729
JOBID PARTITION NAME     USER     ACCOUNT   STATUS TYPE    TIME      TIMELIMIT NODES NODELIST/REASON
30729 CPU       Test_Job cranetest CraneTest Running Batch 00:00:04 00:30:01   1     crane03
```

### Partition Selection

Submit to specific partition:
```bash
cbatch -p GPU cbatch_test.sh
```
```bash
[cranetest@crane01 ~]$ cbatch -p GPU cbatch_test.sh
Job id allocated: 30730
```

```text
[cranetest@crane01 ~]$ cqueue -j=30730
JOBID PARTITION NAME     USER     ACCOUNT   STATUS TYPE    TIME      TIMELIMIT NODES NODELIST/REASON
30730 GPU       Test_Job cranetest CraneTest Running Batch 00:00:03 00:30:01   1     crane03
```

### Time Limit

Set time limit:
```bash
cbatch -t 00:25:25 cbatch_test.sh
```
```bash
[cranetest@crane01 ~]$ cbatch -t 00:25:25 cbatch_test.sh
Job id allocated: 30731
```

```text
[cranetest@crane01 ~]$ cqueue -j=30731
JOBID PARTITION NAME     USER     ACCOUNT   STATUS TYPE    TIME      TIMELIMIT NODES NODELIST/REASON
30731 CPU       Test_Job cranetest CraneTest Running Batch 00:00:06 00:25:25   1     crane02
```

### CPU Cores

Request specific number of CPU cores:
```bash
cbatch -c 2 cbatch_test.sh
```
```bash
[cranetest@crane01 ~]$ cbatch -c 2 cbatch_test.sh
Job id allocated: 30752
```
```text
[cranetest@crane01 ~]$ cqueue -j 30752
JOBID PARTITION NAME     USER     ACCOUNT   STATUS TYPE    TIME      TIMELIMIT NODES NODELIST/REASON
30752 CPU       Test_Job cranetest CraneTest Running Batch 00:02:40 00:30:01   1     crane02
```
```text
[cranetest@crane01 ~]$ ccontrol show node crane02
NodeName=crane02 State=alloc CPU=2.00 AllocCPU=2.00 FreeCPU=0.00
  RealMemory=2048M AllocMem=1024M FreeMem=1024M
  Partition=CPU RunningJob=1
```

### Memory Specification

Specify memory requirements:
```bash
cbatch --mem 123M cbatch_test.sh
```
```bash
[cranetest@crane01 ~]$ cbatch --mem 123M cbatch_test.sh
Job id allocated: 30755
```
```text
[cranetest@crane01 ~]$ cqueue -j 30755
JOBID PARTITION NAME     USER     ACCOUNT   STATUS TYPE    TIME      TIMELIMIT NODES NODELIST/REASON
30755 CPU       Test_Job cranetest CraneTest Running Batch 00:00:12 00:30:01   1     crane02
```
```text
[cranetest@crane01 ~]$ ccontrol show node crane02
NodeName=crane02 State=mix CPU=2.00 AllocCPU=1.00 FreeCPU=1.00
  RealMemory=2048M AllocMem=123M FreeMem=1925M
  Partition=CPU RunningJob=1
```

### Multi-node Jobs

Request multiple nodes with tasks per node:
```bash
cbatch -N 2 --ntasks-per-node 2 cbatch_test.sh
```
```bash
[cranetest@crane01 ~]$ cbatch -N 2 --ntasks-per-node 2 cbatch_test.sh
Job id allocated: 30756
```
```text
[cranetest@crane01 ~]$ cqueue -j 30756
JOBID PARTITION NAME     USER     ACCOUNT   STATUS TYPE    TIME      TIMELIMIT NODES NODELIST/REASON
30756 CPU       Test_Job cranetest CraneTest Running Batch 00:00:25 00:30:01   2     crane[02-03]
```
```text
[cranetest@crane01 ~]$ ccontrol show node
NodeName=crane02 State=alloc CPU=2.00 AllocCPU=2.00 FreeCPU=0.00
  RealMemory=2048M AllocMem=1024M FreeMem=1024M
  Partition=CPU RunningJob=1

NodeName=crane01 State=mix CPU=2.00 AllocCPU=1.00 FreeCPU=1.00
  RealMemory=2048M AllocMem=1024M FreeMem=1024M
  Partition=CPU RunningJob=1

NodeName=crane03 State=alloc CPU=2.00 AllocCPU=2.00 FreeCPU=0.00
  RealMemory=2048M AllocMem=1024M FreeMem=1024M
  Partition=GPU,CPU RunningJob=1
```

### Working Directory

Specify working directory:
```bash
cbatch -D /path test.sh
```
```text
[root@cranetest01 zhouhao]# cbatch -e test_error.log -D /nfs/home/zhouhao/test --export ALL cbatch_test.sh
Job id allocated: 196.

[root@cranetest01 zhouhao]# cqueue
JOBID PARTITION NAME     USER ACCOUNT STATUS TYPE    TIME      TIMELIMIT NODES NODELIST/REASON
196   CPU       Test_Job root ROOT    Running Batch 00:00:03 00:03:01   1     cranetest02
```

### Error Log

Redirect error output:
```bash
cbatch -e error.log test.sh
```
```text
[root@cranetest01 zhouhao]# cbatch -e test_error.log -D /nfs/home/zhouhao/test --export ALL cbatch_test.sh
Job id allocated: 196.

[root@cranetest01 zhouhao]# cqueue
JOBID PARTITION NAME     USER ACCOUNT STATUS TYPE    TIME      TIMELIMIT NODES NODELIST/REASON
196   CPU       Test_Job root ROOT    Running Batch 00:00:03 00:03:01   1     cranetest02
```

### Environment Variables

Export all environment variables:
```bash
cbatch --export ALL test.sh
```
```text
[root@cranetest01 zhouhao]# cbatch -e test_error.log -D /nfs/home/zhouhao/test --export ALL cbatch_test.sh
Job id allocated: 196.

[root@cranetest01 zhouhao]# cqueue
JOBID PARTITION NAME     USER ACCOUNT STATUS TYPE    TIME      TIMELIMIT NODES NODELIST/REASON
196   CPU       Test_Job root ROOT    Running Batch 00:00:03 00:03:01   1     cranetest02
```

### User Environment

Load user's login environment:
```bash
cbatch --get-user-env test.sh
```
```text
[root@cranetest01 zhouhao]# cbatch --get-user-env -N 1 --ntasks-per-node 2 cbatch_test.sh
Job id allocated: 154.

[root@cranetest01 zhouhao]# cqueue
JOBID PARTITION NAME     USER ACCOUNT STATUS TYPE    TIME      TIMELIMIT NODES NODELIST/REASON
154   CPU       Test_Job root ROOT    Running Batch 00:00:04 00:03:01   1     cranetest02
```

### Output Redirection

Redirect standard output:
```bash
cbatch -o output.out test.sh
```

```bash
[root@cranetest01 zhouhao]# cbatch -o test_out.out cbatch_test.sh
Job id allocated: 176.
```

### Quality of Service

Specify QoS:
```bash
cbatch -q qos_test test.sh
```

```text
[root@cranetest01 zhouhao]# cbatch -q test_qos cbatch_test.sh
Job id allocated: 177.

[root@cranetest01 zhouhao]# cqueue
JOBID PARTITION NAME     USER ACCOUNT STATUS TYPE    TIME      TIMELIMIT NODES NODELIST/REASON
177   CPU       Test_Job root ROOT    Running Batch 00:00:14 00:03:01   1     cranetest02
```

### Repeat Submission

Submit job multiple times:
```bash
cbatch --repeat 3 test.sh
```
```text
[root@cranetest01 zhouhao]# cbatch --repeat 3 cbatch_test.sh
Job id allocated: 175, 174, 173.

[root@cranetest01 zhouhao]# cqueue
JOBID PARTITION NAME     USER ACCOUNT STATUS TYPE    TIME      TIMELIMIT NODES NODELIST/REASON
173   CPU       Test_Job root ROOT    Running Batch 00:00:02 00:03:01   1     cranetest02
175   CPU       Test_Job root ROOT    Running Batch 00:00:02 00:03:01   1     cranetest04
174   CPU       Test_Job root ROOT    Running Batch 00:00:02 00:03:01   1     cranetest03
```

## Environment Variables

Common environment variables available in batch scripts:

| Variable | Description |
|----------|-------------|
| **CRANE_JOB_NODELIST** | List of allocated nodes |
| **%j** | Job ID (for use in file patterns) |

## Multi-node Parallel Jobs

Here's an example of submitting a multi-node, multi-core job:

The following job runs on three nodes, using 4 CPU cores per node:

```bash
#!/bin/bash
#CBATCH -o crane_test%j.out
#CBATCH -p CPU
#CBATCH -J "crane_test"
#CBATCH --nodes 3
#CBATCH --ntasks-per-node 4
#CBATCH -c 4
#CBATCH --time 50:00:00

# Generate machine file from allocated nodes
echo "$CRANE_JOB_NODELIST" | tr ";" "\n" > crane.hosts

# Load MPI runtime environment
module load mpich/4.0 

# Execute cross-node parallel task
mpirun -n 13 -machinefile crane.hosts helloWorld > log
```

## Advanced Features

### Delayed Start

Schedule a job to start at a specific time:
```bash
cbatch --begin 2024-12-31T23:00:00 my_script.sh
```

### Held Jobs

Submit a job in held state:
```bash
cbatch --hold my_script.sh
```

Use `ccontrol release <job_id>` to release held jobs.

### Email Notifications

Receive email notifications:
```bash
cbatch --mail-type=ALL --mail-user=user@example.com my_script.sh
```

### JSON Output

Get submission result in JSON format:
```bash
cbatch --json my_script.sh
```

### Wrap Command

Submit a simple command without creating a script file:
```bash
cbatch --wrap "echo Hello && sleep 10 && echo Done"
```

### Signal
When a job is within sig_time seconds of its end time, the system will send it the signal sig_num. sig_num can be either a signal number or name (e.g., "10" or "USR1"). sig_time must be an integer between 0 and 65535. By default, no signal is sent before the job's end time. If only sig_num is specified without sig_time, the default lead time is 60 seconds.

By default, all job steps except for the batch shell itself will receive the signal. Using the "B:" option will send the signal only to the batch shell, and other processes will not receive the signal.

```bash
Send signals to jobs
# Send SIGUSR1 signal 60 seconds before timelimit, all steps except the batch process will receive this signal
cbatch --signal=SIGUSR1@60 my_script.sh

# Send SIGUSR1 signal 60 seconds before timelimit, only the batch process will receive the signal
cbatch --signal=B:SIGUSR1@60 my_script.sh
```

### Container Jobs

Use the `--pod` option to create Pod jobs that support containers:

```bash
#!/bin/bash
#CBATCH --pod
#CBATCH -N 2
#CBATCH -c 4
#CBATCH --mem 8G
#CBATCH -p GPU
#CBATCH --gres gpu:1
#CBATCH -J container_training

# Start containers within the Pod
ccon run -d -v /data:/data pytorch/pytorch:latest -- python /data/train.py

# Wait for all containers to complete
ccon wait
```

Or specify container options via command line:

```bash
cbatch --pod --pod-name my-training --pod-host-network train_job.sh
```

For more container usage examples, see [Container Usage Examples](../reference/container/examples.md).

## See Also

- [cqueue](cqueue.md) - View job queue
- [ccancel](ccancel.md) - Cancel jobs
- [cacct](cacct.md) - View job accounting information
- [ccontrol](ccontrol.md) - Control jobs and system resources
- [ccon](ccon.md) - Container job management
- [Container Overview](../reference/container/index.md) - Container feature introduction
- [creport](creport.md) - Query job-related statistics