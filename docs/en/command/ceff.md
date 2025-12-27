# ceff - View Job Efficiency Statistics

**ceff displays real-time information about job execution.**

View real-time task status:

```bash
$ ceff <job_id[,job_id,...]>
```

**ceff Output Example**

```bash
$ ceff 125
JobId: 125
Qos: test_qos
User/Group: root(0)/root(0)
Account: PKU
JobState: Running
Cores: 1.00
CPU Utilized: 00:00:00
CPU Efficiency: 0.38% of 00:00:26 core-walltime
Job Wall-clock time: 00:00:26
Memory Utilized: 0.59 MB (estimated maximum)
Memory Efficiency: 0.06% of 1024.00 MB (1024.00 MB/node)
WARNING: Efficiency statistics may be misleading for RUNNING jobs.
```

## Main Output Fields

- **JobId**: Unique identifier for the job
- **Qos**: QoS level where the job is running
- **User/Group**: User and user group that submitted the job
- **Account**: Account name
- **State**: Current state of the job (e.g., COMPLETED, FAILED, CANCELLED, etc.)
- **Cores**: Number of cores used by the job
- **Nodes**: Number of nodes allocated to the job
- **Cores per node**: Number of cores allocated per node
- **CPU Utilized**: Actual CPU time used by the job
- **CPU Efficiency**: CPU usage efficiency, typically expressed as the percentage of actual CPU time used versus
  allocated core wall-clock time
- **Job Wall-clock time**: Wall-clock time of the job, i.e., total time from start to end
- **Memory Utilized**: Actual memory used by the job
- **Memory Efficiency**: Memory usage efficiency, typically expressed as the percentage of actual memory used versus
  allocated memory

## Main Options

- **-h/--help**: Display help
- **-C/--config string**: Path to configuration file (default: "/etc/crane/config.yaml")
- **--json**: Output task information returned by backend in JSON format
- **-v/--version**: Display ceff version

## Usage Examples

**Query job efficiency:**

```bash
$ ceff <job_id>
```

**Query multiple jobs:**

```bash
# Query multiple job IDs separated by commas
$ ceff 1234,1235,1236
```

**JSON output:**

```bash
$ ceff 125 --json
{
    "job_id": 125,
    "qos": "test_qos",
    "uid": 0,
    "user_name": "root",
    "gid": 0,
    "group_name": "root",
    "account": "PKU",
    "job_state": 2,
    "nodes": 1,
    "cores_per_node": 1,
    "cpu_utilized_str": "00:00:00",
    "cpu_efficiency": 0.099378559,
    "run_time_str": "00:01:40",
    "total_mem_mb": 0.59375,
    "mem_efficiency": 0.0579833984375,
    "total_malloc_mem_mb": 1024,
    "malloc_mem_mb_per_node": 1024
}
```

## Understanding Efficiency Metrics

### CPU Efficiency

CPU Efficiency indicates how effectively your job utilized the allocated CPU resources:

- **High efficiency (>80%)**: Job is CPU-intensive and makes good use of allocated cores
- **Medium efficiency (40-80%)**: Job may have I/O wait times or is not fully parallelized
- **Low efficiency (<40%)**: Job may be I/O-bound, waiting on resources, or not suitable for parallel execution

### Memory Efficiency

Memory Efficiency shows how much of the allocated memory was actually used:

- **High efficiency (>80%)**: Memory allocation closely matches actual usage
- **Low efficiency (<40%)**: Consider requesting less memory for future jobs to improve resource utilization

## Use Cases

1. **Post-job Analysis**: Review completed jobs to understand resource utilization
2. **Resource Optimization**: Identify over- or under-allocated resources for future job submissions
3. **Troubleshooting**: Investigate why jobs failed or underperformed
4. **Reporting**: Generate efficiency reports for project or user resource usage

## Related Commands

- [cacct](cacct.md) - Query accounting information for completed jobs
- [cqueue](cqueue.md) - View active jobs in the queue
- [cbatch](cbatch.md) - Submit batch jobs
- [crun](crun.md) - Run interactive jobs
