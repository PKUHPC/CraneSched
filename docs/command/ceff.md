# ceff - View Job Efficiency Statistics

**ceff displays real-time information about job execution.**

View real-time task status:

```bash
ceff <job_id[,job_id,...]>
```

**ceff Output Example**

![ceff](../images/ceff/ceff.png)

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
- **CPU Efficiency**: CPU usage efficiency, typically expressed as the percentage of actual CPU time used versus allocated core wall-clock time
- **Job Wall-clock time**: Wall-clock time of the job, i.e., total time from start to end
- **Memory Utilized**: Actual memory used by the job
- **Memory Efficiency**: Memory usage efficiency, typically expressed as the percentage of actual memory used versus allocated memory

## Main Options

- **-h/--help**: Display help
- **-C/--config string**: Path to configuration file (default: "/etc/crane/config.yaml")
- **--json**: Output task information returned by backend in JSON format
- **-v/--version**: Display ceff version

## Usage Examples

**Display help:**
```bash
ceff -h
```

![ceff](../images/ceff/h.png)

**Query job efficiency:**
```bash
ceff <job_id>
```

**Query multiple jobs:**
```bash
# Query multiple job IDs separated by commas
ceff 1234,1235,1236
```

**JSON output:**
```bash
ceff <job_id> --json
```

![ceff](../images/ceff/json.png)

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
