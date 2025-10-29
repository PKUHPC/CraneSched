# Use Cases

This guide focuses on the most common everyday workflows in CraneSched and walks through each scenario from job submission to result retrieval. For specialized or large-scale workloads, follow the advanced guides linked in the **Next Steps** section.

---

## Use Case 1: Basic Batch Job

**Scenario**: Run a simple computational task on a single node.

### Task Description

You have a script that processes data and outputs results to a file. This is the most common use case for batch processing.

### Step 1: Prepare Your Script

Create a file named `simple_job.sh`:

```bash
#!/bin/bash
#CBATCH --job-name=simple_test
#CBATCH --partition=CPU
#CBATCH --nodes=1
#CBATCH --ntasks-per-node=1
#CBATCH --cpus-per-task=1
#CBATCH --mem=100M
#CBATCH --time=0:05:00
#CBATCH --output=job_%j.out

# Print job information
echo "Job started at: $(date)"
echo "Running on node: $(hostname)"
echo "Job ID: $CRANE_JOB_ID"

# Simulate some computational work
echo "Processing data..."
sleep 5
echo "Data processing complete!"

# Print job completion
echo "Job finished at: $(date)"
```

### Step 2: Submit the Job

```bash
cbatch simple_job.sh
```

Expected output:
```
Job <12345> is submitted to partition <CPU>
```

### Step 3: Check Job Status

```bash
cqueue -j 12345
```

or check all your jobs:

```bash
cqueue --self
```

### Step 4: View Results

After the job completes, check the output file:

```bash
cat job_12345.out
```

---

## Use Case 2: Interactive Session

**Scenario**: You need to interactively test code or debug an application on a compute node.

### Task Description

Instead of submitting a batch job, you want direct command-line access to a compute node with specific resources.

### Step 1: Request an Interactive Session

```bash
crun -p CPU -N 1 -c 2 --mem 500M -t 1:00:00 /bin/bash
```

This allocates:
- 1 node from the CPU partition
- 2 CPU cores
- 500 MB memory
- 1 hour time limit

### Step 2: Work Interactively

Once connected, you'll see a shell prompt on the compute node:

```bash
# Check where you are
hostname

# Check allocated resources
echo $CRANE_JOB_NODELIST

# Run your commands
python test_script.py
./my_program --input data.txt

# When done, exit
exit
```

Resources are released automatically when you exit.

---

## Common Patterns

### Check Job History

After jobs complete, use `cacct` to view job statistics:

```bash
# View your recent jobs
cacct --user $USER --starttime $(date -d "7 days ago" +%Y-%m-%d)

# View specific job details
cacct --job <job_id>
```

### Cancel Jobs

```bash
# Cancel a specific job
ccancel <job_id>

# Cancel all your pending jobs
ccancel --state=PENDING --user=$USER
```

### Resource Efficiency

Check how efficiently your job used allocated resources:

```bash
ceff <job_id>
```

---

## Tips and Best Practices

1. **Start Small**: Test your job with minimal resources first, then scale up
2. **Set Accurate Time Limits**: Overestimating wastes resources, underestimating kills jobs
3. **Use Job Names**: Makes it easier to identify jobs in the queue
4. **Monitor Resource Usage**: Use `ceff` after job completion to optimize future submissions
5. **Error Handling**: Always specify separate `--error` file to catch stderr messages
6. **Checkpointing**: For long jobs, implement checkpointing to resume from failures

---

## Next Steps

- Explore advanced scenarios:
	- [Multi-node parallel job](use_case_multi_node.md)
	- [GPU-accelerated training](use_case_gpu.md)
	- [Parameter sweep with job arrays](use_case_param_sweep.md)
	- [Chained jobs with dependencies](use_case_pipeline.md)
- Review the [command reference](../command/cbatch.md) for detailed options
- Learn about [cluster configuration](../deployment/configuration/config.md)
- Check [exit codes](exit_code.md) when debugging job failures
