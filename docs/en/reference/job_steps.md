# Job Steps

**Job steps are sub-execution units within a job that allow multiple execution phases or concurrent tasks within a single job allocation.**

## What are Steps?

A step is a distinct execution unit that runs within the context of a parent job. Instead of allocating resources for each individual task, you can allocate resources once (as a job) and then run multiple steps within that allocation. This approach is more efficient and provides better control over resource utilization.

### Key Characteristics

- **Resource Sharing**: Steps share the resources allocated to the parent job
- **Independent Execution**: Each step can have its own command, resource requirements, and lifecycle
- **Flexible Scheduling**: Steps can run sequentially or in parallel within the job's resource limits
- **Simplified Management**: Steps automatically inherit job properties like partition, account, and QoS

---

## Step Types

CraneSched distinguishes between three types of steps:

### 1. DAEMON Step (step_id=0)

- **Purpose**: Resource holder for the job lifetime
- **Lifecycle**: Lives for the entire duration of the job
- **Execution**: Does not execute user code
- **Creation**: Automatically created by the system for every job
- **Use Case**: Internal resource management and job coordination

### 2. PRIMARY Step (step_id=1)

- **Purpose**: Main execution unit of the job
- **Lifecycle**: Runs the primary job task
- **Execution**: Executes the command/script specified at job submission
- **Creation**: Automatically created for every job
- **Use Case**: For batch jobs, runs the submitted script; for interactive jobs, runs the user shell

### 3. COMMON Step (step_id≥2)

- **Purpose**: Additional interactive steps
- **Lifecycle**: Can be launched dynamically during job execution
- **Execution**: Runs user-specified commands
- **Creation**: Created by users via `crun` within a job allocation
- **Use Case**: Running multiple parallel or sequential tasks within a job

---

## Step ID Format

Steps are identified using a two-part format:

**Format**: `jobid.stepid`

**Examples**:
- `123.0` - DAEMON step of job 123
- `123.1` - PRIMARY step of job 123
- `123.2` - First COMMON step of job 123
- `456.3` - Second COMMON step of job 456

This format is used consistently across all CraneSched commands that support step operations.

---

## Step Lifecycle

Steps progress through the following states:

```
PENDING → RUNNING → COMPLETED
               ↓
         FAILED/CANCELLED
```

### Step States

- **PENDING**: Step is waiting for resources within the parent job
- **RUNNING**: Step is actively executing
- **COMPLETED**: Step finished successfully
- **FAILED**: Step terminated with an error
- **CANCELLED**: Step was cancelled by user or system

---

## Step vs Job

| Aspect | Job | Step |
|--------|-----|------|
| Resource Allocation | Allocates resources from cluster | Uses resources from parent job |
| Partition | Must specify partition | Inherits from parent job |
| Account | Must specify account | Inherits from parent job |
| QoS | Must specify QoS | Inherits from parent job |
| ID Format | Integer (e.g., 123) | jobid.stepid (e.g., 123.2) |
| Creation | via cbatch, crun, calloc | via crun within job allocation |
| Independence | Fully independent | Depends on parent job |
| Lifecycle | Independent lifetime | Cannot outlive parent job |

---

## When to Use Steps

### Use Steps When:

1. **Running Multiple Tasks in Sequence**
   - Pre-processing → Computation → Post-processing workflows
   - Each task needs different resource configurations
   - You want to keep all tasks under one job allocation

2. **Parallel Task Execution**
   - Running multiple independent computations simultaneously
   - Testing different parameters or configurations in parallel
   - Resource-efficient parallel workflows

3. **Interactive Computing Sessions**
   - Allocating resources once with `calloc`
   - Running various tools and commands interactively
   - Dynamically launching tasks as needed

4. **Resource Efficiency**
   - Reducing scheduler overhead by using one job allocation
   - Maximizing utilization of allocated resources
   - Avoiding repeated allocation/deallocation cycles

### Use Separate Jobs When:

- Tasks have completely different resource requirements (partition, account, QoS)
- Tasks need to run at different times or have different priorities
- Tasks should be billed to different accounts
- Maximum isolation between tasks is required

---

## Usage Examples

### Example 1: Basic Step Workflow

```bash
# Allocate resources for interactive use
calloc -N 2 -c 8 --mem 4G -t 2:00:00

# Now CRANE_JOB_ID is set, and crun will create steps
# Run preprocessing step
crun -N 1 -c 4 ./preprocess.sh input.dat

# Run multiple computation steps in parallel
crun -N 1 -c 8 ./compute.sh part1 &
crun -N 1 -c 8 ./compute.sh part2 &
wait

# Run postprocessing step
crun -N 2 -c 4 ./postprocess.sh results
```

### Example 2: Monitoring Steps

```bash
# In another terminal, monitor the steps
cqueue --step -j 12345

# View detailed information about a specific step
ccontrol show step 12345.2

# Check step status with custom format
cqueue --step --format "%i %n %t %e %L"
```

### Example 3: Managing Steps

```bash
# Cancel a specific step (others continue running)
ccancel 12345.2

# Cancel multiple steps
ccancel 12345.2,12345.3

# Cancel all steps (entire job)
ccancel 12345
```

### Example 4: Step-Aware Scripts

```bash
#!/bin/bash
# script.sh - Detects if running as a step

if [ -n "$CRANE_STEP_ID" ]; then
    echo "Running as step $CRANE_STEP_ID of job $CRANE_JOB_ID"
    echo "Resources allocated to this step"
else
    echo "Running as standalone job $CRANE_JOB_ID"
    echo "Primary job execution"
fi

# Run computation
./my_program
```

### Example 5: Parameter Sweep with Steps

```bash
# Allocate resources once
calloc -N 4 -c 16 -t 4:00:00

# Run parameter sweep as parallel steps
for param in 0.1 0.5 1.0 2.0 5.0; do
    crun -N 1 -c 4 ./simulate --param=$param &
done
wait

# Collect results
crun -N 1 ./collect_results.sh
```

---

## Best Practices

1. **Allocate Sufficient Resources**: Ensure the job allocation has enough resources for all planned steps

2. **Monitor Step Progress**: Use `cqueue --step` to track step execution and resource usage

3. **Handle Step Failures**: Implement error handling in scripts to manage step failures gracefully

4. **Resource Planning**: Plan step resource requirements to maximize parent job utilization

5. **Use Descriptive Names**: Name your steps clearly for easier monitoring and debugging

6. **Clean Up**: Cancel unwanted steps explicitly rather than waiting for them to timeout

7. **Log Management**: Direct step output to separate log files for easier debugging

---

## Limitations and Considerations

- **Resource Constraints**: Steps cannot request more resources than available in the parent job
- **Job Dependency**: Steps cannot outlive the parent job
- **No Property Override**: Steps cannot change partition, account, or QoS from parent job
- **Scheduler Overhead**: Very large numbers of short-lived steps may create scheduling overhead
- **Time Limits**: Steps must complete within the parent job's time limit

---

## Related Commands

- [crun](../command/crun.md) - Run interactive tasks and create steps
- [calloc](../command/calloc.md) - Allocate resources for step execution
- [cqueue](../command/cqueue.md) - Query step information with `--step` flag
- [ccancel](../command/ccancel.md) - Cancel steps
- [ccontrol](../command/ccontrol.md) - Control and query step details
