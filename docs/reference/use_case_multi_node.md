# Use Case: Multi-Node Parallel Job

**Scenario**: Run an MPI application across multiple nodes for parallel computation.

Multi-node jobs are ideal for tightly coupled workloads such as MPI programs. This guide assumes you already have an MPI toolchain available on the cluster nodes.

## Step 1: Prepare the MPI Job Script

Create `mpi_job.sh`:

```bash
#!/bin/bash
#CBATCH --job-name=mpi_test
#CBATCH --partition=CPU
#CBATCH --nodes=3
#CBATCH --ntasks-per-node=4
#CBATCH --cpus-per-task=1
#CBATCH --mem=2G
#CBATCH --time=1:00:00
#CBATCH --output=mpi_job_%j.out

# Load MPI module (adjust according to your environment)
module load mpich/4.0

# Generate machine file from allocated nodes
echo "$CRANE_JOB_NODELIST" | tr ";" "\n" > machinefile

# Display allocated nodes
echo "Running on nodes:"
cat machinefile

# Run MPI application
# This will use 12 processes total (3 nodes × 4 tasks per node)
mpirun -n 12 -machinefile machinefile ./my_mpi_program

echo "MPI job completed at $(date)"
```

## Step 2: Compile the MPI Program (if needed)

```bash
mpicc -o my_mpi_program my_mpi_program.c
```

## Step 3: Submit the Job

```bash
cbatch mpi_job.sh
```

## Step 4: Monitor Progress

```bash
# Check job status
cqueue -j <job_id>

# View real-time output (while job is running)
tail -f mpi_job_<job_id>.out
```

## Tips

- Double-check that the requested tasks per node align with your application design.
- Keep the `machinefile` for post-run diagnostics; it records the actual nodes used.
- Combine this workflow with `ceff` to evaluate CPU efficiency after completion.
