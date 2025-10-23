# Use Case: Parameter Sweep with Job Arrays

**Scenario**: Run the same analysis with different parameters (e.g., hyperparameter tuning).

Job arrays make it easy to explore multiple configurations without submitting many individual jobs.

## Step 1: Create the Array Job Script

Create `param_sweep.sh`:

```bash
#!/bin/bash
#CBATCH --job-name=param_sweep
#CBATCH --partition=CPU
#CBATCH --nodes=1
#CBATCH --cpus-per-task=2
#CBATCH --mem=4G
#CBATCH --time=0:30:00
#CBATCH --output=sweep_%j.out
#CBATCH --repeat=10

# Define parameter array
learning_rates=(0.001 0.005 0.01 0.05 0.1 0.5 1.0 2.0 5.0 10.0)

# Get parameter for this task
# Note: Job array implementation may vary
param_index=$(( ($CRANE_JOB_ID % 10) ))
learning_rate=${learning_rates[$param_index]}

echo "Testing with learning_rate=$learning_rate"

# Run your program with the parameter
python train.py --learning-rate $learning_rate --output results_lr_${learning_rate}.txt

echo "Completed test for learning_rate=$learning_rate"
```

## Step 2: Submit the Array Job

```bash
cbatch --repeat 10 param_sweep.sh
```

Each array index receives its own learning rate configuration.

## Step 3: Monitor All Array Jobs

```bash
# View all jobs in the array
cqueue --self -n param_sweep
```

## Tips

- Use `--output` and `--error` formats that include `%j` (job ID) to avoid file collisions.
- Collect results into a shared directory and record the parameter value inside each result file.
- For large sweeps, consider staggering submissions or adding dependencies to control concurrency.
