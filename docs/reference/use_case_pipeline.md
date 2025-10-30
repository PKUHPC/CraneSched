# Use Case: Chained Jobs with Dependencies

**Scenario**: Run a pipeline where each step depends on the previous step completing successfully.

Dependencies help enforce ordering between multi-stage workloads such as preprocess → train → evaluate.

## Step 1: Submit the First Job

```bash
# Preprocessing job
job1=$(cbatch preprocess.sh | grep -oP '\d+')
echo "Preprocessing job: $job1"
```

## Step 2: Submit Dependent Jobs

```bash
# Training job (waits for preprocessing)
# Note: Dependency implementation may vary by version
job2=$(cbatch --dependency=afterok:$job1 train.sh | grep -oP '\d+')
echo "Training job: $job2"

# Evaluation job (waits for training)
job3=$(cbatch --dependency=afterok:$job2 evaluate.sh | grep -oP '\d+')
echo "Evaluation job: $job3"
```

## Step 3: Monitor the Pipeline

```bash
# Watch all three jobs
cqueue -j $job1,$job2,$job3
```

## Tips

- Combine dependency types (`after`, `afterok`, `afterany`) to express both strict and best-effort sequencing.
- Store the job IDs in a file so that downstream automation (CI/CD, notebooks) can check job states.
- If a later stage depends on artifacts produced by earlier jobs, write results to shared storage accessible across nodes.
