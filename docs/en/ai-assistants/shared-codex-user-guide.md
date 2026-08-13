# Using Shared Codex

Shared Codex can help you write CraneSched job scripts, understand job states, and troubleshoot
common problems from command output. The administrator has configured the model service and the
CraneSched Skill on the shared node, so you do not need the shared API key.

## Start Codex

Log in to the node specified by your administrator, replace the angle-bracket placeholder with the
real path, enter your working directory, and run:

```bash
cd <your_working_directory>
codex
```

Describe your goal in natural language. Include the program entry point, resource requirements, job
ID, complete error, and relevant command output when available. If `codex` is not found or cannot
connect to the model service, contact the cluster administrator; do not try to read or change the
system Codex configuration.

The conversations below are illustrative and do not come from a real cluster. Actual responses vary
with your input, site policy, and installed version.

## Example 1: Create a job script

**You:**

> Write a CraneSched batch script for `train.py` using one GPU, four CPUs, 16 GB of memory, and a
> two-hour time limit. I do not know this site's GPU partition name yet.

**Codex (example response):**

> Keep the partition as a placeholder until you confirm it in the site documentation:
>
> ```bash
> #!/bin/bash
> #CBATCH --job-name=train
> #CBATCH --partition=<gpu_partition>
> #CBATCH --nodes=1
> #CBATCH --ntasks-per-node=1
> #CBATCH --cpus-per-task=4
> #CBATCH --mem=16G
> #CBATCH --gres=gpu:1
> #CBATCH --time=02:00:00
> #CBATCH --output=train_%j.out
> #CBATCH --error=train_%j.err
>
> python train.py
> ```
>
> Save it as `train.sh`, check the shell syntax with `bash -n train.sh`, then run
> `cbatch train.sh`. Add an account, QoS, or environment module only when required by your site;
> do not guess those values.

## Example 2: Understand a pending job

**You:**

> Job 4812 has remained `Pending`, and the `NODELIST/REASON` column from `cqueue -j 4812 -F`
> shows `Resource`. Did submission fail?

**Codex (example response):**

> This does not mean submission failed. The confirmed facts are that the job remains queued and its
> current highest-priority blocker concerns available resources. That line alone cannot predict an
> exact start time. Inspect your own job details with:
>
> ```bash
> ccontrol show job 4812
> ```
>
> Check whether the requested GPUs, node count, memory, and time limit match your intent. If the
> reason persists, give the administrator the job ID, submission time, resource request, and this
> output rather than repeatedly resubmitting the same job.

## Example 3: Troubleshoot a failed job

**You:**

> `cacct -j 4930 -F` shows `State=Failed` and `ExitCode=1:0`. The first error in stderr is
> `ModuleNotFoundError: No module named 'torch'`. What should I check first?

**Codex (example response):**

> `ExitCode=1:0` means the application returned exit code 1, with no evidence of signal termination.
> Together with the first error, the likely issue is that the batch environment did not load the
> Python environment containing `torch`. Check whether the script explicitly loads the site's
> recommended module or activates a virtual environment on shared storage, for example:
>
> ```bash
> # Choose one approach using the site's real environment
> module load <python_or_ai_module>
> # or
> source <shared_venv>/bin/activate
> python -c 'import torch; print(torch.__version__)'
> python train.py
> ```
>
> Do not guess the module name or environment path. If you provide the current script and site
> environment instructions, I can place the initialization steps correctly. Do not include passwords,
> tokens, or a complete environment dump.

## Boundaries

- Review commands and scripts before running them; site documentation and the installed command's `--help` take precedence.
- Share only relevant output. Never provide passwords, API keys, private keys, or a complete environment dump.
- Codex gains no additional CraneSched permissions and cannot confirm site partitions, accounts, QoS, or cluster state on an administrator's behalf.
- Before submitting or cancelling a job or changing a file, confirm that the target is yours and the effect is intended.
