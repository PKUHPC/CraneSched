# Using Shared Codex

Shared Codex can help you write CraneSched job scripts, query your jobs, explain scheduler states,
and troubleshoot problems using command output and files in your working directory. The
administrator has configured the model service and CraneSched Skill, so you do not need the shared
API key.

## Start Codex

Log in to the node specified by your administrator, replace the angle-bracket placeholder with the
real path, enter your working directory, and run:

```bash
cd <your_working_directory>
codex
```

Describe your goal directly. A job ID, program entry point, and expected behavior are usually enough;
Codex can query missing information about jobs visible to your account.

## How the Agent works

By default, the cluster installation allows Codex to run these read-only commands without asking for
approval each time:

- `cqueue` for the active queue and your current jobs;
- `cacct` for job history, states, and exit codes;
- `ccontrol show job` / `ccontrol show step` for job and step details.

The commands still run as you, and CraneSched server permissions determine what they can see. A site
or user may configure stricter rules. Codex should prefer a specific job ID or your own jobs and
avoid continuous refreshes or broad queries.

Codex can also find scripts, logs, and existing environments in the current working directory and
run local checks. These operations remain subject to the Codex sandbox, file permissions, and site
policy; depending on the active configuration, the CLI may prompt before an operation that needs
additional access.

Writing a script, running `cbatch`, cancelling a job, or changing job state has side effects and is
not covered by the default read-only list. For such a task, ask Codex to explain the proposed action
and impact and wait for your explicit confirmation. Whether the CLI also displays a built-in approval
prompt depends on the active site and user configuration. You can refuse or revise the plan.

The conversations below are illustrative rather than results from a real cluster. Command output and
site names exist only to demonstrate the workflow.

## Example 1: Explain a basic state

**You:**

> My job is `Running`. Does that mean training succeeded?

**Codex (example response):**

> `Running` only means that resources are allocated and the job is executing. It does not prove that
> training is producing correct results. Check stdout, stderr, and application metrics, then use the
> final `State` and `ExitCode` from `cacct` after the job ends.

## Example 2: Draft a script

**You:**

> Draft a script for `train.py`. This site's values are partition `gpu-a100`, account `ml`, and QoS
> `normal`; request one A100, four CPUs, 16 GB of memory, and two hours. Do not write or submit yet.

**Codex (example response):**

> I will provide a draft in the conversation without changing a file or submitting a job:
>
> ```bash
> #!/bin/bash
> #CBATCH --job-name=train
> #CBATCH --partition=gpu-a100
> #CBATCH --account=ml
> #CBATCH --qos=normal
> #CBATCH --nodes=1
> #CBATCH --ntasks-per-node=1
> #CBATCH --cpus-per-task=4
> #CBATCH --mem=16G
> #CBATCH --gres=gpu:a100:1
> #CBATCH --time=02:00:00
> #CBATCH --output=train_%j.out
> #CBATCH --error=train_%j.err
>
> python train.py
> ```

## Example 3: Query active jobs

**You:**

> Check which of my current jobs need attention.

**Codex runs read-only tools:**

```bash
cqueue --self -o="%j %P %n %t %l %N %L"
ccontrol show job 4812
```

**Relevant tool output (example):**

```text
JOBID PARTITION NAME       STATUS  TIMELIMIT NODES NODELIST(REASON)
4812  gpu-a100  train      Pending 02:00:00  1     (Resource)
4815  cpu       preprocess Running 01:00:00  1     cn032

JobId=4812 JobState=Pending Account=ml Qos=normal Partition=gpu-a100
Reason=Resource TimeLimit=02:00:00 ReqRes:node=1 cpu=4 mem=16G gres=gpu:a100:1
```

**Codex (example response):**

> You have two active jobs:
>
> - `4815` is running on `cn032` in the `cpu` partition, with no scheduler error shown.
> - `4812` was accepted but is waiting for resources that satisfy the complete displayed request,
>   including one A100, four CPUs, and 16 GB. Its current reason is `Resource`, not a submission
>   failure. The details also confirm account `ml`, QoS `normal`, and a two-hour time limit.
>
> Do not resubmit it now. `Resource` describes current availability for the complete request; it does
> not provide a guaranteed start time.

## Example 4: Combine history, details, and files to diagnose a failure

**You:**

> My job 4930 failed. Investigate the cause for me.

**Codex queries job data and inspects the working directory:**

```bash
cacct -j 4930 -F -o "%j %t %e %P %a"
pwd
rg --files | rg '4930|train.*\.(out|err)$'
sed -n '1,160p' train.sh
sed -n '1,120p' train_4930.err
rg -n 'venvs/.*/bin/python|source .*/bin/activate' --glob '*.sh'
test -x /home/alice/venvs/torch/bin/python
```

**Relevant results, grouped for readability (example):**

```text
JOBID STATE  EXITCODE PARTITION ACCOUNT
4930  Failed 1:0      gpu-a100  ml

/home/alice/project
train_4930.err
```

```bash
#!/bin/bash
#CBATCH --job-name=train
#CBATCH --partition=gpu-a100
#CBATCH --output=train_%j.out
#CBATCH --error=train_%j.err

python train.py
```

```text
ModuleNotFoundError: No module named 'torch'
tools/check_gpu.sh:8:/home/alice/venvs/torch/bin/python -c 'import torch; print(torch.__version__)'
The candidate interpreter is executable (test exit status 0).
```

**Codex (example response):**

> The job entered `gpu-a100` and the application returned exit code 1, with no evidence of signal
> termination. The first error occurs during Python import. The complete `train.sh` invokes an
> unqualified `python` and contains no explicit module load, activation, or interpreter path. The
> immediate failure is therefore that the Python environment selected by this script cannot import
> `torch`, not queueing or a timeout.
>
> Another project script already uses the executable
> `/home/alice/venvs/torch/bin/python` for a `torch` check. This is a sourced candidate, not proof
> that it works on a compute node. I recommend changing the training command to:
>
> ```bash
> /home/alice/venvs/torch/bin/python train.py
> ```
>
> This would modify `train.sh` but would not submit a job. Do you approve the edit and a
> `bash -n train.sh` validation? A new job must still confirm that the environment works on a compute
> node.

## Example 5: Discover configuration, write a script, and submit it

**You:**

> Use my latest successful training job to prepare and submit the current `train.py`. Ask before each
> change.

**Codex first queries history and searches project files:**

```bash
id -un
cacct -u alice -n train -t completed -m 10 \
  --type Batch -o "%j %n %P %a %q %C %M %l %t %e %s"
rg -n '^#CBATCH --(gres|partition|account|qos|cpus-per-task|mem|time)=' \
  /home/alice/project --glob '*.sh'
```

**Relevant tool output (example):**

```text
alice
JOBID JOBNAME PARTITION ACCOUNT QOS    REQCPUS REQMEM TIMELIMIT STATE     EXITCODE SUBMITTIME
4701  train   gpu-a100  ml      normal 4.00    16G    02:00:00  Completed 0:0      2026-08-10 09:42:11
4638  train   gpu-a100  ml      normal 4.00    16G    02:00:00  Completed 0:0      2026-08-07 16:18:03
/home/alice/project/train_4701.sh:4:#CBATCH --partition=gpu-a100
/home/alice/project/train_4701.sh:5:#CBATCH --account=ml
/home/alice/project/train_4701.sh:6:#CBATCH --qos=normal
/home/alice/project/train_4701.sh:10:#CBATCH --gres=gpu:a100:1
```

**Codex (example response):**

> Among the returned records, the successful matching job with the latest submission time is `4701`,
> with state `Completed` and exit code `0:0`. `cacct` confirms partition `gpu-a100`, account `ml`,
> QoS `normal`, four CPUs, 16 GB, and a two-hour limit.
> The project's `train_4701.sh` also confirms `#CBATCH --gres=gpu:a100:1`. I propose creating
> `train.sh` with these sourced values and the current `train.py` as its entry point. I will not
> submit it yet. Do you approve writing the file?

**You:**

> Approved.

**Codex writes and validates, then responds:**

> Created `train.sh`; `bash -n train.sh` passed. Submitting it will create a job and consume cluster
> resources. Do you approve running `cbatch train.sh`?

**You:**

> Submit it.

**Codex submits and verifies automatically:**

```bash
cbatch train.sh
cqueue -j 5012 -o="%j %P %n %t %l %N %L"
ccontrol show job 5012
```

**Relevant tool output (example):**

```text
Job id allocated: 5012
JOBID PARTITION NAME  STATUS  TIMELIMIT NODES NODELIST/REASON
5012  gpu-a100  train Pending 02:00:00  1     Priority
JobId=5012 JobName=train
        User=alice(1001) Account=ml
        JobState=Pending TimeLimit=02:00:00 Partition=gpu-a100
        Priority=874 Qos=normal
        ReqRes:node=1 cpu=4.00 mem=16G gres=gpu:a100:1
```

**Codex (example response):**

> Submission succeeded with job ID `5012`. The query confirms `gpu-a100` / `ml` / `normal`, one A100,
> four CPUs, 16 GB, and two hours. Its current state is `Pending` with reason `Priority`. The job is
> correctly queued; do not submit another copy.

## Boundaries

- Codex can give specific conclusions from actual query results but gains no CraneSched permissions beyond your account.
- Site documentation and the installed command's `--help` take precedence; review generated scripts and change summaries before execution.
- Never provide passwords, API keys, private keys, or a complete environment dump.
- Nodes, partitions, accounts, QoS, services, and system configuration remain administrator responsibilities.
