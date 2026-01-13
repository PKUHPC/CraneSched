# Quick Start

This page helps you quickly get started with CraneSched Container Support. After reading, you will be able to submit container jobs, view container status, append container steps, and perform runtime interaction.

!!! note "Prerequisites"
    - Cluster administrator has enabled container support (CRI configured)
    - You have a valid user account and partition access
    - Container images are accessible from nodes (local or remote registry)

---

## Your First Container Job

Use `ccon run` to submit a simple container job:

```bash
ccon -p CPU run ubuntu:22.04 echo "Hello from container"
```

Parameter description:

| Parameter | Description |
| --- | --- |
| `-p CPU` | Specify partition (Crane Flag, placed between `ccon` and `run`) |
| `ubuntu:22.04` | Container image |
| `echo "Hello from container"` | Container startup command |

After execution, the job enters the queue for scheduling. Once scheduled, the container runs on the allocated node, outputs results, and exits.

---

## View Container Status

### View Container Steps

```bash
# View running container steps
ccon ps

# View all container steps (including completed)
ccon ps -a
```

Example output:

```
CONTAINER ID   IMAGE          COMMAND                  CREATED        STATUS     NODE
123.1          ubuntu:22.04   echo "Hello from co..."  2 minutes ago  Running    node01
124.1          python:3.11    python train.py          5 minutes ago  Completed  node02
```

### View Container Jobs (Pods)

```bash
# View running container jobs
ccon pods

# View all container jobs
ccon pods -a
```

Example output:

```
POD ID   NAME       CREATED         STATUS     NODES
123      myjob      2 minutes ago   Running    node01
124      training   5 minutes ago   Completed  node02
```

---

## Interactive Container

Use `-it` parameters to start an interactive container:

```bash
ccon -p CPU run -it ubuntu:22.04 /bin/bash
```

Parameter description:

| Parameter | Description |
| --- | --- |
| `-i` | Keep stdin open |
| `-t` | Allocate a pseudo-TTY |

Once inside the container, you can execute commands as in a normal terminal. The job automatically ends when you exit.

---

## Background Container

Use `-d` parameter to run a container in the background:

```bash
ccon -p CPU run -d --name myserver nginx:latest
```

The command returns the container ID (format: `JOBID.STEPID`), and the container continues running in the background.

---

## Using cbatch --pod

`cbatch --pod` allows you to create container jobs using batch scripts, suitable for scenarios requiring orchestration of multiple container tasks.

### Create Script

Create file `job.sh`:

```bash
#!/bin/bash
#CBATCH --job-name=container-job
#CBATCH -p CPU
#CBATCH -N 1
#CBATCH --pod

echo "Job started on $(hostname)"

# Run first container task
ccon run python:3.11 python -c "print('Step 1: Data preprocessing')"

# Run second container task
ccon run python:3.11 python -c "print('Step 2: Model training')"

echo "Job completed"
```

### Submit Job

```bash
cbatch job.sh
```

The `#CBATCH --pod` directive indicates this is a container job. When the job starts, the system automatically creates a Pod, and the script runs as the Primary Step. The `ccon run` commands in the script append container steps, reusing the job's allocated resources.

---

## Appending Container Steps

During container job execution, you can append new container steps.

### Within cbatch Script

Use `ccon run` directly in the script:

```bash
#!/bin/bash
#CBATCH --pod
#CBATCH -p CPU

# Append container step
ccon run ubuntu:22.04 echo "This is a container step"

# Append background container step
ccon run -d redis:latest

# Wait for all container steps to complete
ccon wait
```

### In Interactive Session

First allocate resources and create a container job using `cbatch --pod`:

```bash
# Allocate resources
cbatch --pod -N 1 -p CPU --wrap "sleep infinity" &

# Assuming job ID is 125, append container steps from another terminal
export CRANE_JOB_ID=125
ccon run python:3.11 python script.py
```

---

## Runtime Interaction

### View Container Logs

```bash
# View container logs
ccon logs 123.1

# Follow logs in real-time
ccon logs -f 123.1

# View last 100 lines
ccon logs --tail 100 123.1
```

### Connect to Running Container

```bash
# Connect to container's stdin/stdout
ccon attach 123.1
```

Use `Ctrl+C` to disconnect (does not terminate the container).

### Execute Commands in Container

```bash
# Execute single command
ccon exec 123.1 ls -la /app

# Start interactive shell
ccon exec -it 123.1 /bin/bash
```

---

## Stop Container

```bash
# Stop container step
ccon stop 123.1

# Specify timeout (seconds)
ccon stop -t 30 123.1
```

Stopping a container step sends SIGTERM to the container; SIGKILL is sent after timeout.

---

## Common Resource Parameters

`ccon run` supports standard Crane resource parameters:

```bash
# Specify CPU and memory
ccon -c 4 --mem 8G -p CPU run python:3.11 python train.py

# Use GPU
ccon --gres gpu:1 -p GPU run pytorch/pytorch:latest python train.py

# Specify time limit
ccon -t 1:00:00 -p CPU run ubuntu:22.04 ./long_task.sh

# Specify node count
ccon -N 2 -p CPU run mpi-image mpirun ./my_mpi_app
```

!!! tip "Crane Flags Placement"
    Crane resource parameters (like `-p`, `-c`, `--mem`, `--gres`) must be placed between `ccon` and `run`, while container parameters (like `-d`, `-it`, `-v`) go after `run`.
    
    ```bash
    ccon [Crane Flags] run [Container Flags] IMAGE [COMMAND]
    ```

---

## Mount Directories

Use `-v` parameter to mount host directories to the container:

```bash
# Mount data directories
ccon -p CPU run -v /data/input:/input -v /data/output:/output python:3.11 python process.py
```

Format: `host_path:container_path`.

---

## Environment Variables

Use `-e` parameter to set environment variables:

```bash
ccon -p CPU run -e MODEL_PATH=/models -e BATCH_SIZE=32 python:3.11 python train.py
```

---

## Next Steps

- [Examples](examples.md): More typical usage scenarios
- [Core Concepts](concepts.md): Deep dive into container job model
- [Troubleshooting](troubleshooting.md): Problem diagnosis and resolution
