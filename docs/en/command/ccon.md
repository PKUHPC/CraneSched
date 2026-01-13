# ccon - Container Job Management

ccon is the container job management tool for CraneSched, used to create, manage, and monitor containerized jobs. ccon's design is inspired by Docker CLI, enabling users to operate containers in a familiar way.

!!! info "Prerequisites"
    Before using ccon, ensure the cluster has container support enabled. See [Container Deployment](../deployment/container.md).

## Quick Start

Run a simple container job:

```bash
# Run alpine container in CPU partition
ccon -p CPU run alpine:latest -- echo "Hello from container"
```

## Command Overview

| Command | Description |
|:--------|:------------|
| `run` | Create and run new container |
| `ps` | List containers (steps) |
| `pods` | List container Pods (jobs) |
| `stop` | Stop running container |
| `wait` | Wait for all container steps in current job to complete |
| `logs` | View container logs |
| `attach` | Connect to running container |
| `exec` | Execute command inside running container |
| `inspect` | Show container step details |
| `inspectp` | Show Pod details |
| `login` | Login to container image registry |
| `logout` | Logout from container image registry |

## Global Options

**-C, --config=&lt;path&gt;**

:   Configuration file path. Default: `/etc/crane/config.yaml`.

**--debug-level=&lt;level&gt;**

:   Set debug output level. Available levels: `trace`, `debug`, `info`. Default: `info`.

**--json**

:   Output results in JSON format.

**-h, --help**

:   Display help information.

**-v, --version**

:   Display version information.

## run Command

Create and run a new container job.

```bash
ccon [Crane Options] run [Run Options] IMAGE [COMMAND] [ARG...]
```

!!! tip "Crane Options Placement"
    Crane options (like `-p`, `-N`, `--mem`) must be placed between `ccon` and `run`, not after `run`.

### Crane Options (Resource Scheduling)

These options control job resource allocation and scheduling behavior:

**-N, --nodes=&lt;num&gt;**

:   Number of nodes required. Default: 1.

**-c, --cpus-per-task=&lt;ncpus&gt;**

:   Number of CPU cores per task. Default: 1.

**--ntasks-per-node=&lt;ntasks&gt;**

:   Number of tasks to invoke on each node. Default: 1.

**--mem=&lt;size&gt;**

:   Maximum real memory. Supports units: GB (G, g), MB (M, m), KB (K, k), Bytes (B). Default unit: MB.

**--gres=&lt;list&gt;**

:   Generic resources per task. Format: `gpu:a100:1` or `gpu:1`.

**-p, --partition=&lt;partition&gt;**

:   Requested partition.

**-A, --account=&lt;account&gt;**

:   Account for job submission.

**-q, --qos=&lt;qos&gt;**

:   Quality of Service (QoS) for the job.

**-t, --time=&lt;time&gt;**

:   Time limit, format: `[day-]hours:minutes:seconds`.

**-w, --nodelist=&lt;nodes&gt;**

:   Nodes to allocate to job (comma-separated list).

**-x, --exclude=&lt;nodes&gt;**

:   Exclude specific nodes from allocation (comma-separated list).

**-r, --reservation=&lt;name&gt;**

:   Use reserved resources.

**--exclusive**

:   Request exclusive node resources.

**-H, --hold**

:   Submit job in held state.

**--extra-attr=&lt;json&gt;**

:   Extra job attributes (JSON format).

**--mail-type=&lt;type&gt;**

:   Mail notification type. Supported: `NONE`, `BEGIN`, `END`, `FAIL`, `TIMELIMIT`, `ALL`.

**--mail-user=&lt;email&gt;**

:   Email address for notifications.

**--comment=&lt;string&gt;**

:   Job comment.

### Run Options (Container Configuration)

These options configure container runtime parameters:

**--name=&lt;name&gt;**

:   Specify container name.

**-e, --env=&lt;KEY=VALUE&gt;**

:   Set environment variable. Can be used multiple times.

**-v, --volume=&lt;host:container[:ro]&gt;**

:   Bind mount volume. Format: `host_path:container_path[:readonly]`. Can be used multiple times.

**-p, --ports=&lt;host:container&gt;**

:   Publish container port to host. Format: `host_port:container_port`. Can be used multiple times.

**-d, --detach**

:   Run container in background and output container ID.

**-i, --interactive**

:   Keep stdin available to container process.

**-t, --tty**

:   Allocate pseudo-TTY for container.

**--entrypoint=&lt;cmd&gt;**

:   Override image default entrypoint.

**-u, --user=&lt;uid[:gid]&gt;**

:   Run container with specified UID. When `--userns=false`, only allows current user and accessible groups.

**--userns**

:   Enable user namespace. Default: `true` (container user mapped to root).

**--network=&lt;mode&gt;**

:   Container network mode. Supports `host` (use host network) and `default` (default Pod network).

**-w, --workdir=&lt;dir&gt;**

:   Working directory inside container.

**--pull-policy=&lt;policy&gt;**

:   Image pull policy. Supported: `Always`, `IfNotPresent`, `Never`.

### Examples

Basic container job:

```bash
ccon -p CPU run alpine:latest -- echo "Hello"
```

Interactive container:

```bash
ccon -p CPU run -it ubuntu:22.04 -- /bin/bash
```

GPU container with resource limits:

```bash
ccon -p GPU --gres gpu:1 --mem 8G run pytorch/pytorch:latest -- python train.py
```

Mount data directories:

```bash
ccon -p CPU run -v /data/input:/input:ro -v /data/output:/output alpine:latest -- cp /input/file /output/
```

Multi-node container job:

```bash
ccon -p CPU -N 4 run mpi-image:latest -- mpirun -np 4 ./my_program
```

Background execution:

```bash
ccon -p CPU run -d nginx:latest
# Output: 123.1 (JobID.StepID)
```

## ps Command

List container steps.

```bash
ccon ps [options]
```

**-a, --all**

:   Show all containers (default shows only running).

**-q, --quiet**

:   Only display container IDs.

### Examples

```bash
# List running containers
ccon ps

# List all containers (including finished)
ccon ps -a

# Only output container IDs
ccon ps -q
```

## pods Command

List container Pods (container jobs).

```bash
ccon pods [options]
```

**-a, --all**

:   Show all Pods (default shows only running).

**-q, --quiet**

:   Only display Pod IDs.

### Examples

```bash
# List running Pods
ccon pods

# List all Pods
ccon pods -a
```

## stop Command

Stop running container.

```bash
ccon stop [options] CONTAINER
```

**-t, --timeout=&lt;seconds&gt;**

:   Timeout for container to stop (seconds), force terminate after timeout. Default: 10.

### Examples

```bash
# Stop container (wait 10 seconds)
ccon stop 123.1

# Stop container immediately
ccon stop -t 0 123.1
```

## wait Command

Wait for all container steps in current job to complete. Usually used in cbatch scripts.

```bash
ccon wait [options]
```

**-t, --interval=&lt;seconds&gt;**

:   Polling interval (seconds), minimum 10 seconds.

### Examples

```bash
# Wait for all containers to complete in cbatch script
ccon wait
```

## logs Command

View container logs.

```bash
ccon logs [options] CONTAINER
```

**-f, --follow**

:   Follow log output continuously.

**--tail=&lt;lines&gt;**

:   Number of lines to show from end of logs.

**-t, --timestamps**

:   Show timestamps.

**--since=&lt;time&gt;**

:   Show logs since specified time. Format: `2025-01-15T10:30:00Z` or relative like `42m`.

**--until=&lt;time&gt;**

:   Show logs before specified time. Same format as above.

**-n, --target-node=&lt;node&gt;**

:   Get logs from specified node.

### Examples

```bash
# View container logs
ccon logs 123.1

# Follow log output
ccon logs -f 123.1

# View last 100 lines
ccon logs --tail 100 123.1
```

## attach Command

Connect to running container's stdin, stdout, and stderr.

```bash
ccon attach [options] CONTAINER
```

**--stdin**

:   Connect stdin. Default: `true`.

**--stdout**

:   Connect stdout. Default: `true`.

**--stderr**

:   Connect stderr. Default: `false`.

**--tty**

:   Allocate pseudo-TTY. Default: `true`.

**--transport=&lt;protocol&gt;**

:   Transport protocol. Supported: `spdy`, `ws`. Default: `spdy`.

**-n, --target-node=&lt;node&gt;**

:   Connect to container on specified node.

### Examples

```bash
# Connect to container
ccon attach 123.1
```

## exec Command

Execute command inside running container.

```bash
ccon exec [options] CONTAINER COMMAND [ARG...]
```

**-i, --interactive**

:   Keep stdin open.

**-t, --tty**

:   Allocate pseudo-TTY.

**--transport=&lt;protocol&gt;**

:   Transport protocol. Supported: `spdy`, `ws`. Default: `spdy`.

**-n, --target-node=&lt;node&gt;**

:   Execute command on specified node.

### Examples

```bash
# Execute command in container
ccon exec 123.1 ls -la

# Interactive shell
ccon exec -it 123.1 /bin/bash
```

## inspect Command

Show container step details.

```bash
ccon inspect CONTAINER
```

### Examples

```bash
ccon inspect 123.1
```

## inspectp Command

Show Pod details.

```bash
ccon inspectp POD
```

### Examples

```bash
ccon inspectp 123
```

## login Command

Login to container image registry.

```bash
ccon login [options] SERVER
```

**-u, --username=&lt;user&gt;**

:   Username.

**-p, --password=&lt;pass&gt;**

:   Password.

**--password-stdin**

:   Read password from stdin.

### Examples

```bash
# Interactive login
ccon login registry.example.com

# Login with username and password
ccon login -u myuser -p mypass registry.example.com

# Read password from stdin
echo $TOKEN | ccon login -u myuser --password-stdin registry.example.com
```

## logout Command

Logout from container image registry.

```bash
ccon logout SERVER
```

### Examples

```bash
ccon logout registry.example.com
```

## Container ID Format

ccon uses `JOBID.STEPID` format to identify containers:

- **JOBID**: Job number assigned by scheduling system
- **STEPID**: Step number, increments from 0

Examples:
- `123.0`: First container step of job 123
- `123.1`: Second container step of job 123

Pods (container jobs) are identified by pure JOBID, e.g., `123`.

## Using with cbatch

### Create Pod Job

Use `cbatch --pod` to create a container-capable job, then use `ccon run` in the script to start containers:

```bash
#!/bin/bash
#CBATCH --pod
#CBATCH -N 2
#CBATCH -c 4
#CBATCH --mem 8G
#CBATCH -p GPU

# Start container within job
ccon run -d pytorch/pytorch:latest -- python train.py

# Wait for containers to complete
ccon wait
```

Submit job:

```bash
cbatch train_job.sh
```

### cbatch Pod Options

| Option | Description |
|:-------|:------------|
| `--pod` | Enable container mode, create Pod job |
| `--pod-name` | Pod name (defaults to job name) |
| `--pod-port` | Pod port mapping, format: `HOST:CONTAINER` or `PORT` |
| `--pod-user` | Run Pod as specified UID[:GID] |
| `--pod-userns` | Enable Pod user namespace (default: true) |
| `--pod-host-network` | Use host network namespace |

## See Also

- [Container Support Overview](../reference/container/index.md) - Introduction to container features
- [Core Concepts](../reference/container/concepts.md) - Explanation of Pod, container steps, and other concepts
- [Quick Start](../reference/container/quickstart.md) - Quick experience with container features
- [Examples](../reference/container/examples.md) - Typical usage scenarios
- [Troubleshooting](../reference/container/troubleshooting.md) - Common issues and solutions
- [cbatch](cbatch.md) - Batch job submission
- [cqueue](cqueue.md) - View job queue
