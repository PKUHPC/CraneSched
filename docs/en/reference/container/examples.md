# Examples

This page provides typical usage examples for CraneSched Container Support. Most examples use widely available public images (`alpine`, `ubuntu`, `python:3.11`, `nginx`, `nicolaka/netshoot`) so you can copy-paste and run them directly; the few that demonstrate real workflows are templates and require your own scripts, which is noted in the comments.

---

## Basic Usage

```bash
# One-shot command: container exits as soon as the command finishes
ccon -p CPU run alpine:latest echo "Hello, World!"

# With resource limits
#   -c 4         allocate 4 CPU cores
#   --mem 8G     allocate 8 GiB memory
#   -t 2:00:00   maximum run time of 2 hours (hh:mm:ss)
ccon -p CPU -c 4 --mem 8G -t 2:00:00 run python:3.11 \
    python -c "import math; print(math.pi)"

# Interactive container
#   -i  keep stdin open    -t  allocate a pseudo-TTY
ccon -p CPU run -it ubuntu:22.04 /bin/bash

# Background execution: returns the container ID and detaches from the terminal
#   -d  detached mode
ccon -p CPU run -d python:3.11 python -m http.server 8000
```

---

## GPU Jobs

```bash
# Single-GPU smoke test: check CUDA availability
#   --gres gpu:1   request 1 GPU (any model)
ccon -p GPU --gres gpu:1 run pytorch/pytorch:latest \
    python -c "import torch; print(torch.cuda.is_available(), torch.cuda.device_count())"

# Request a specific GPU model (e.g. A100), and list visible devices
#   --gres gpu:a100:2   request 2 A100 GPUs
ccon -p GPU --gres gpu:a100:2 run pytorch/pytorch:latest nvidia-smi

# Multi-GPU training template (requires your own train.py)
ccon -p GPU --gres gpu:4 run pytorch/pytorch:latest \
    torchrun --nproc_per_node=4 train.py
```

---

## Multi-Node Jobs

`-N` sets the number of nodes; the container is launched simultaneously on every allocated node.

```bash
# Print hostname from each of 2 nodes
ccon -p CPU -N 2 run alpine:latest hostname

# Multi-node GPU training template (4 nodes × 2 GPUs, requires your own train.py)
ccon -p GPU -N 4 --gres gpu:2 run pytorch/pytorch:latest \
    torchrun --nnodes=4 --nproc_per_node=2 train.py
```

Querying logs or executing commands on a multi-node job requires `-n` to pick the target node:

```bash
ccon logs -n node01 123.1
ccon exec -n node01 123.1 hostname
```

---

## Data Mounts and Environment Variables

```bash
# -v HOST:CONTAINER   mount a host directory into the container
# -e KEY=VALUE        inject an environment variable
ccon -p CPU run \
    -v /tmp:/data \
    -e GREETING=hello \
    alpine:latest sh -c 'echo "$GREETING from $(hostname)"; ls /data | head'
```

---

## cbatch Script Orchestration

Chain multiple container steps into a single batch script; every step reuses the same resource allocation.

Create `pipeline.sh`:

```bash
#!/bin/bash
#CBATCH --job-name=pipeline-demo
#CBATCH -p CPU
#CBATCH -t 0:30:00
#CBATCH --pod                       # declare this as a container job (Pod)

# Step 1: simulated data prep
ccon run alpine:latest sh -c 'echo "prepare $(date)"'

# Step 2: simulated compute
ccon run python:3.11 python -c "print(sum(range(1000)))"

# Step 3: simulated evaluation
ccon run alpine:latest sh -c 'echo "evaluate done"'
```

Submit:

```bash
cbatch pipeline.sh
```

---

## Mixed Steps

Container and non-container steps can coexist in the same job:

```bash
#!/bin/bash
#CBATCH -p CPU
#CBATCH -N 2
#CBATCH --pod

# Container step
ccon run alpine:latest sh -c 'echo prepare on $(hostname)'

# Non-container step (runs directly on the allocated nodes)
crun -N 2 hostname

# Container step
ccon run alpine:latest sh -c 'echo postprocess done'
```

---

## Private Image Registry

```bash
# Login: prompts for username / password; credentials are stored in user config
ccon login registry.example.com

# Run with a private image
ccon -p CPU run registry.example.com/team/image:v1.0 echo "private ok"

# Logout: clear stored credentials
ccon logout registry.example.com
```

---

## Debugging

```bash
# View logs (123.1 = JOB.STEP)
ccon logs 123.1
ccon logs -f 123.1          # -f follow output in real time
ccon logs --tail 100 123.1  # show only the last 100 lines

# Exec into a running container
ccon exec -it 123.1 /bin/bash

# Inspect
ccon inspect 123.1          # container step
ccon inspectp 123           # container job (Pod)
```

---

## User Namespace

User namespacing is enabled by default: root (uid 0) inside the container is mapped to your regular user on the host, preventing in-container root from escaping its bounds.

```bash
# Default: shown as root inside the container
ccon -p CPU run ubuntu:22.04 id
# uid=0(root) gid=0(root)

# Disabled: process runs as the host user
ccon -p CPU run --userns=false ubuntu:22.04 id
# uid=1000(user) gid=1000(user)
```

---

## Network Configuration

`nicolaka/netshoot` ships with `curl`, `dig`, `tcpdump`, `ip`, and other network tooling, so it's ideal for verifying container networking.

```bash
# Default networking: container gets its own network namespace
ccon -p CPU run nicolaka/netshoot ip addr show

# --network host: share the host's network stack (no port isolation)
ccon -p CPU run --network host nicolaka/netshoot \
    sh -c 'hostname; ss -ltn | head'

# Port mapping: expose container port 80 on host port 8080
#   -p HOST:CONTAINER
ccon -p CPU run -p 8080:80 nginx:latest
```

Equivalent directives inside a cbatch script:

```bash
#CBATCH --pod
#CBATCH --pod-host-network       # equivalent to: ccon run --network host
#CBATCH --pod-port=8080:80       # equivalent to: ccon run -p 8080:80
```

---

## Related Documentation

- [Quick Start](quickstart.md)
- [Core Concepts](concepts.md)
- [Troubleshooting](troubleshooting.md)
