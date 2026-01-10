# Examples

This page provides typical usage examples for CraneSched Container Support.

---

## Basic Usage

```bash
# Run simple command
ccon -p CPU run ubuntu:22.04 echo "Hello, World!"

# With resource limits
ccon -p CPU -c 4 --mem 8G -t 2:00:00 run python:3.11 python compute.py

# Interactive container
ccon -p CPU run -it ubuntu:22.04 /bin/bash

# Background execution
ccon -p CPU run -d python:3.11 python server.py
```

---

## GPU Jobs

```bash
# Single GPU
ccon -p GPU --gres gpu:1 run pytorch/pytorch:latest python train.py

# Multi-GPU (single node)
ccon -p GPU --gres gpu:4 run pytorch/pytorch:latest \
    torchrun --nproc_per_node=4 train.py

# Specific GPU type
ccon -p GPU --gres gpu:a100:2 run pytorch/pytorch:latest python train.py
```

---

## Multi-Node Jobs

When using `-N` parameter, containers start simultaneously on multiple nodes:

```bash
# 2-node parallel
ccon -p CPU -N 2 run python:3.11 python worker.py

# Multi-node GPU training (4 nodes × 2 GPUs)
ccon -p GPU -N 4 --gres gpu:2 run pytorch/pytorch:latest \
    torchrun --nnodes=4 --nproc_per_node=2 train.py
```

Multi-node container interaction requires specifying target node:

```bash
ccon logs -n node01 123.1
ccon exec -n node01 123.1 hostname
```

---

## Data Mounts and Environment Variables

```bash
ccon -p GPU --gres gpu:1 run \
    -v /shared/data:/data:ro \
    -v /home/user/output:/output \
    -e BATCH_SIZE=64 \
    -e NUM_WORKERS=4 \
    pytorch/pytorch:latest python train.py
```

---

## cbatch Script Orchestration

Create `pipeline.sh`:

```bash
#!/bin/bash
#CBATCH --job-name=ml-pipeline
#CBATCH -p GPU
#CBATCH --gres=gpu:1
#CBATCH -t 4:00:00
#CBATCH --pod

# Data preprocessing
ccon run python:3.11 python preprocess.py

# Model training
ccon run pytorch/pytorch:latest python train.py

# Model evaluation
ccon run pytorch/pytorch:latest python evaluate.py
```

Submit:

```bash
cbatch pipeline.sh
```

---

## Mixed Steps

Container and non-container steps can be mixed:

```bash
#!/bin/bash
#CBATCH -p CPU
#CBATCH -N 2
#CBATCH --pod

# Container step
ccon run python:3.11 python prepare.py

# Non-container step
crun -N 2 ./mpi_compute

# Container step
ccon run python:3.11 python postprocess.py
```

---

## Private Image Registry

```bash
# Login
ccon login registry.example.com

# Use private image
ccon -p CPU run registry.example.com/team/image:v1.0 ./run.sh

# Logout
ccon logout registry.example.com
```

---

## Debugging

```bash
# View logs
ccon logs 123.1
ccon logs -f 123.1          # Real-time follow

# Enter container
ccon exec -it 123.1 /bin/bash

# View details
ccon inspect 123.1          # Container step
ccon inspectp 123           # Container job
```

---

## User Namespace

```bash
# Default enabled (root inside container)
ccon -p CPU run ubuntu:22.04 id
# uid=0(root) gid=0(root)

# Disable user namespace
ccon -p CPU run --userns=false ubuntu:22.04 id
# uid=1000(user) gid=1000(user)
```

---

## Network Configuration

```bash
# Use host network
ccon -p CPU run --network host python:3.11 python server.py

# Port mapping
ccon -p CPU run -p 8888:8888 jupyter/minimal-notebook
```

In cbatch scripts:

```bash
#CBATCH --pod
#CBATCH --pod-host-network
#CBATCH --pod-port=8888:8888
```

---

## Related Documentation

- [Quick Start](quickstart.md)
- [Core Concepts](concepts.md)
- [Troubleshooting](troubleshooting.md)
