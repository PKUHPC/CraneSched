# PMIx User Guide

PMIx (Process Management Interface for Exascale) is a process management interface standard for HPC. It provides inter-node process discovery, information exchange (modex), and collective barrier (fence) services for MPI and other parallel programs.

CraneSched ships with a built-in PMIx server hosted by the Supervisor process on each compute node. When using `crun --mpi=pmix`, there is no need for `mpirun`—CraneSched itself acts as the Process Manager, directly launching MPI processes on the allocated nodes and assigning each process a rank and a namespace via the built-in PMIx server.

## 1. System Requirements

### 1.1 Runtime Dependencies

| Component | Minimum Version | Notes |
|----------|------------------|-------|
| OpenPMIx | 4.x              | PMIx runtime library (required) |
| Open MPI | 4.1+             | MPI implementation with PMIx support (5.x recommended) |
| UCX (optional) | 1.12+       | Enable high-performance direct connection mode (requires InfiniBand/RoCE, etc.) |

### 1.2 Installing OpenPMIx

Build from source (recommended for production). Refer to the OpenPMIx official documentation for details.

```bash
sudo dnf install libevent-devel
sudo dnf install hwloc-devel
# Example: install to /opt/pmix
wget https://github.com/openpmix/openpmix/releases/download/v4.2.9/pmix-4.2.9.tar.bz2
tar xf pmix-4.2.9.tar.bz2 && cd pmix-4.2.9
./configure --prefix=/opt/pmix
make -j$(nproc) && sudo make install
```

### 1.3 Installing an MPI Implementation with PMIx Support

Open MPI (recommended)

Using Open MPI 5.0.6 + system PMIx as an example. Refer to the Open MPI official documentation for details.

```bash
sudo dnf install gcc-gfortran
wget https://download.open-mpi.org/release/open-mpi/v5.0/openmpi-5.0.6.tar.bz2
tar xf openmpi-5.0.6.tar.bz2 && cd openmpi-5.0.6

# If PMIx is installed in /opt/pmix, specify it via --with-pmix
./configure --prefix=/opt/openmpi \
            --with-pmix=/opt/pmix

make -j$(nproc) && sudo make install
# Verify PMIx support:
/opt/openmpi/bin/ompi_info | grep pmix
# The output should contain MCA psec: pmix or similar
```

If OpenPMIx was installed via a package manager under `/opt`, you can omit `--with-pmix`; Open MPI’s configure script will detect it automatically.

### 1.4 Confirming CraneSched PMIx Support

PMIx support must be enabled at CraneSched compile time via a CMake option. Please verify with your cluster administrator.

```bash
# Administrator enablement example:
cmake -DWITH_PMIX=/opt/pmix [other options] ..
# If UCX is also installed, the system will detect it via pkg-config and enable direct connection mode:
# Check whether UCX is supported
pkg-config --libs ucx
```

---

## 2. Quick Start

The following example runs an MPI program on 2 nodes with 4 MPI processes per node (8 ranks total):

```bash
crun --nodes=2 --ntasks-per-node=4 --mpi=pmix ./my_mpi_program
```

CraneSched will:

1. Request resources for 2 nodes from the scheduler

2. Directly launch 4 instances of `./my_mpi_program` on each node

3. Assign ranks (0–7) and a namespace to all 8 processes via the built-in PMIx server

4. When each MPI program calls `MPI_Init()`, it completes the handshake via PMIx and automatically obtains its rank

No `mpirun` is required.

---

## 3. Basic Usage

### 3.1 crun Direct Mode (Job Mode)

From a login node, request resources and launch the MPI program in one step:

```
crun [resource flags] --mpi=pmix <path_to_mpi_program> [program args]
```

Examples:

2 nodes, 8 processes per node, 16 ranks total
```bash
crun --nodes=2 \
     --ntasks-per-node=8 \
     --mpi=pmix \
     ./my_simulation input.cfg
```

Specify only total tasks (the scheduler decides node placement)
```bash
crun --ntasks=32 \
     --mpi=pmix \
     ./my_simulation
```

### 3.2 cbatch Batch Job

Inside a batch script, use `crun` to launch the MPI program with resource parameters specified via `#CBATCH` directives:

```sh
#!/bin/bash
#CBATCH --job-name=mpi_job
#CBATCH --nodes=4
#CBATCH --ntasks-per-node=16
#CBATCH --cpus-per-task=2
#CBATCH --mem-per-cpu=2G
#CBATCH --time=04:00:00
#CBATCH --partition=CPU
#CBATCH --output=mpi_%j.out
#CBATCH --error=mpi_%j.err
# Load the MPI environment (adjust paths as needed)

export PATH=/opt/openmpi/bin:$PATH
export LD_LIBRARY_PATH=/opt/openmpi/lib:$LD_LIBRARY_PATH

echo "Job ID: $CRANE_JOB_ID"
echo "Node list: $CRANE_JOB_NODELIST"
echo "Num nodes: $CRANE_JOB_NUM_NODES"

# Launch the PMIx job step via crun
# --nodes and --ntasks-per-node can be inherited from #CBATCH, or overridden here
crun --nodes=4 --ntasks-per-node=16 --mpi=pmix ./my_simulation
```

Submit:

```bash
cbatch mpi_job.sh
```

### 3.3 Job Step Mode (inside a `calloc` session)

First allocate resources with `calloc`, then create multiple independent PMIx job steps within the allocation using `crun`:

1. Allocate resources and enter an interactive shell
```bash
calloc --nodes=4 --ntasks-per-node=8 --time=02:00:00
```
2. Run two MPI steps sequentially within the allocation (each `crun` starts a new PMIx server; ranks are numbered independently from 0)
```bash
crun --ntasks-per-node=8 --mpi=pmix ./preprocess data/
crun --ntasks-per-node=8 --mpi=pmix ./main_solver
```

---

## 4. Advanced Configuration

Set environment variables in your job script to adjust the behavior of CraneSched’s PMIx server.

### 4.1 Collective Algorithm (Fence Type)

PMIx fence (barrier synchronization) supports two collective algorithms:

| Value | Algorithm        | Best for                             |
|-------|------------------|--------------------------------------|
| tree  | Tree aggregation | Larger node counts (default)         |
| ring  | Ring pass        | Smaller node counts, lower latency   |

Use ring (good for small jobs)
```bash
export CRANE_PMIX_FENCE=ring
crun --nodes=4 --ntasks-per-node=4 --mpi=pmix ./my_program
```

Use tree (good for large jobs; default is tree and can be omitted)
```bash
export CRANE_PMIX_FENCE=tree
crun --nodes=64 --ntasks-per-node=16 --mpi=pmix ./my_program
```

In a `#CBATCH` script:
```sh
#!/bin/bash
#CBATCH --nodes=8
#CBATCH --ntasks-per-node=16
export CRANE_PMIX_FENCE=ring

crun --nodes=8 --ntasks-per-node=16 --mpi=pmix ./my_program
```

### 4.2 UCX Direct Connection Mode

When the cluster has high-speed networking such as InfiniBand or RoCE and UCX is installed, you can enable UCX direct connection mode. This routes inter-node PMIx metadata exchange (modex, fence) directly via UCX instead of gRPC, reducing latency:

```bash
export CRANE_PMIX_DIRECT_CONN_UCX=true
crun --nodes=16 --ntasks-per-node=8 --mpi=pmix ./my_program
```

This option requires CraneSched to detect UCX at build time (`pkg-config --libs ucx` must succeed). If CraneSched was not built with UCX support, the task will fail to launch.

---

## 5. Complete Examples

Example 1: Single-node MPI Hello World
```bash
crun --nodes=1 --ntasks-per-node=4 --mpi=pmix ./hello_mpi
```
Sample output:
```
Hello from rank 0 of 4 on node cn01

Hello from rank 1 of 4 on node cn01

Hello from rank 2 of 4 on node cn01

Hello from rank 3 of 4 on node cn01
```

Example 2: Multi-node CFD simulation (batch)
```sh
#!/bin/bash
#CBATCH --job-name=cfd_sim
#CBATCH --nodes=8
#CBATCH --ntasks-per-node=16
#CBATCH --cpus-per-task=2
#CBATCH --mem-per-cpu=2G
#CBATCH --time=06:00:00
#CBATCH --partition=CPU
#CBATCH --output=cfd_%j.out

export PATH=/opt/openmpi/bin:$PATH

echo "Start time: $(date)"
echo "Node list: $CRANE_JOB_NODELIST"

crun --nodes=8 --ntasks-per-node=16 --mpi=pmix ./cfd_solver -c config.json

echo "End time: $(date)"
```

Example 3: Multi-stage pipeline job
```sh
#!/bin/bash

#CBATCH --job-name=pipeline
#CBATCH --nodes=4
#CBATCH --ntasks-per-node=8
#CBATCH --time=04:00:00

export PATH=/opt/openmpi/bin:$PATH
export CRANE_PMIX_FENCE=ring

# Stage 1: data preprocessing (16 processes)
crun --nodes=2 --ntasks-per-node=8 --mpi=pmix ./preprocess data/input

# Stage 2: main computation (32 processes)
crun --nodes=4 --ntasks-per-node=8 --mpi=pmix ./solver data/preprocessed

# Stage 3: postprocessing (8 processes)
crun --nodes=1 --ntasks-per-node=8 --mpi=pmix ./postprocess data/results
```

Example 4: High-performance network environment (InfiniBand + UCX)
```sh
#!/bin/bash

#CBATCH --job-name=hpc_large
#CBATCH --nodes=64
#CBATCH --ntasks-per-node=16
#CBATCH --cpus-per-task=2
#CBATCH --time=12:00:00
#CBATCH --partition=IB

export CRANE_PMIX_FENCE=tree
export CRANE_PMIX_DIRECT_CONN_UCX=true
# UCX transport is used for PMIx metadata exchange and MPI point-to-point communication

crun --nodes=64 --ntasks-per-node=16 --mpi=pmix \
     ./large_scale_sim -config run.cfg

echo "Simulation completed at $(date)"
```

---

## 6. Troubleshooting

Q1: Submitting a job shows `PMIx support is not compiled in.`

Cause: CraneSched was compiled without PMIx support (i.e., `-DWITH_PMIX` was not set).

Solution: Contact your cluster administrator to recompile CraneSched:

```bash
# Administrator example
cmake -DWITH_PMIX=/opt/pmix -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)
```
