# PMIx User Guide

PMIx (Process Management Interface for Exascale) is a process management interface standard for high-performance computing. It provides inter-node process discovery, information exchange (modex), and collective barrier (fence) services for MPI and other parallel programs.

CraneSched has a built-in PMIx server hosted by the Supervisor process on each compute node. When using `crun --mpi=pmix`, **no `mpirun` wrapper is needed**—CraneSched itself acts as the Process Manager, directly launching MPI processes on allocated nodes according to `--ntasks-per-node`, and assigning each process a rank and namespace via the built-in PMIx server.

---

## 1. System Requirements

### 1.1 Runtime Dependencies

| Component | Minimum Version | Notes |
|-----------|----------------|-------|
| OpenPMIx | 4.x | PMIx runtime library (required) |
| Open MPI | 4.1+ | PMIx-capable MPI implementation (5.x recommended) |
| UCX (optional) | 1.12+ | High-performance direct connection mode (requires InfiniBand/RoCE) |

---

### 1.2 Installing OpenPMIx

=== "Rocky Linux 9 / RHEL 9 (package manager)"

    ```bash
    sudo dnf install -y epel-release
    sudo dnf install -y pmix pmix-devel
    ```

=== "Ubuntu 22.04 / Debian 12 (package manager)"

    ```bash
    sudo apt-get install -y libpmix-dev libpmix2
    ```

=== "Build from source (recommended for production)"

    ```bash
    # Example: install to /opt/pmix
    wget https://github.com/openpmix/openpmix/releases/download/v4.2.9/pmix-4.2.9.tar.bz2
    tar xf pmix-4.2.9.tar.bz2 && cd pmix-4.2.9
    ./configure --prefix=/opt/pmix
    make -j$(nproc) && sudo make install
    ```

---

### 1.3 Installing a PMIx-Enabled MPI Implementation

#### Open MPI (recommended)

```bash
# Example: Open MPI 5.0.6 with custom PMIx
wget https://download.open-mpi.org/release/open-mpi/v5.0/openmpi-5.0.6.tar.bz2
tar xf openmpi-5.0.6.tar.bz2 && cd openmpi-5.0.6

# Specify PMIx path if installed in /opt/pmix
./configure --prefix=/opt/openmpi \
            --with-pmix=/opt/pmix

make -j$(nproc) && sudo make install
```

Verify PMIx support:

```bash
/opt/openmpi/bin/ompi_info | grep pmix
# Should show: MCA psec: pmix or similar
```

!!! tip
    If OpenPMIx was installed via the system package manager (under `/usr`), you can omit `--with-pmix`; Open MPI's configure script will detect it automatically.

#### MPICH 4.x

MPICH 4.0+ supports PMIx through PRRTE (PMIx Reference Runtime Environment):

```bash
wget https://www.mpich.org/static/downloads/4.2.3/mpich-4.2.3.tar.gz
tar xf mpich-4.2.3.tar.gz && cd mpich-4.2.3
./configure --prefix=/opt/mpich \
            --with-pmix=/opt/pmix \
            --with-device=ch4:ucx   # optional: UCX transport layer
make -j$(nproc) && sudo make install
```

---

### 1.4 Confirming CraneSched PMIx Support

PMIx support must be enabled at **CraneSched compile time** via a CMake option. Please verify with your cluster administrator.

Administrator command to enable:

```bash
cmake -DWITH_PMIX=/opt/pmix [other options] ..
```

If UCX is also installed, it will be detected automatically via `pkg-config` and UCX direct connection mode will be enabled:

```bash
# Check if UCX support is available
pkg-config --libs ucx
```

---

## 2. Quick Start

The following example runs an MPI program on 2 nodes with 4 processes per node (8 ranks total):

```bash
crun --nodes=2 --ntasks-per-node=4 --mpi=pmix ./my_mpi_program
```

CraneSched will:

1. Request resources for 2 nodes from the scheduler
2. Directly launch 4 instances of `./my_mpi_program` on each node
3. Assign ranks (0–7) and a namespace to all 8 processes via the built-in PMIx server
4. When each MPI process calls `MPI_Init()`, it completes the handshake through PMIx and receives its rank automatically

**No `mpirun` is required.**

---

## 3. Basic Usage

### 3.1 `crun` Direct Mode (Job Mode)

From a login node, request resources and launch an MPI program in one step:

```
crun [resource flags] --mpi=pmix <mpi_program> [program args]
```

Examples:

```bash
# 2 nodes, 8 processes per node, 16 ranks total
crun --nodes=2 \
     --ntasks-per-node=8 \
     --cpus-per-task=2 \
     --mem=16G \
     --time=01:00:00 \
     --partition=CPU \
     --mpi=pmix \
     ./my_simulation input.cfg

# Only specify total tasks; let the scheduler determine node placement
crun --ntasks=32 \
     --cpus-per-task=1 \
     --mpi=pmix \
     ./my_simulation
```

### 3.2 `cbatch` Batch Job

Inside a batch script, use `crun` to launch the MPI program, with resource parameters in `#CBATCH` directives:

```bash
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

# Load MPI environment (adjust paths as needed)
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

First allocate resources with `calloc`, then use `crun` to create multiple independent PMIx job steps within the allocation:

```bash
# 1. Allocate resources and enter an interactive shell
calloc --nodes=4 --ntasks-per-node=8 --time=02:00:00

# 2. Run two MPI steps sequentially within the allocation
#    (each crun starts a fresh PMIx server; ranks are independently numbered from 0)
crun --ntasks-per-node=8 --mpi=pmix ./preprocess data/
crun --ntasks-per-node=8 --mpi=pmix ./main_solver
```

---

## 4. Supported MPI Implementations

### 4.1 Open MPI

Open MPI 4.x / 5.x natively supports PMIx. Simply run the compiled binary:

```bash
crun --nodes=4 --ntasks-per-node=8 --mpi=pmix ./my_mpi_program
```

---

## 5. Advanced Configuration

Set environment variables in your job script to tune the behavior of CraneSched's PMIx server.

### 5.1 Collective Algorithm (Fence Type)

PMIx fence (barrier synchronization) supports two collective algorithms:

| Value | Algorithm | Best for |
|-------|-----------|----------|
| `tree` (default) | Tree-based aggregation | Larger node counts |
| `ring` | Ring-based pass | Smaller node counts, lower latency |

```bash
# Use ring algorithm (good for small jobs)
export CRANE_PMIX_FENCE=ring
crun --nodes=4 --ntasks-per-node=4 --mpi=pmix ./my_program

# Use tree algorithm (default for large jobs; can be omitted)
export CRANE_PMIX_FENCE=tree
crun --nodes=64 --ntasks-per-node=16 --mpi=pmix ./my_program
```

In a `#CBATCH` script:

```bash
#!/bin/bash
#CBATCH --nodes=8
#CBATCH --ntasks-per-node=16

export CRANE_PMIX_FENCE=ring

crun --nodes=8 --ntasks-per-node=16 --mpi=pmix ./my_program
```

### 5.2 UCX Direct Connection Mode

When the cluster has high-speed networking (InfiniBand, RoCE, etc.) and UCX is installed, you can enable UCX direct connection mode. This routes inter-node PMIx metadata exchange (modex, fence) directly through UCX instead of gRPC, reducing latency:

```bash
export CRANE_PMIX_DIRECT_CONN_UCX=true
crun --nodes=16 --ntasks-per-node=8 --mpi=pmix ./my_program
```

!!! note
    This option requires CraneSched to have been compiled with UCX support (`pkg-config --libs ucx` must succeed at build time). If CraneSched was not built with UCX support, the task will fail to launch.

### 5.3 Combining Both Options

```bash
#!/bin/bash
#CBATCH --nodes=32
#CBATCH --ntasks-per-node=24
#CBATCH --cpus-per-task=2
#CBATCH --time=08:00:00

export CRANE_PMIX_FENCE=tree
export CRANE_PMIX_DIRECT_CONN_UCX=true

crun --nodes=32 --ntasks-per-node=24 --mpi=pmix ./cfd_solver run.cfg
```

---

## 7. Complete Examples

### Example 1: Single-Node MPI Hello World

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

### Example 2: Multi-Node CFD Simulation (Batch)

```bash
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

### Example 3: Multi-Stage Pipeline Job

```bash
#!/bin/bash

#CBATCH --job-name=pipeline
#CBATCH --nodes=4
#CBATCH --ntasks-per-node=8
#CBATCH --time=04:00:00

export PATH=/opt/openmpi/bin:$PATH
export CRANE_PMIX_FENCE=ring

# Stage 1: preprocessing (16 processes)
crun --nodes=2 --ntasks-per-node=8 --mpi=pmix ./preprocess data/input

# Stage 2: main computation (32 processes)
crun --nodes=4 --ntasks-per-node=8 --mpi=pmix ./solver data/preprocessed

# Stage 3: postprocessing (8 processes)
crun --nodes=1 --ntasks-per-node=8 --mpi=pmix ./postprocess data/results
```

### Example 4: High-Performance Network (InfiniBand + UCX)

```bash
#!/bin/bash

#CBATCH --job-name=hpc_large
#CBATCH --nodes=64
#CBATCH --ntasks-per-node=16
#CBATCH --cpus-per-task=2
#CBATCH --time=12:00:00
#CBATCH --partition=IB

export CRANE_PMIX_FENCE=tree
export CRANE_PMIX_DIRECT_CONN_UCX=true
# UCX used for both PMIx metadata exchange and MPI point-to-point communication

crun --nodes=64 --ntasks-per-node=16 --mpi=pmix \
     ./large_scale_sim -config run.cfg

echo "Simulation completed at $(date)"
```

---

## 8. Troubleshooting

### Q1: `MPI type pmix is not supported` error when submitting

**Cause**: CraneSched was compiled without PMIx support (i.e., `-DWITH_PMIX` was not set).

**Solution**: Contact your cluster administrator to recompile CraneSched:

```bash
# Administrator example
cmake -DWITH_PMIX=/opt/pmix -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)
```

---

### Q2: MPI program hangs at `MPI_Init()`

**Possible causes**: Network connectivity issue between nodes causing PMIx fence timeout, or Supervisor process crashed.

**Diagnostic steps**:

1. Check Supervisor logs:
   ```bash
   tail -f /var/crane/supervisor/supervisor.log | grep -iE 'pmix|error'
   ```
2. Verify that craned's listening port (default: 10010) is reachable between all compute nodes.
3. Try reducing the number of nodes or switching the fence algorithm:
   ```bash
   export CRANE_PMIX_FENCE=ring
   ```

---

### Q4: Ranks start from 0 in every job step—is that correct?

**Yes, this is expected behavior.** Each `crun --mpi=pmix` call creates an independent job step with its own PMIx namespace (e.g., `crane.123.0`, `crane.123.1`). Within a single step, ranks run from `0` to `total_tasks - 1`.

---

## 9. Notes

1. **MPI program compilation requirement**: Your MPI program must be linked against an MPI library with PMIx support (e.g., Open MPI built with `--with-pmix`). Programs that only support PMI1/PMI2 may fail to initialize in PMIx mode.

2. **PMIx namespace format**: Each job step's namespace follows the pattern `crane.<job_id>.<step_id>`. For example, job ID=123, step ID=0 produces `crane.123.0`.
