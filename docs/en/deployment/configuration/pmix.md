# PMIx User Guide

PMIx (Process Management Interface for Exascale) is a process management interface standard for HPC. It provides inter-node process discovery, information exchange (modex), and collective barrier (fence) services for MPI and other parallel programs.

CraneSched ships with a built-in PMIx server hosted by the Supervisor process on each compute node. When using `crun --mpi=pmix`, there is no need for `mpirun`—CraneSched itself acts as the Process Manager, directly launching MPI processes on the allocated nodes and assigning each process a rank and a namespace via the built-in PMIx server.

## 1. System Requirements

### 1.1 Runtime Dependencies

| Component | Minimum Version | Notes |
|----------|------------------|-------|
| OpenPMIx | 3.x              | PMIx runtime library (required) |
| Open MPI | 4.1+             | MPI implementation with PMIx support (5.x recommended) |
| UCX (optional) | 1.12+       | Enable high-performance direct connection mode (requires InfiniBand/RoCE, etc.) |

### 1.2 Installing OpenPMIx

PMIx support is enabled by default. Building CraneSched requires a PMIx 3 or newer SDK; runtime nodes need the matching library.
Prefer distribution packages: `pmix-devel` on Rocky Linux 9/10, or `libpmix-dev` on Ubuntu. Let the package manager resolve
libevent, hwloc and other dependencies. Rocky Linux 8 ships PMIx 2.2.5, which does not meet the requirement.
When distribution packages are insufficient, follow the [OpenPMIx installation guide](https://docs.openpmix.org/en/latest/installing-pmix/index.html)
to build and distribute a maintained dependency version. Installing it only on the build host is insufficient.

For a custom installation, set `PMIx_ROOT` or `CMAKE_PREFIX_PATH`. A custom shared-library directory may require
`CMAKE_INSTALL_RPATH_USE_LINK_PATH=ON`; verify the installed binary's actual library resolution. A matching SONAME alone does not prove version compatibility.

### 1.3 Installing an MPI Implementation with PMIx Support

Building CraneSched's PMIx support does not require Open MPI. Running MPI applications additionally requires a PMIx-capable
MPI implementation. Use distribution Open MPI packages or the [Open MPI installation guide](https://docs.open-mpi.org/en/main/installing-open-mpi/index.html)
to install a compatible version. Validate client/server interoperability and consistent support libraries, including libevent/hwloc,
with a real MPI job. A version command is insufficient. `crun --mpi=pmix` launches application processes directly without an additional `mpirun` launcher.

### 1.4 Configuring CraneSched PMIx Support

`CRANE_ENABLE_PMIX` defaults to ON. Missing or incompatible dependencies fail CMake configuration when enabled.
System packages are discovered automatically; a custom prefix can be selected as follows:

```bash
cmake -S . -B build -DCRANE_ENABLE_PMIX=ON -DPMIx_ROOT=/opt/pmix
cmake --build build --parallel 4
```

Use `-DCRANE_ENABLE_PMIX=OFF` to disable the feature explicitly. A path hint is not an enable/disable switch.
UCX transport defaults to OFF. After installing its SDK, opt in with `-DCRANE_ENABLE_UCX=ON`; PMIx must also be enabled,
and pkg-config must find UCX 1.12 or newer. Merely installing UCX does not change the selected product features.

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

### Node identities, addresses and TLS

PMIx uses the parsed `name` (canonical CranedId), `NodeHostname` and `NodeAddr`
from the [node configuration](config.md). Craned supplies the local ID to
Supervisor. Port exchange, peer lookup and tree/ring membership use canonical
IDs, rather than matching the system hostname directly against the step's IDs.
PMIx hostname metadata and node maps use `NodeHostname`.

gRPC connects to `NodeAddr` and, when TLS is enabled, verifies the certificate
against `NodeHostname`, following the controller's Craned connection semantics.
The existing configuration parser defaults `NodeHostname` to `name` and
`NodeAddr` to `NodeHostname`. `NodeAddr` is not a node alias. PMIx does not append
`TLS.DomainSuffix` or require logical IDs to resolve in DNS. Certificates must
cover the configured `NodeHostname`; non-TLS connections use the same address map.

Set environment variables in your job script to adjust the behavior of CraneSched’s PMIx server.
Ensure they are exported before the `crun` command and included by its `--export` policy. The complete input list, generated task variables, and node-list scope are documented in [Job Environment Variables](../../reference/job_env_vars.md).

### 4.1 Collective Algorithm (Fence Type)

PMIx fence (barrier synchronization) supports two collective algorithms:

| Value | Algorithm        | Best for                             |
|-------|------------------|--------------------------------------|
| tree  | Tree aggregation | Larger node counts                   |
| ring  | Ring pass        | Smaller node counts, lower latency   |

If `CRANE_PMIX_FENCE` is unset or is neither `tree` nor `ring`, the fence callback selects `ring` when collecting nonempty data and `tree` otherwise. The default is not unconditionally `tree`; algorithm performance depends on the workload and network.

Use ring (good for small jobs)
```bash
export CRANE_PMIX_FENCE=ring
crun --nodes=4 --ntasks-per-node=4 --mpi=pmix ./my_program
```

Explicitly select tree:
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

This option requires `CRANE_ENABLE_UCX=ON` and a compatible UCX SDK at build time. If CraneSched was not built with UCX support, the task will fail to launch.
Only the exact value `true` enables this mode; other values use gRPC.

### 4.3 Timeout and Client Temporary Directory

- `CRANE_PMIX_TIMEOUT`: timeout in seconds for PMIx operation/collective cleanup, default `5`. Use a positive whole number; empty values or conversion failures retain the default.
- `PMIXP_PMIXLIB_TMPDIR`: optional user-accessible base directory for client files. CraneSched appends `pmix.crane.cli/spmix_appdir_<uid>.<job>.<step>`. If unset or empty, PMIx chooses its default directory.

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
# This switch selects PMIx metadata transport; MPI point-to-point transport is configured by MPI itself

crun --nodes=64 --ntasks-per-node=16 --mpi=pmix \
     ./large_scale_sim -config run.cfg

echo "Simulation completed at $(date)"
```

---

## 6. Troubleshooting

Q1: Submitting a job shows `PMIx support is not compiled in.`

Cause: CraneSched was built with `CRANE_ENABLE_PMIX=OFF`, or the running artifact predates enabled PMIx support.

Solution: Ask the cluster administrator to rebuild CraneSched using
[Configuring CraneSched PMIx Support](#14-configuring-cranesched-pmix-support).
