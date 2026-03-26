# CraneSched vs. Slurm Feature Comparison

CraneSched is China's first open-source, domestically-developed compute scheduling system supporting both HPC and AI workloads. It comprehensively benchmarks against Slurm — the leading international scheduler — and surpasses it in performance, container support, and domestic hardware compatibility.

---

## Performance Comparison { #performance-comparison }

CraneSched significantly outperforms Slurm and OpenPBS in scheduling throughput. Measured results:

### Average Jobs Scheduled Per Minute

| Scheduler | Avg. Jobs/Min | Relative to CraneSched |
|-----------|:------------:|:---------------------:|
| **CraneSched** | **105,538** | **1x** |
| OpenPBS | 11,136 | 9.4x slower |
| Slurm | 4,259 | 24.7x slower |

### Peak Jobs Scheduled Per Minute

| Scheduler | Peak Jobs/Min | Relative to CraneSched |
|-----------|:------------:|:---------------------:|
| **CraneSched** | **122,427** | **1x** |
| OpenPBS | 20,541 | 6x slower |
| Slurm | 4,551 | 26.9x slower |

### Key Performance Metrics

| Metric | CraneSched |
|--------|-----------|
| Scheduling throughput | **5–20x** faster than Slurm |
| Cluster scale | Supports **100,000+** nodes |
| Job throughput | Real-time scheduling of **10,000+/sec**; hourly throughput exceeds **38 million** |
| Concurrency | **2,000,000+** concurrent jobs |
| Response latency | **Millisecond-level** low latency |

---

## Scheduling Feature Comparison

| Feature | CraneSched | Slurm | Description |
|---------|:----------:|:-----:|-------------|
| **Basic Scheduling** | | | |
| Backfill Scheduling | :material-check-circle:{ .success } | :material-check-circle:{ .success } | Run short jobs in idle time windows to improve utilization |
| Fair-Share Scheduling | :material-check-circle:{ .success } | :material-check-circle:{ .success } | Fair scheduling policy based on historical usage |
| Priority Scheduling | :material-check-circle:{ .success } | :material-check-circle:{ .success } | Multi-factor priority calculation |
| FIFO Scheduling | :material-check-circle:{ .success } | :material-check-circle:{ .success } | Basic first-in, first-out scheduling |
| **Resource Management** | | | |
| Preemption | :material-check-circle:{ .success } | :material-check-circle:{ .success } | High-priority jobs preempt resources from lower-priority ones |
| Reservation | :material-check-circle:{ .success } | :material-check-circle:{ .success } | Reserve resource time windows for specific users or jobs |
| TRES Fine-Grained Tracking | :material-check-circle:{ .success } | :material-check-circle:{ .success } | Trackable resource types (CPU, memory, GPU, etc.) |
| QOS Management | :material-check-circle:{ .success } | :material-check-circle:{ .success } | Differentiated service level control |
| Resource Escape Protection | :material-check-circle:{ .success } | :material-check-circle:{ .success } | Prevent jobs from exceeding allocated resources |
| **Job Management** | | | |
| Job Dependencies | :material-check-circle:{ .success } | :material-check-circle:{ .success } | Control dependency relationships between jobs |
| Job Arrays | :material-check-circle:{ .success } | :material-check-circle:{ .success } | Batch submission of parameterized jobs |
| Job Steps | :material-check-circle:{ .success } | :material-check-circle:{ .success } | Multi-step management within a job |
| Interactive Jobs | :material-check-circle:{ .success } | :material-check-circle:{ .success } | Real-time interactive computing |
| **Energy Saving & Efficiency** | | | |
| Power Saving Scheduling | :material-check-circle:{ .success } | :material-check-circle:{ .success } | Automatically shut down idle nodes under low load |
| AI Job Runtime Prediction (ORA) | :material-check-circle:{ .success } | :material-close-circle:{ .danger } | LLM-based job runtime prediction; 41% accuracy improvement |
| Smart Fair-Share (TSMF) | :material-check-circle:{ .success } | :material-close-circle:{ .danger } | In-house two-stage multi-factor algorithm; utilization improved to 97.3% |
| Automated Power Saving (EcoSched) | :material-check-circle:{ .success } | :material-close-circle:{ .danger } | Automated power control; 78.64% energy reduction under low load |
| **Account & Permissions** | | | |
| Hierarchical Account Management | :material-check-circle:{ .success } | :material-check-circle:{ .success } | Tree-structured user/account management |
| RBAC Access Control | :material-check-circle:{ .success } | :material-check-circle:{ .success } | Role-based access control |
| **High Availability** | | | |
| Automatic Fault Recovery | :material-check-circle:{ .success } | :material-check-circle:{ .success } | Automatic recovery after control node failure |
| Distributed Fault Tolerance | :material-check-circle:{ .success } | :material-check-circle:{ .success } | No single point of failure |

---

## Container Support Comparison

CraneSched and Slurm take different technical approaches to container support:

| Dimension | CraneSched | Slurm |
|-----------|-----------|-------|
| **Technical approach** | Based on K8s-underlying CRI RPC interface (the de facto cloud-native standard) | OCI model compatibility via CLI |
| **Container runtime** | containerd / CRI-O (via CRI interface) | Singularity / Enroot (via CLI) |
| **Image management** | Auto-pull; no manual handling needed | User must download and convert image formats |
| **Dedicated CLI** | `ccon` command, Docker CLI-inspired design, easy to learn | Native Slurm commands (sbatch/srun), different from Docker usage |
| **Network isolation** | CNI-based multi-tenant network isolation (Calico Underlay) | Minimal support |
| **Filesystem isolation** | Full user/network/mount namespace isolation | Limited |
| **Fake Root** | User Namespace-based, root experience inside container | Relies on Singularity's fakeroot |
| **RDMA support** | Supports SR-IOV shared RNIC and direct passthrough | Limited |
| **Operations tools** | Mature tools: crictl/nerdctl/ctr, etc. | Relies on community tools |

### Unique Advantages of CraneSched Containers

- **Ease of use**: No manual image pulling; dedicated CLI (`ccon`) designed for Docker users with no Slurm experience
- **Complete network isolation**: CNI support allows admins to implement various container networking strategies including multi-tenant isolation
- **RDMA network support**: Supports mid-to-large-scale RoCE networks (SR-IOV) and large-scale AI training clusters (Spine-Leaf architecture)
- **Pod/Job concept mapping**: Maps K8s Pod/Container concepts to Job/Step, enabling imperative orchestration

---

## Command Compatibility { #command-compatibility }

CraneSched provides an in-house Slurm & LSF Wrapper with full compatibility for Slurm and LSF command-line syntax:

| Slurm Command | CraneSched Native | Function |
|--------------|------------------|----------|
| `sbatch` | `cbatch` | Submit batch jobs |
| `squeue` | `cqueue` | View job queue |
| `srun` | `crun` | Run interactive jobs |
| `salloc` | `calloc` | Allocate interactive resources |
| `sinfo` | `cinfo` | View cluster information |
| `sacct` | `cacct` | View job history |
| `sacctmgr` | `cacctmgr` | Account management |
| `scancel` | `ccancel` | Cancel jobs |
| `scontrol` | `ccontrol` | System control |

**Zero migration cost**: Via the Slurm Wrapper, users can switch from Slurm to CraneSched transparently without modifying any scripts or workflows. Peking University's Weiming Teaching Cluster No.2 has successfully completed a transparent migration from Slurm to CraneSched, supporting hundreds of user software packages.

---

## Heterogeneous Hardware Support { #heterogeneous-hardware }

CraneSched fully supports mainstream domestic and international hardware platforms:

### Architecture Support

| Architecture | Support |
|-------------|---------|
| X86 | :material-check-circle:{ .success } |
| ARM | :material-check-circle:{ .success } |
| RISC-V | :material-check-circle:{ .success } |

### CPU Compatibility

| Category | Supported Brands |
|----------|----------------|
| International | Intel, AMD |
| Domestic | Phytium, Hygon, Huawei Kunpeng |

### Accelerator Compatibility

| Category | Supported Brands |
|----------|----------------|
| International | Nvidia GPU, AMD GPU |
| Domestic | Huawei Ascend, Hygon DCU, Cambricon MLU, Iluvatar CoreX, Kunlunxin, Metax, Moore Threads |

### Operating System Compatibility

| Category | Supported Systems |
|----------|-----------------|
| International | CentOS, Ubuntu, Rocky Linux |
| Domestic | OpenEuler, KylinOS |

CraneSched has received product compatibility certifications from multiple vendors including Inspur, Phytium, Hygon, and Kunlunxin.

---

## Summary

| Dimension | CraneSched Advantage |
|-----------|---------------------|
| **Performance** | 5–20x faster than Slurm |
| **Features** | Full Slurm feature coverage plus AI prediction, intelligent power saving, and more |
| **Containers** | Native CRI/CNI support, multi-tenant network isolation, RDMA support |
| **Compatibility** | Fully compatible with Slurm/LSF commands, zero migration cost |
| **Domestic hardware** | Full support for domestic CPUs, GPUs/NPUs, and operating systems |
| **Convergence** | HPC + AI integration; full Storage·Compute·Usage convergence |
