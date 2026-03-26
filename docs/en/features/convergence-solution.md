# Storage·Compute·Usage Convergence Solution for HPC+AI

CraneSched provides an HTC+HPC+AI unified convergence solution, achieving full integration of storage, compute, and usage — one cluster for all computing workloads.

---

## Background and Pain Points

The current compute scheduling market faces the following key challenges:

### Scheduling Systems Dominated by US Vendors

- Among 12 publicly known national supercomputing centers, 11 use US-made Slurm and 1 uses LSF
- Among the top 22 universities, 15 use Slurm, 5 use LSF, and 2 use PBS
- In the AI computing domain, Kubernetes (K8s) holds a de facto monopoly

### Difficulty Converging HPC and AI Computing

Independent deployment of HPC and AI clusters leads to fragmented resources — compute, storage, and data cannot be shared, creating resource silos and waste.

### Poor Compatibility with Domestic Ecosystems

High adaptation costs for domestic chip ecosystems; incomplete software support on domestic platforms.

### Low Performance of Existing Scheduling Systems

High response latency under heavy throughput; low job scheduling efficiency.

---

## Traditional Approach vs. CraneSched Convergence

| Dimension | Traditional Approach | CraneSched Convergence |
|-----------|---------------------|----------------------|
| **Scheduling** | Slurm/LSF for HPC, K8s for AI — two separate systems | CraneSched unified scheduling for HPC + AI |
| **Storage** | Separate storage for HPC and AI | Unified storage with pooled data resources |
| **Portal** | Separate HPC and AI portals | Unified portal (SCOW) for both HPC and AI |
| **Resource Sharing** | Compute, storage, data siloed and hard to share | Compute, storage, data pooled and efficiently shared |
| **Job Scheduling** | Difficult multi-platform coordination, low data flow efficiency | Unified scheduler with global coordination |
| **Operations** | Fragmented management systems, high complexity | Integrated platform simplifies operations |
| **User Experience** | Complex authentication and usage | Unified user authentication and resource management |

---

## Three-Layer Convergence: Storage · Compute · Usage

### Compute Convergence ("Compute")

CraneSched natively supports both HPC and AI computing workloads:

**HPC Workloads**

CraneSched is a purpose-built HPC scheduler, compatible with all HPC applications via its Slurm & LSF Wrapper:

| Domain | Typical Software |
|--------|-----------------|
| Atmosphere, Ocean & Environment | WRF, OpenFOAM, CMAQ |
| Astronomy & Geophysics | CESM, iCESM, Fds, Salome |
| Industrial Design & Manufacturing | ABAQUS, Ansys Fluent |
| New Energy & Materials | MPB, CP2K, GROMACS |

**AI Workloads**

CraneSched provides the `ccon` command for native containerized AI jobs:

- Supports training and inference of large models: DeepSeek, Qwen, Llama, CPMBee, ChatGLM, etc.
- Supports OCI-standard containers (Docker, Podman, containerd) and Singularity
- Supports multi-node container jobs with intra-container networking
- Supports automatic container image pulling
- Full container lifecycle management: start, stop, exec, logs, etc.

**HTC Workloads**

Supports high-throughput computing (HTC) scenarios such as chip design that demand extremely high scheduler throughput.

### Storage Convergence ("Storage")

Traditional approaches use separate storage for HPC and AI, making data sharing impossible. CraneSched's convergence solution provides unified storage for both:

- A single storage system serves both HPC and AI jobs simultaneously
- Container jobs can directly access shared file systems (with Fake Root support)
- Pooled data resources eliminate data movement overhead

### Usage Convergence ("Usage")

A unified CraneSched + SCOW platform achieves convergence at the user layer:

- **Unified authentication**: one account system for all resources
- **Unified resource management**: one platform manages both HPC and AI resources
- **Unified job submission**: users need not care whether the underlying resource is HPC or AI
- **Unified monitoring and billing**: closed-loop lifecycle management

---

## CraneSched Container Orchestration

CraneSched uses **imperative orchestration** (Slurm-style), allowing users to mix host and container operations for maximum flexibility:

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

### Comparison with K8s

| Dimension | CraneSched | K8s |
|-----------|-----------|-----|
| Orchestration style | Imperative (Slurm-style) | Declarative (YAML) |
| Flexibility | Mix host and container operations | Requires custom Controllers |
| Concept mapping | Pod → Job, Container → Step | Native Pod/Container |
| Use case | HPC + AI converged workloads | Cloud-native microservices |

---

## In-House Scheduling Algorithms { #scheduling-algorithms }

CraneSched develops multiple innovative algorithms to comprehensively optimize scheduling efficiency and energy efficiency.

### ORA Job Runtime Prediction Algorithm

**Published at CCF-B conference ICS (2025)**

- First use of a large language model (LLM) to predict HPC job runtimes
- Uses an online-updated historical job vector database to address prediction accuracy degradation caused by shifting job distributions
- Uses diff-based in-context learning to highlight differences between historical and current jobs, mitigating the impact of high-redundancy retrieved samples on prediction accuracy
- **Job runtime prediction accuracy improved by 41%**

### TSMF Fair-Share Scheduling Algorithm

**Published as a cover paper in CCF-B Chinese journal *Computer Science***

- Validated on 3 real clusters at Peking University's university-level HPC platform
- Uses GBDT (Gradient Boosting Decision Tree) for job time prediction
- **Average job queue time reduced by 13.6 minutes**
- **CPU utilization improved to 97.3% under 90% cluster load**
- User queue experience metric (lower is better) reduced by an average of 50.53%

### EcoSched Power-Saving Scheduling Algorithm

Automated power control scheduling algorithm:

- **Total cluster energy savings**: In simulation, cluster energy consumption without optimization was 22,220.29 kWh; EcoSched reduced it to 4,746.64 kWh — **a 78.64% reduction in total energy consumption**
- **Electricity cost savings**: Approximately 10,484 RMB in simulated electricity cost savings, significantly reducing operating overhead
- **Improved resource utilization**: Optimized scheduling strategies significantly increase cluster resource utilization and reduce idle time

---

## Heterogeneous Resource Integration

CraneSched fully supports mainstream domestic and international hardware for true unified heterogeneous resource management:

| Dimension | Supported |
|-----------|----------|
| **Architecture** | X86, ARM, RISC-V |
| **CPU (International)** | Intel, AMD |
| **CPU (Domestic)** | Phytium, Hygon, Huawei Kunpeng |
| **Accelerator (International)** | Nvidia GPU, AMD GPU |
| **Accelerator (Domestic)** | Huawei Ascend, Hygon DCU, Cambricon MLU, Iluvatar CoreX, Kunlunxin, Metax, Moore Threads |
| **OS (International)** | CentOS, Ubuntu, Rocky Linux |
| **OS (Domestic)** | OpenEuler, KylinOS |

---

## Use Cases

CraneSched's Storage·Compute·Usage convergence solution applies to a wide range of industries:

| Industry | Typical Applications |
|----------|---------------------|
| Aerospace | Aerodynamics simulation, aircraft design |
| Smart Manufacturing | Industrial simulation, digital twins |
| Biopharmaceuticals | Molecular dynamics simulation, drug design |
| Geophysics | Atmospheric simulation, climate prediction |
| New Energy & Materials | Battery material research, catalyst design |
| Autonomous Driving | Self-driving model training |
| Smart Cities | City brain, traffic optimization |
| Smart Healthcare | Medical large models, image analysis |
| Smart Finance | Financial AI assistants, risk control models |
| Smart Education | Education large models, personalized learning |
| Chip Design | EDA toolchains, chip verification |

---

## Recognitions

- Selected for the Ministry of Industry and Information Technology (MIIT) "Typical Application Cases" and "Key Recommended Application Cases" lists in 2024
- Selected for the Ministry of Education's domestic technology application case collection
- Multiple invention patents and software copyrights granted
- Participated in drafting IEEE international standards and multiple national standards
- Deployed in 10+ computing centers across 8 provinces and cities nationwide
