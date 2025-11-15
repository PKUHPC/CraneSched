## CraneSched Latest Updates

[2025/04/08] v1.1.2: Upgraded toolchains to GCC 15 / Clang 20; added node drain/resume events, partition-based account control; integrated HashiCorp Vault.

[2025/01/24] v1.1.0: Added X11 forwarding, user QoS limits, multiple GID support; support for cgroup v2 and Ascend NPU; improved scheduler and event library.

[2024/10/24] v1.0.0: Added job monitoring, plugin modules, and device support; enhanced scheduler; improved resource and job management; IPv6 support.

# Introduction

[[中文](./README.md) | English]

**CraneSched** is an open-source distributed scheduler for HPC and AI workloads, developed by the High-Performance Computing (HPC) Public Platform at Peking University. It covers the core needs of job scheduling: resource management and monitoring, job submission/query, and resource isolation. The backend is implemented in C++, and the frontend in Go.

We welcome community contributions.

**Frontend**: [CraneSched-FrontEnd](https://github.com/PKUHPC/CraneSched-FrontEnd)

**Backend**: [CraneSched](https://github.com/PKUHPC/CraneSched)

**Documentation**: <https://pkuhpc.github.io/CraneSched/>

**Demo cluster** (test account: demo_admin / demo_admin): <https://hpc.pku.edu.cn/demo/cranesched>

# Highlights

- **Performance**: >100,000 scheduling decisions per second
- **Scalability**: Scales to million-core clusters
- **Usability**: Simple user and admin commands
- **Security**: RBAC and encrypted communication
- **Resilience**: Automatic job recovery, no SPOF, fast state recovery
- **Open Source**: All code available

# Architecture

**Cranectld** is the control plane: manages node lifecycles, schedules queues, manages resources, and processes job submission/modification/query.

**Craned** runs on compute nodes: monitors resources and job status, receives user commands, forwards them to Cranectld, and returns results.

![Architecture](./docs/images/Architecture.png)

CraneSched introduces a **Resource Manager** to support both HPC and AI workloads:

- For **HPC jobs**: the **Cgroup Manager** allocates resources and isolates jobs via cgroups.
- For **AI jobs**: the **Container Manager** allocates resources with Kubernetes, packages apps into containers, and manages their lifecycle.
- Additionally, for **containerized workloads** (experimental): supports CRI (Container Runtime Interface) with runtimes like containerd or CRI-O for running containerized applications.

# Application Scenarios

CraneSched serves mixed HPC + AI workloads across distributed clusters. Clusters can be connected via cloud, and the scheduler places jobs on the most available cluster to improve utilization and reduce wait time.

![Scenario](./docs/images/Scenario.png)