# CraneSched + SCOW Integrated Computing Solution

CraneSched is deeply integrated with SCOW (Super Computing On Web), forming a complete computing center solution spanning from underlying resource scheduling to upper-level operations management — providing one-stop computing platform services for governments, universities, enterprises, and operators.

---

## About SCOW

SCOW is an integrated "HPC·AI·Quantum·Cloud" computing platform designed for computing centers, addressing the widespread challenges of **difficult operations management, difficult user access, and difficult resource convergence**.

SCOW can simultaneously manage heterogeneous computing resources from different hardware vendors and software stacks, including both HPC and AI computing, providing users and administrators with convenient and comprehensive resource management and usage capabilities.

- **Open-source repository**: <https://github.com/PKUHPC/OPENSCOW>
- **Deployment reach**: Covers 23 provinces and cities nationwide, 70+ computing centers, 53,000+ open-source downloads

---

## Integrated Architecture

The CraneSched + SCOW integrated solution is organized into four layers:

### Application Layer

| Module | Function |
|--------|----------|
| Xiaosu LLM Agent Platform | RAG knowledge base applications, intelligent agent applications |
| MaaS Large Model Service Platform | Combines local compute and cloud services for unified LLM capabilities |
| ShadowDesk Remote Desktop | High-performance remote desktop supporting EDA, CAE simulation, data visualization, etc. |
| Interactive Applications | Visualization-based interactive apps: VSCode, Jupyter, model training, etc. |

### Platform Layer — SCOW Computing Platform

SCOW provides a unified user interface covering three management domains:

**Operations Management**

| Feature | Description |
|---------|-------------|
| Billing & Charging | Flexible billing strategies and charge management |
| Job Management | Platform-wide job monitoring and management |
| User Management | Multi-level user hierarchy management |
| Account Management | Account creation, top-up, and consumption records |
| Identity Authentication | Integration with LDAP and other authentication systems |
| Permission Management | Fine-grained role-based access control |

**Resource Usage**

| Feature | Description |
|---------|-------------|
| Online Job Submission | Submit and manage jobs via web interface |
| Online Resource Requests | Self-service resource request workflow |
| Online Shell Platform | Browser-based terminal access |
| Cross-Cluster File Transfer | Data transfer between multiple clusters |
| Visual Desktop | Remote graphical desktop |
| Visual Applications | Graphical scientific computing applications |

**Resource Management**

| Feature | Description |
|---------|-------------|
| Resource Virtualization | Virtualization support for HPC environments |
| Resource Authorization | Fine-grained resource authorization management |
| Resource Configuration | Flexible resource configuration policies |

### Scheduling Layer — CraneSched

CraneSched serves as the scheduling kernel, providing:

- **High-concurrency scheduling**: schedules over 10,000 jobs per second
- **Hierarchical access control**: tree-structured user/account management
- **Heterogeneous resource integration**: unified management of domestic and international CPUs and accelerators
- **Compatibility**: compatible with Slurm and LSF

### Hardware Layer

Supports connection to various computing resources:

- **HPC resources**: managed by Slurm, CraneSched, etc.
- **AI computing resources**: managed by CraneSched, K8s
- **Quantum resources**: via quantum access systems
- **Cloud resources**: public cloud and private cloud compute

---

## Feature Highlights

### Graphical Interface for Easy Use

SCOW provides multiple web-based features for computing resource usage, lowering the barrier to entry so that users unfamiliar with Linux can successfully utilize computing resources.

### Rich Features, Convenient Management

For computing center operations, SCOW provides full-lifecycle management capabilities covering the entire process, helping computing centers quickly establish management and operational workflows.

### Standardized Platform with Resource Convergence Support

Supports connection to multiple schedulers including Slurm, CraneSched, and K8s, enabling management of diverse computing resources. Provides standardized management interfaces for computing networks to support resource convergence.

### Open, Neutral, and Open-Source

SCOW is independent of all vendors, helping computing centers break vendor lock-in. Initiates and maintains the community project OPENSCOW, open-sourced under the Mulan Permissive Software License.

### HPC·AI·Quantum·Cloud Four-Way Convergence

Connect and manage HPC, AI, quantum, and cloud computing resources on a single platform — the first in China to achieve four-way compute convergence.

### Fast Deployment, Ready Out of the Box

Quickly deployable on new clusters or connectable to existing ones with minimal invasiveness, able to coexist with other management platforms.

---

## Computing Network Convergence — XSCOW

Building on SCOW, the XSCOW computing network convergence platform further enables cross-domain resource integration:

- **Resource management**: unified resource management across computing centers
- **Permission management**: unified authentication and permission systems
- **Job scheduling**: cross-domain job scheduling and dispatch
- **Billing management**: unified billing and payment management
- **End-to-end monitoring**: comprehensive monitoring and auditing

**Application case**: The Ministry of Education's University AI Computing Convergence and Sharing Platform, led by the Ministry of Education's Educational Management Information Center, aims to integrate supercomputing centers across universities nationwide. Jianshan Tatu serves as the technical support provider, with resources from 10 universities and users from 16 universities already connected to the platform.

---

## Typical Deployment Cases

### Peking University Weiming Excellence No.1 Cluster

Fully domestic Huawei equipment, valued at over 30 million RMB. Adopts the complete CraneSched + SCOW integrated solution:

- **Application layer**: Xiaosu intelligent Q&A assistant + ShadowDesk remote desktop + interactive applications (VSCode, Jupyter, etc.)
- **Platform layer**: SCOW computing platform (user management, job management, file management, billing management, application management, image and model management, log management, Shell terminal)
- **Scheduling layer**: CraneSched (high-concurrency scheduling, hierarchical access control, heterogeneous resource integration, compatible with Slurm/LSF/K8s/OpenPBS)
- **Hardware layer**: OpenEuler domestic open-source OS + RoCE high-speed network + Ascend training NPU (910B) + Ascend inference NPU (310P) + Kunpeng CPU (ARM)

### A Chip Design Company

Since 2024, a chip design company (approximately 200 employees) has adopted the CraneSched + SCOW integrated solution to manage its new cluster. The solution resolved the frequent compute node overload caused by unreasonable resource control mechanisms in the previous commercial software, and through deep adaptation of mainstream EDA tools, provides precise and efficient support for various chip design tasks.

---

## Related Links

| Project | Link |
|---------|------|
| SCOW Open-source Repository | <https://github.com/PKUHPC/OPENSCOW> |
| CraneSched Backend Repository | <https://github.com/PKUHPC/CraneSched> |
| CraneSched Frontend Repository | <https://github.com/PKUHPC/CraneSched-FrontEnd> |
| Jianshan Tatu Official Website | <https://csjstt.com> |
