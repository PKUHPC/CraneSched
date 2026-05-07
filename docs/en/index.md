---
hide:
  - navigation
---

# CraneSched

<p class="lead">A next-generation open-source compute scheduler for unified HPC and AI workloads.</p>

<p>CraneSched is a distributed job scheduling system for HPC and AI workloads. It unifies supercomputing and AI compute resources on a single platform, delivering efficient, stable, and reliable scheduling for education, industry, meteorology, and more.</p>

<p>It is jointly developed by Peking University, the Changsha Institute of Computing and Digital Economy of Peking University, and Changsha Jianshan Tatu Technology Co., Ltd.</p>

[Get Started](deployment/index.md){ .md-button .md-button--primary } [Try the Demo](https://hpc.pku.edu.cn/demo/cranesched){ .md-button } [GitHub](https://github.com/PKUHPC/CraneSched){ .md-button }

---

## Why CraneSched?

<div class="grid cards" markdown>

- :material-speedometer: **Industry-Leading Performance**

    ---

    Scheduling throughput 5–20x faster than Slurm; real-time scheduling of over 10,000 jobs per second; supports 2,000,000+ concurrent jobs; job dispatch latency under 10 ms. Supports clusters with 100,000+ nodes.

    [:octicons-arrow-right-24: View performance comparison](features/slurm-comparison.md#performance-comparison)

- :material-merge: **Deep HPC+AI Convergence**

    ---

    One cluster handles both HPC and AI workloads, covering all HTC+HPC+AI computing scenarios. Compute, storage, and data resources are pooled for efficient sharing — no more resource silos.

    [:octicons-arrow-right-24: Learn about the convergence solution](features/convergence-solution.md)

- :material-chip: **Unified Heterogeneous Hardware Management**

    ---

    Supports X86, ARM, and RISC-V architectures; adapts to Intel, AMD, Phytium, and Kunpeng CPUs; supports Nvidia and AMD GPUs as well as Huawei Ascend, Cambricon, and Kunlunxin accelerators.

    [:octicons-arrow-right-24: View compatibility list](features/slurm-comparison.md#heterogeneous-hardware)

- :material-brain: **Intelligent Algorithms for Efficiency and Energy Savings**

    ---

    In-house ORA job runtime prediction algorithm (published at CCF-B conference ICS) with 41% accuracy improvement; in-house TSMF algorithm significantly improves resource utilization; in-house EcoSched algorithm reduces cluster energy consumption by 78.64% under low load.

    [:octicons-arrow-right-24: Learn about scheduling algorithms](features/convergence-solution.md#scheduling-algorithms)

- :material-swap-horizontal: **Full Slurm/LSF Command Compatibility**

    ---

    In-house Slurm & LSF Wrapper enables zero-cost, transparent migration — users need not modify any scripts or workflows.

    [:octicons-arrow-right-24: View compatibility](features/slurm-comparison.md#command-compatibility)

- :material-monitor-dashboard: **CraneSched + SCOW Integrated Solution**

    ---

    Deeply integrated with the SCOW computing platform, providing closed-loop lifecycle management spanning resource management, job scheduling, monitoring, and billing — an all-in-one "HPC·AI·Quantum·Cloud" computing service.

    [:octicons-arrow-right-24: Learn about the integrated solution](features/scow-integration.md)

</div>

---

## Solutions

<div class="grid cards" markdown>

- :material-database-sync: **Storage·Compute·Usage Convergence for HPC+AI**

    ---

    Traditional approaches deploy HPC and AI clusters independently, making compute, storage, and data sharing difficult and creating resource silos. CraneSched's HPC+AI convergence solution delivers:

    - **Compute convergence**: one cluster handles both HPC and AI workloads
    - **Storage convergence**: unified storage with pooled data resources
    - **Usage convergence**: unified platform simplifies operations, unified user authentication and resource management

    [:octicons-arrow-right-24: Learn more](features/convergence-solution.md)

- :material-application-brackets: **CraneSched + SCOW Integrated Computing Solution**

    ---

    CraneSched is deeply integrated with SCOW (Super Computing On Web), forming a complete computing center solution:

    - **Operations management**: billing, user management, account management, identity authentication, permission management
    - **Resource usage**: online job submission, shell platform, visual desktop, cross-cluster file transfer
    - **Resource management**: resource virtualization, resource authorization, resource configuration

    [:octicons-arrow-right-24: Learn more](features/scow-integration.md)

</div>

---

## Feature Completeness

CraneSched comprehensively benchmarks against Slurm in scheduling capabilities, and surpasses it in several key areas.

| Feature | CraneSched | Slurm | LSF | Description |
|---------|:----------:|:-----:|:---:|-------------|
| Backfill Scheduling | :material-check-circle:{ .success } | :material-check-circle:{ .success } | :material-check-circle:{ .success } | Run short jobs in idle time windows to improve utilization |
| Fair-Share Scheduling | :material-check-circle:{ .success } | :material-check-circle:{ .success } | :material-check-circle:{ .success } | Fair scheduling policy based on historical usage |
| Preemption | :material-check-circle:{ .success } | :material-check-circle:{ .success } | :material-check-circle:{ .success } | High-priority jobs preempt resources from lower-priority ones |
| Reservation | :material-check-circle:{ .success } | :material-check-circle:{ .success } | :material-check-circle:{ .success } | Reserve resource time windows for specific users or jobs |
| Power Saving Scheduling | :material-check-circle:{ .success } | :material-check-circle:{ .success } | :material-check-circle:{ .success } | Automatically shut down idle nodes under low load |
| TRES Fine-Grained Tracking | :material-check-circle:{ .success } | :material-check-circle:{ .success } | :material-check-circle:{ .success } | Trackable resource types (CPU, memory, GPU, etc.) |
| Job Dependencies | :material-check-circle:{ .success } | :material-check-circle:{ .success } | :material-check-circle:{ .success } | Control dependency relationships between jobs |
| Job Arrays | :material-check-circle:{ .success } | :material-check-circle:{ .success } | :material-check-circle:{ .success } | Batch submission of parameterized jobs |
| QOS Management | :material-check-circle:{ .success } | :material-check-circle:{ .success } | :material-check-circle:{ .success } | Differentiated service level control |
| Native Container Orchestration (CRI/CNI) | :material-check-circle:{ .success } | :material-check-circle:{ .success } | :material-check-circle:{ .success } | Native container orchestration based on CRI/CNI standards |
| Multi-Tenant Container Network Isolation | :material-check-circle:{ .success } | :material-close-circle:{ .danger } | :material-close-circle:{ .danger } | CNI-based multi-tenant network isolation (Calico Underlay) |
| Container RDMA Network Support | :material-check-circle:{ .success } | :material-close-circle:{ .danger } | :material-close-circle:{ .danger } | Supports SR-IOV shared RNIC and direct passthrough |
| Extended Hardware Compatibility | :material-check-circle:{ .success } | :material-close-circle:{ .danger } | :material-close-circle:{ .danger } | Supports diverse CPU architectures (x86, ARM, RISC-V) and accelerators from multiple vendors including Nvidia, AMD, Huawei Ascend, and more |
| HPC+AI Converged Scheduling | :material-check-circle:{ .success } | :material-close-circle:{ .danger } | :material-close-circle:{ .danger } | One cluster handles both HPC and AI workloads |
| AI Job Runtime Prediction | :material-check-circle:{ .success } | :material-close-circle:{ .danger } | :material-close-circle:{ .danger } | LLM-based job runtime prediction with 41% accuracy improvement |

[:octicons-arrow-right-24: View full feature comparison](features/slurm-comparison.md){ .md-button }

---

## Use Cases

CraneSched is suitable for a wide variety of computing scenarios:

<div class="grid cards" markdown>

- :material-weather-cloudy: **Traditional HPC**

    ---

    Aerodynamics simulation, atmospheric modeling, high-energy physics research, and more. Supports mainstream applications including WRF, OpenFOAM, CMAQ, CESM, ABAQUS, and GROMACS.

- :material-robot: **AI Computing**

    ---

    Efficient training and inference for large models including DeepSeek, Qwen, Llama, CPMBee, and ChatGLM. Supports multiple container environments including Docker and Singularity.

- :material-integrated-circuit-chip: **Chip Design**

    ---

    Supports EDA chip design and other high-throughput computing workloads with extremely demanding scheduling requirements. Deep adaptation for mainstream EDA tools from Cadence, Synopsys, and others.

- :material-flask: **Scientific Research**

    ---

    Big data analytics, biopharmaceutical design, battery material research, medical large models, and other research scenarios.

</div>

---

## Deployment Cases

CraneSched is deployed and in production across **8 provinces and cities** and **10+ computing centers** nationwide.

<div class="grid cards" markdown>

- :material-school: **Peking University Weiming Teaching Cluster No.2**

    ---

    Launched in June 2024. Supports real-time online teaching and research for faculty and students, over 300 credit-hours of online teaching, and compatibility with hundreds of user software packages. Transparently migrated from Slurm to CraneSched with no user disruption; stable operation ever since.

- :material-brain: **Peking University Weiming Excellence No.1 Cluster**

    ---

    Launched in November 2024. Fully domestic Huawei Ascend and Kunpeng architecture — the first university-level cluster in China to adopt an all-domestic HPC+AI converged solution. Runs large model training and inference tasks for DeepSeek, Qwen, Llama, and more.

</div>

**Additional deployments**: Institute of Software, Chinese Academy of Sciences; Tianjin University; Beijing Union University; Nanjing University of Aeronautics and Astronautics; Guizhou University of Finance and Economics; Ocean University of China; and others.

**Awards**: Selected for MIIT "Typical Application Cases" and "Key Recommended Application Cases" in 2024; selected for the "2024 Education Information Technology Application Innovation Outstanding Case Collection."

---

## Technical Highlights

<div class="grid cards" markdown>

- :material-speedometer: **High Performance**

    ---

    Over 100,000 scheduling decisions per second with fast job–resource matching.

- :material-arrow-expand: **Scalability**

    ---

    Proven design for million-core clusters and large-scale deployments.

- :material-account-cog: **Usability**

    ---

    Clean, consistent CLI for users and admins (cbatch, cqueue, crun, calloc, cinfo, etc.).

- :material-shield-lock: **Security**

    ---

    Built-in RBAC and encrypted communication; fully open-source and self-controlled; compliant with domestic technology security standards.

- :material-heart-pulse: **Resilience**

    ---

    Automatic job recovery, no single point of failure, fast state restoration. Distributed fault-tolerant design for stable and reliable operation.

- :material-source-repository: **Open Source**

    ---

    Licensed under AGPLv3; community-driven and extensible with a pluggable architecture.

</div>

---

## CLI Reference

- User commands: [cbatch](command/cbatch.md), [cqueue](command/cqueue.md), [crun](command/crun.md), [calloc](command/calloc.md), [cinfo](command/cinfo.md)
- Admin commands: [cacct](command/cacct.md), [cacctmgr](command/cacctmgr.md), [ceff](command/ceff.md), [ccontrol](command/ccontrol.md), [ccancel](command/ccancel.md)
- Container commands: [ccon](command/ccon.md)
- Exit codes: [reference](reference/exit_code.md)

---

## Links

- Demo cluster: <https://hpc.pku.edu.cn/demo/cranesched>
- Backend repository: <https://github.com/PKUHPC/CraneSched>
- Frontend repository: <https://github.com/PKUHPC/CraneSched-FrontEnd>
- SCOW computing platform: <https://github.com/PKUHPC/OPENSCOW>

---

## License

CraneSched is dual-licensed under AGPLv3 and a commercial license. See `LICENSE` or contact mayinping@pku.edu.cn for commercial licensing.

---
