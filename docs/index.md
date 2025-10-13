<!-- Home page for the CraneSched documentation site (MkDocs) -->

# CraneSched Documentation

A distributed intelligent scheduler for HPC and AI workloads, designed for performance, scale, and simplicity.

---

## What is CraneSched?

CraneSched is an open-source, distributed scheduling system developed by the HPC Public Platform at Peking University. It unifies HPC and AI scheduling with efficient resource management, robust isolation, and a modern architecture.

Key components:

- Cranectld: the central controller and scheduler (control node)
- Craned: the node agent/daemon (compute nodes)
- Frontend tools and services: CLI commands (cbatch, cqueue, crun, etc.), cfored, cplugind

---

## Highlights

- **Unified workloads:** HPC and AI job payload in a single, integrated system
- **High throughput:** 100k+ scheduling decisions per second for rapid job–resource matching
- **Massive scalability:** designed to manage clusters with millions of cores
- **User-friendly:** concise, consistent user and admin commands and workflows
- **Security-conscious:** role-based access control (RBAC), mTLS-encrypted communications, and secure-by-default configurations
- **Designed for resilience:** automatic job recovery, no single point of failure, and fast state restoration
- **Open and extensible:** community-driven, pluggable architecture for customization and integration

---

## Quick start

1. Choose your deployment guide:

	 - [Backend (Rocky 9, recommended)](<./deployment/Backend/Rocky9.md>)
	 - [Backend (CentOS 7, legacy)](<./deployment/Backend/CentOS7.md>)
	 - [Frontend components](./deployment/Frontend.md)
	 - [eBPF for GRES on cgroup v2](<./deployment/Backend/eBPF.md>)

2. Install and start services:

	 - Start cranectld on control node(s)
	 - Start craned on all compute nodes
	 - Deploy optional frontend services where needed (cfored, cplugind)

3. Submit a job

	 - [Batch jobs](./command/cbatch.md)
	 - Interactive jobs: [crun](./command/crun.md) and [calloc](./command/calloc.md)

> Tip: Prefer to complete backend setup first, then deploy frontend tools to login/control/compute nodes as needed.
> Note: For production deployments we recommend **Rocky Linux 9** for better stability and kernel compatibility.

---

## Documentation Index

- Deployment & Configuration
	- [Overview](./deployment/index.md)
	- [Backend (Rocky 9)](<./deployment/Backend/Rocky9.md>)
	- [Backend (CentOS 7)](<./deployment/Backend/CentOS7.md>)
	- [eBPF for GRES (cgroup v2)](<./deployment/Backend/eBPF.md>)
	- [Frontend deployment](./deployment/Frontend.md)

- User & Admin Commands
	- Jobs: [cbatch](./command/cbatch.md) · [cqueue](./command/cqueue.md) · [ccancel](./command/ccancel.md)
	- Interactive: [crun](./command/crun.md) · [calloc](./command/calloc.md)
	- Info & control: [cinfo](./command/cinfo.md) · [ccontrol](./command/ccontrol.md)
	- Accounting: [cacct](./command/cacct.md) · [cacctmgr](./command/cacctmgr.md) · [ceff](./command/ceff.md)

- Reference
	- [Exit codes](./referrence/exit_code.md)

---

## Architecture

![CraneSched architecture](./images/Architecture.png)

CraneSched introduces a Resources Manager abstraction to handle different workload types:

- HPC jobs: Cgroup Manager allocates resources and provides cgroup-based isolation
- AI jobs: Container Manager leverages Kubernetes for resource allocation and container lifecycle management

---

## Demo and repositories

- [Demo cluster](https://hpc.pku.edu.cn/demo/cranesched)
- [Backend repository](https://github.com/PKUHPC/CraneSched)
- [Frontend repository](https://github.com/PKUHPC/CraneSched-FrontEnd)

---

## Latest updates

- 2025-04-08 — v1.1.2: GCC 15/Clang 20 toolchains, node drain/resume events, partition account control, Vault integration
- 2025-01-24 — v1.1.0: X11 forwarding, user QoS limits, multi-GID, cgroupv2 & Ascend NPU, scheduler/event optimizations
- 2024-10-24 — v1.0.0: Job monitoring, plugins, device support, IPv6, resource & job management improvements

---

## License

CraneSched is dual-licensed under AGPLv3 and a commercial license. See `LICENSE` for details or contact mayinping@pku.edu.cn for commercial licensing.

---

## Need help?

- Browse the docs via the sections above
- Open an issue on GitHub: [https://github.com/PKUHPC/CraneSched/issues](https://github.com/PKUHPC/CraneSched/issues)
- Contributions are welcome. See the repositories for guidelines.
