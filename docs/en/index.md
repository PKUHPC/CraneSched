---
hide:
  - navigation
---

# CraneSched

<p class="lead">A distributed scheduling system for HPC and AI workloads — built for performance, scale, and simplicity.</p>

[Get started](deployment/index.md){ .md-button .md-button--primary } [Try the demo](https://hpc.pku.edu.cn/demo/cranesched){ .md-button } [GitHub](https://github.com/PKUHPC/CraneSched){ .md-button }

---

## Why CraneSched?

<div class="grid cards" markdown>

- :material-speedometer: **Performance**

    ---

    Over 100k scheduling decisions per second with fast job–resource matching.

- :material-arrow-expand: **Scalability**

    ---

    Proven design for million-core clusters and large-scale deployments.

- :material-account-cog: **Usability**

    ---

    Clean, consistent CLI for users and admins (cbatch, cqueue, crun, calloc, cinfo…).

- :material-shield-lock: **Security**

    ---

    RBAC and encrypted communication out of the box.

- :material-heart-pulse: **Resilience**

    ---

    Automatic job recovery, no single point of failure, fast state restoration.

- :material-source-repository: **Open Source**

    ---

    Community-driven and extensible with a pluggable architecture.

</div>

---

## Quick Start

<div class="grid cards" markdown>

- :material-rocket-launch: **Deploy Backend** (Rocky Linux 9)

    ---

    Recommended for production.

    [Open guide →](deployment/backend/Rocky9.md)

- :material-cog: **Configure Cluster**

    ---

    Database, partitions, nodes, and policies.

    [Database](deployment/configuration/database.md) • [Config](deployment/configuration/config.md)

- :material-code-tags: **Deploy Frontend**

    ---

    User tools and services (CLI, cfored, cplugind).

    [Open guide →](deployment/frontend/frontend.md)

- :material-console-line: **Run Your First Job**

    ---

    Batch: [cbatch](command/cbatch.md) • Interactive: [crun](command/crun.md), [calloc](command/calloc.md)

</div>

---

## Architecture

![CraneSched architecture](images/Architecture.png)

CraneSched supports both HPC and AI workloads:

- HPC jobs: the Cgroup Manager allocates resources and provides cgroup-based isolation.
- AI jobs: the Cgroup Manager + CRI runtime handles resource allocation and container lifecycle management.

---

## CLI Reference

- User commands: [cbatch](command/cbatch.md), [cqueue](command/cqueue.md), [crun](command/crun.md), [calloc](command/calloc.md), [cinfo](command/cinfo.md)
- Admin commands: [cacct](command/cacct.md), [cacctmgr](command/cacctmgr.md), [ceff](command/ceff.md), [ccontrol](command/ccontrol.md), [ccancel](command/ccancel.md)
- Exit codes: [reference](reference/exit_code.md)

---

## Links

- Demo cluster: <https://hpc.pku.edu.cn/demo/cranesched>
- Backend: <https://github.com/PKUHPC/CraneSched>
- Frontend: <https://github.com/PKUHPC/CraneSched-FrontEnd>

---

## License

CraneSched is dual-licensed under AGPLv3 and a commercial license. See `LICENSE` or contact mayinping@pku.edu.cn for commercial licensing.
