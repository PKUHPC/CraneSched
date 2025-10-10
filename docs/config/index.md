# Deployment and Configuration

This section provides step-by-step guides to deploy and configure CraneSched backend components on different operating systems.

⚠️ Important: We strongly recommend using Rocky Linux 9 for production backend environments. It offers better stability, long-term support, and compatibility with modern kernels and system components.

• Preferred: Rocky Linux 9 — follow the recommended path for the best compatibility and experience.
• Compatible: CentOS 7 — for legacy environments only; lifecycle and ecosystem are outdated.
• Optional/Required for GRES on cgroup v2: eBPF — used to enforce GRES device limits under cgroup v2; enable on supported kernels if you need GRES.

## What you will deploy

The CraneSched backend typically includes the following services:

- cranectld: the central control and scheduling service (runs on control node[s]).
- craned: the worker node daemon (runs on every compute/worker node).

Common accompaniments:

- System services: use systemd to manage services (this repo provides `cranectld.service.in` and `craned.service.in` templates).
- Configuration files: global config is typically at `/etc/config.yaml`; database settings at `/etc/database.yaml`.

For frontend-related configuration, see the “Frontend” document in the parent directory.

## OS- and topic-specific guides

- [Rocky Linux 9 guide (recommended)](./Backend/Rocky9.md)
- [CentOS 7 legacy support](./Backend/Centos7.md)
- [eBPF for GRES on cgroup v2](./Backend/EBPF.md)

More general configuration and overview:

- [Command Line Tool configuration](../Frontend.md)

## Prerequisites and recommendations

- Administrative privileges: root or a user with sudo.
- Time sync: enable NTP/chrony to keep node clocks consistent.
- Networking and hostnames: ensure inter-node connectivity and proper name resolution (DNS or hosts).
- systemd: a Linux distribution using systemd to manage services (e.g., Rocky Linux 9).
- Optional kernel capabilities: if using eBPF, ensure kernel and dependencies meet requirements.

## Quick start

1) Choose your OS guide:
    - [Preferred Rocky Linux 9](./Backend/Rocky9.md)
    - [Legacy environments](./Backend/Centos7.md)

2) Install and start services per the guide:
    - Configure and start cranectld (control node)
    - Deploy and start craned on all worker nodes

3) Optional/Required for GRES (cgroup v2):
    - If you use GRES on cgroup v2, [enable eBPF](./Backend/EBPF.md)

After completing backend setup, continue with the configuration overview and frontend docs in the parent directory to validate end-to-end functionality.
# Backend Deployment and Configuration

This section provides step-by-step guides to deploy and configure CraneSched backend components on different operating systems.

⚠️ Important: We strongly recommend using Rocky Linux 9 for production backend environments. It offers better stability, long-term support, and compatibility with modern kernels and system components.

• Preferred: Rocky Linux 9 — follow the recommended path for the best compatibility and experience.
• Compatible: CentOS 7 — for legacy environments only; lifecycle and ecosystem are outdated.
• Optional/Required for GRES on cgroup v2: eBPF — used to enforce GRES device limits under cgroup v2; enable on supported kernels if you need GRES.

## What you will deploy

The CraneSched backend typically includes the following services:

- cranectld: the central control and scheduling service (runs on control node[s]).
- craned: the worker node daemon (runs on every compute/worker node).

Common accompaniments:

- System services: use systemd to manage services (this repo provides `cranectld.service.in` and `craned.service.in` templates).
- Configuration files: global config is typically at `/etc/config.yaml`; database settings at `/etc/database.yaml`.

For frontend-related configuration, see the “Frontend” document in the parent directory.

## OS- and topic-specific guides

- [Rocky Linux 9 guide (recommended)](./Backend/Rocky9.md)
- [CentOS 7 legacy support](./Backend/Centos7.md)
- [Command Line Tool configuration](Frontend.md)
- [eBPF for GRES on cgroup v2](./Backend/EBPF.md)

## Prerequisites and recommendations

- Administrative privileges: root or a user with sudo.
- Time sync: enable NTP/chrony to keep node clocks consistent.
- Networking and hostnames: ensure inter-node connectivity and proper name resolution (DNS or hosts).
- systemd: a Linux distribution using systemd to manage services (e.g., Rocky Linux 9).
- Optional kernel capabilities: if using eBPF, ensure kernel and dependencies meet requirements.

## Quick start

1) Choose your OS guide:
    - [Preferred Rocky Linux 9](./Backend/Rocky9.md)
    - [Legacy environments](./Backend/Centos7.md)

2) Install and start services per the guide:
    - Configure and start cranectld (control node)
    - Deploy and start craned on all worker nodes

3) Optional/Required for GRES (cgroup v2):
    - If you use GRES on cgroup v2, [enable eBPF](./Backend/EBPF.md)

After completing backend setup, continue with the configuration overview and frontend docs in the parent directory to validate end-to-end functionality.
