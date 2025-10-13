# Overview

!!! tip
    We strongly recommend using **Rocky Linux 9** for production environments. It offers better stability, long-term support, and compatibility with modern kernels and system components.

This section provides step-by-step guides to deploy and configure CraneSched components on different operating systems.

## What you will deploy

The CraneSched typically includes the following services:

### Backend service
- cranectld: the central control and scheduling service (runs on control node[s]).
- craned: the worker node daemon (runs on every compute/worker node).

### Frontend Service
- cfored: daemon service for crun and calloc.
- cplugind: daemon service for plugins.
- command line tools: cbatch, ccontrol, cacct, cacctmgr, cqueue, cinfo, crun, etc.

### Configuration files

- Configuration files: global config is typically at `/etc/crane/config.yaml`; database settings at `/etc/crane/database.yaml`.

## Prerequisites

!!! tip
    These are general prerequisites; specific guides may have additional requirements.

- **Privileges**: Root or sudo on all nodes.
- **Networking**: Ensure inter-node connectivity and name resolution (DNS or /etc/hosts).
- **Time sync**: Enable **NTP** or **chrony** to keep clocks synchronized.
- **Cgroups**: Mount and configure **cgroup v1 or v2** on all nodes. If using **cgroup v2**, set up **eBPF** for GRES (see eBPF guide).
- **Systemd**: Use **systemd** to manage services (recommended).


## OS and topic-specific guides

Choose the guide that matches your OS:

- [Rocky Linux 9 guide (recommended)](<./Backend/Rocky9.md>)
- [CentOS 7 guide (legacy)](<./Backend/CentOS7.md>)

If cgroup v2 is used, refer to the following guide: 

- [eBPF for GRES on cgroup v2](<./Backend/eBPF.md>)

After backend setup, deploy frontend components as needed:

- [Frontend configuration](./Frontend.md)
