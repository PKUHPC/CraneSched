# Deployment Guide of Frontend Components

!!! tip
    This tutorial has been verified on **Rocky Linux 9**. It is expected to work on other systemd-based distributions, such as **Debian, Ubuntu, AlmaLinux, and Fedora**.

    This tutorial targets the **x86-64** architecture. For other architectures (for example, **ARM64**), adjust download links and commands accordingly.

This guide assumes a demo cluster with the following nodes:

- **login01**: User login and job submission node.
- **cranectld**: Control node.
- **crane[01-04]**: Compute nodes.

Run all commands as the root user. Make sure the backend environment is in place before proceeding.

## Overview

A brief overview of the main frontend components you will install and run:

- CLI tools (`cbatch`, `cqueue`, `cinfo`, etc.):
  
    - User-facing command-line utilities for job submission, queue and job status queries, accounting, and job control.
    - Designed to be lightweight and distributed to login nodes. They communicate with the control node (`cranectld`).

- `cfored` (interactive job daemon):
  
    - Provides support for interactive jobs (used by `crun`, `calloc`).
    - Typically runs on login nodes where interactive jobs are submitted. Managed by systemd as `cfored.service`.

- `cplugind` (plugin daemon):
  
    - Loads and manages plugins (mail, monitor, energy, event, etc.) and exposes plugin services to CraneSched components.
    - Must run on nodes that need plugin functionality. Plugin `.so` files and configuration are registered in `/etc/crane/plugin.yaml`.

## Deployment Strategy

Frontend components are deployed through RPM/DEB packages by default.

If you do not yet have `cranesched-frontend` and `cranesched-plugin` packages, follow the [Packaging Guide](../packaging.md) to install the build dependencies and generate them.

Install `cranesched-frontend` on login nodes and any other nodes that need CLI tools. Install `cranesched-plugin` only on nodes that need plugin functionality.

!!! note "Other installation methods"
    The project's GitHub Action uploads RPM/DEB artifacts after every master build. These artifacts have not been fully tested and are only suitable for quick validation.

    In addition, you can also use the source installation method at the end of this page without generating RPM/DEB packages.

## Install Frontend Packages

Distribute the generated packages to the target nodes, then install them with the appropriate package manager:

```bash
# RPM systems: install CLI tools and cfored on login nodes
sudo dnf install /tmp/cranesched-frontend-*.rpm

# RPM systems: install only on nodes that need plugin functionality
sudo dnf install /tmp/cranesched-plugin-*.rpm

# DEB systems: install CLI tools and cfored on login nodes
sudo apt install ./cranesched-frontend_*.deb

# DEB systems: install only on nodes that need plugin functionality
sudo apt install ./cranesched-plugin_*.deb
```

## Enable and Verify Services

```bash
# cfored is needed on login nodes for interactive jobs
pdsh -w login01 systemctl enable --now cfored

# cplugind runs on nodes that need plugin features
pdsh -w login01,cranectld,crane[01-04] systemctl enable --now cplugind
```

### Verify the deployment

```bash
which cbatch cqueue cinfo
systemctl status cfored
systemctl status cplugind
```

### Plugin path reminder

!!! note "Installation paths"
    Packages install files under `/usr/bin/`, `/usr/lib/crane/plugin/`, and `/usr/lib/systemd/system/`. When updating `/etc/crane/plugin.yaml`, use the actual `.so` paths under `/usr/lib/crane/plugin/`.

## Optional: Slurm Command Compatibility

!!! tip
    To make it easier for users familiar with Slurm to migrate to CraneSched, we provide the cwrapper tool. Administrators can follow the instructions below to set up aliases for Slurm commands.

```bash
cat > /etc/profile.d/cwrapper.sh << 'EOCWRAPPER'
alias sbatch='cwrapper sbatch'
alias sacct='cwrapper sacct'
alias sacctmgr='cwrapper sacctmgr'
alias scancel='cwrapper scancel'
alias scontrol='cwrapper scontrol'
alias sinfo='cwrapper sinfo'
alias squeue='cwrapper squeue'
alias srun='cwrapper srun'
alias salloc='cwrapper salloc'
EOCWRAPPER

pdcp -w login01,crane[01-04] /etc/profile.d/cwrapper.sh /etc/profile.d/cwrapper.sh
pdsh -w login01,crane[01-04] chmod 644 /etc/profile.d/cwrapper.sh
```

Note: cwrapper aliases only offer **basic compatibility**. To access advanced features, please use CraneSched’s command-line tools directly.

## Optional: Source Installation

Source installation is intended for development and quick validation. See the [Packaging Guide](../packaging.md) for the build dependencies and tool installation. After preparing the environment, run:

```bash
git clone https://github.com/PKUHPC/CraneSched-FrontEnd.git
cd CraneSched-FrontEnd
make
make install
```

By default, binaries are installed to `/usr/local/bin/` and systemd units to `/usr/local/lib/systemd/system/`. Use `make install PREFIX=/opt/crane` to change the installation prefix. After distributing the files to the target nodes, run `systemctl daemon-reload`, then enable the required services as described above.
