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
    - Must run on nodes that need plugin functionality. Plugin `.so` files and configuration are registered in `/etc/crane/config.yaml`.

## Deployment Strategy

There are no official pre-built frontend RPM/DEB packages at the moment. You must build the components from source and deploy them in one of the following ways:

- **Install directly from source**: Run `make install` on the nodes that need the frontend. This is convenient for quick validation and development environments.
- **Build your own RPM/DEB packages**: Use GoReleaser to produce packages and install them through the system package manager. This is suitable for production environments that require standardized delivery.
- **Use GitHub Action artifacts**: CI uploads experimental packages after each build. They are meant for testing only and are not recommended for production.

The following sections describe how to prepare the build environment and how to deploy either directly or via self-built packages.

## Prepare the Build Environment

### Install Golang

```bash
GOLANG_TARBALL=go1.25.4.linux-amd64.tar.gz
# ARM architecture: wget https://dl.google.com/go/go1.25.4.linux-arm64.tar.gz
curl -L https://go.dev/dl/${GOLANG_TARBALL} -o /tmp/go.tar.gz

# Remove old Golang environment
rm -rf /usr/local/go

tar -C /usr/local -xzf /tmp/go.tar.gz && rm /tmp/go.tar.gz
echo 'export GOPATH=/root/go' >> /etc/profile.d/go.sh
echo 'export PATH=$GOPATH/bin:/usr/local/go/bin:$PATH' >> /etc/profile.d/go.sh
echo 'go env -w GO111MODULE=on' >> /etc/profile.d/go.sh
echo 'go env -w GOPROXY=https://goproxy.cn,direct' >> /etc/profile.d/go.sh

source /etc/profile.d/go.sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

### Install Protoc

```bash
PROTOC_ZIP=protoc-33.1-linux-x86_64.zip
# aarch64: protoc-33.1-linux-aarch_64.zip
curl -L https://github.com/protocolbuffers/protobuf/releases/download/v33.1/${PROTOC_ZIP} -o /tmp/protoc.zip
unzip /tmp/protoc.zip -d /usr/local
rm /tmp/protoc.zip /usr/local/readme.txt
```

### Install GoReleaser (only if you build packages)

```bash
go install github.com/goreleaser/goreleaser/v2@latest
```

## Fetch and Build

### Clone the frontend repository

```bash
git clone https://github.com/PKUHPC/CraneSched-FrontEnd.git
cd CraneSched-FrontEnd
```

### Build the binaries

```bash
make
```

The binaries are placed in `build/bin/` and the systemd units are placed in `etc/` after a successful build.

### Install on the current node

```bash
make install
```

By default, binaries go to `/usr/local/bin/` and services to `/usr/local/lib/systemd/system/`. Set `PREFIX` if you need a different installation root:

```bash
make install PREFIX=/opt/crane
```

## Build RPM/DEB Packages (optional)

If you prefer to deploy via package managers, run the following in the repository root:

```bash
make package
```

Packages are generated under `build/dist/`:

- `cranesched-frontend_<version>_amd64.rpm` / `cranesched-frontend_<version>_amd64.deb`
- `cranesched-plugin_<version>_amd64.rpm` / `cranesched-plugin_<version>_amd64.deb`

The version number is taken from the `VERSION` file in the repository root. Update it before `make package` if you need a custom version.

Install the self-built packages on the target nodes:

```bash
# RPM example
sudo dnf install /tmp/cranesched-frontend-*.rpm
sudo dnf install /tmp/cranesched-plugin-*.rpm

# DEB example
sudo apt install ./cranesched-frontend_*.deb
sudo apt install ./cranesched-plugin_*.deb
```

## Distribute and Enable

### Deploy compiled binaries (when not using packages)

```bash
pdcp -w login01,cranectld,crane[01-04] build/bin/* /usr/local/bin/
pdcp -w login01,cranectld,crane[01-04] etc/* /usr/local/lib/systemd/system/
```

Adjust the destination paths if you used a custom `PREFIX`. After the files are copied, run `systemctl daemon-reload` on every node to reload systemd units.

### Enable the required services

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
    Source installations default to the `/usr/local/` prefix. Package installations place files under `/usr/bin/`, `/usr/lib/crane/plugin/`, and `/usr/lib/systemd/system/`. When updating `/etc/crane/plugin.yaml`, make sure the `.so` paths match the installation method you used.

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

## GitHub Action artifacts (testing only)

Every master-branch CI run uploads RPM/DEB artifacts. These builds are not fully validated and should be used only for quick testing or CI reproduction. Download them from the corresponding workflow page if you need to inspect them, but always rely on self-built binaries or packages for production deployments.
