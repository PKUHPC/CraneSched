# Deployment Guide of Frontend Components

!!! tip
    This tutorial has been verified on **Rocky Linux 9**. It is expected to work on other systemd-based distributions, such as **Debian, Ubuntu, AlmaLinux, and Fedora**.

    This tutorial is designed for **x86-64** architecture. For other architectures, such as **ARM64**, ensure you modify the download links and commands as needed.

This guide assumes a demo cluster with the following nodes:

- **login01**: User login and job submission node.
- **cranectld**: Control node.
- **crane[01-04]**: Compute nodes.

Please run all commands as the root user throughout this tutorial. Ensure the backend environment installation is completed before proceeding.

## Overview

A brief overview of the main frontend components you will install and run:

- CLI tools (`cbatch`, `cqueue`, `cinfo`...):
    - User-facing command-line utilities for job submission, querying queues and job status, accounting, and job control.
    - Designed to be lightweight and distributed to user login nodes. They communicate with the control node (`cranectld`).

- `cfored` (Frontend daemon for interactive jobs):
    - Provides support for interactive jobs (used by `crun`, `calloc`).
    - Typically runs on login nodes where interactive jobs are submitted. Managed by systemd as `cfored.service`.

- `cplugind` (Plugin daemon):
    - Loads and manages plugins (mail, monitor, energy, event, etc.) and exposes plugin services to CraneSched components.
    - Must be running on nodes that require plugin functionality. Plugin `.so` files and plugin configuration are registered in `/etc/crane/config.yaml`.

## 1. Install Golang
```bash
GOLANG_TARBALL=go1.22.0.linux-amd64.tar.gz
# ARM architecture: wget https://dl.google.com/go/go1.22.0.linux-arm64.tar.gz
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

## 2. Install Protoc
```bash
PROTOC_ZIP=protoc-30.2-linux-x86_64.zip
# aarch64: protoc-30.2-linux-aarch_64.zip
curl -L https://github.com/protocolbuffers/protobuf/releases/download/v30.2/${PROTOC_ZIP} -o /tmp/protoc.zip
unzip /tmp/protoc.zip -d /usr/local
rm /tmp/protoc.zip /usr/local/readme.txt
```

## 3. Pull the frontend repository
```bash
git clone https://github.com/PKUHPC/CraneSched-FrontEnd.git
```

## 4. Build and install

The working directory is CraneSched-FrontEnd. In this directory, compile all Golang components and install.
```bash
cd CraneSched-FrontEnd
make
make install
```

## 5. Distribute and start services

```bash
pdcp -w login01,cranectld,crane[01-04] build/bin/* /usr/local/bin/
pdcp -w login01,cranectld,crane[01-04] etc/* /usr/lib/systemd/system/

# If you need to submit interactive jobs (crun, calloc), enable Cfored:
pdsh -w login01 systemctl daemon-reload
pdsh -w login01 systemctl enable cfored
pdsh -w login01 systemctl start cfored

# If you configured with plugin, enable cplugind
pdsh -w login01,crane[01-04] systemctl daemon-reload
pdsh -w login01,cranectld,crane[01-04] systemctl enable cplugind
pdsh -w login01,cranectld,crane[01-04] systemctl start cplugind
```

## 6. Install CLI aliases (optional)
You can install Slurm-style aliases for Crane using the following commands, allowing you to use Crane with Slurm command forms:

```bash
cat > /etc/profile.d/cwrapper.sh << 'EOF'
alias sbatch='cwrapper sbatch'
alias sacct='cwrapper sacct'
alias sacctmgr='cwrapper sacctmgr'
alias scancel='cwrapper scancel'
alias scontrol='cwrapper scontrol'
alias sinfo='cwrapper sinfo'
alias squeue='cwrapper squeue'
alias srun='cwrapper srun'
alias salloc='cwrapper salloc'
EOF

pdcp -w login01,crane[01-04] /etc/profile.d/cwrapper.sh /etc/profile.d/cwrapper.sh
pdsh -w login01,crane[01-04] chmod 644 /etc/profile.d/cwrapper.sh
```