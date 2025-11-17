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

## Installation Methods

CraneSched frontend components can be installed in two ways:

1. **Via RPM/DEB packages** (Recommended): Pre-built binary packages for quick deployment in production environments. This method requires no build dependencies and provides automatic systemd integration.

2. **From source**: Manual compilation and installation for development or when customization is needed. This method requires Golang and other build tools.

For most production deployments, we recommend using the package-based installation method described below.

## Installation via RPM/DEB Packages (Recommended)

The frontend is distributed as two separate packages:

- **cranesched-frontend**: Core CLI tools and cfored daemon
- **cranesched-plugin**: Optional plugin daemon (cplugind) and plugin shared objects

### Prerequisites

No build dependencies are required. You only need a package manager:
- RPM-based systems: `dnf` or `yum`
- DEB-based systems: `apt` or `dpkg`

### Obtaining Packages

You can obtain packages in two ways:

1. **Download from GitHub Releases**: Visit the [CraneSched-FrontEnd releases page](https://github.com/PKUHPC/CraneSched-FrontEnd/releases) and download the appropriate packages for your distribution.

2. **Build from source**: See [Building Packages from Source](#building-packages-from-source) below.

### Installing Frontend Package

For RPM-based systems (Rocky Linux, CentOS, Fedora, AlmaLinux):
```bash
sudo dnf install cranesched-frontend-*.rpm
```

For DEB-based systems (Debian, Ubuntu):
```bash
sudo apt install ./cranesched-frontend_*.deb
```

This installs the following CLI tools to `/usr/bin/`:
- `cacct`, `cacctmgr`, `calloc`, `cbatch`, `ccancel`, `ccon`, `ccontrol`
- `ceff`, `cfored`, `cinfo`, `cqueue`, `crun`, `cwrapper`

The package also installs the `cfored.service` systemd unit to `/usr/lib/systemd/system/`.

### Installing Plugin Package (Optional)

If you need plugin functionality (monitoring, email notifications, power management, etc.):

For RPM-based systems:
```bash
sudo dnf install cranesched-plugin-*.rpm
```

For DEB-based systems:
```bash
sudo apt install ./cranesched-plugin_*.deb
```

This installs:
- `cplugind` daemon to `/usr/bin/`
- Plugin shared objects to `/usr/lib/crane/plugin/`:
    - `dummy.so`, `energy.so`, `event.so`, `mail.so`, `monitor.so`, `powerControl.so`
- `cplugind.service` systemd unit to `/usr/lib/systemd/system/`

### Distributing Packages to Cluster Nodes

After installing packages on the login node, distribute them to other nodes:

```bash
# Copy packages to all nodes
pdcp -w login01,cranectld,crane[01-04] cranesched-frontend-*.rpm /tmp/
pdcp -w login01,cranectld,crane[01-04] cranesched-plugin-*.rpm /tmp/

# Install on all nodes (RPM example)
pdsh -w login01,cranectld,crane[01-04] "dnf install -y /tmp/cranesched-frontend-*.rpm"

# Install plugins on nodes that need them
pdsh -w login01,cranectld,crane[01-04] "dnf install -y /tmp/cranesched-plugin-*.rpm"
```

### Enabling Services

After installation, enable and start the required services:

```bash
# Enable cfored on login nodes (for interactive jobs)
pdsh -w login01 systemctl enable --now cfored

# Enable cplugind on nodes that need plugin functionality
pdsh -w login01,cranectld,crane[01-04] systemctl enable --now cplugind
```

### Verification

Verify the installation:

```bash
# Check installed binaries
which cbatch cqueue cinfo

# Check service status
systemctl status cfored
systemctl status cplugind  # if plugins installed
```

### Important Notes

!!! note "Installation Paths"
    Package installations use FHS-compliant paths:

    - Binaries: `/usr/bin/`
    - Plugins: `/usr/lib/crane/plugin/`
    - Services: `/usr/lib/systemd/system/`

    These differ from source installations which default to `/usr/local/` prefix. When configuring plugins, ensure you use the correct paths based on your installation method.

## Building Packages from Source

If you need to build the RPM/DEB packages yourself:

### Prerequisites

Install the required build tools:

```bash
# Install Golang (see detailed instructions in "Installation from Source" section below)
# Install Protoc (see detailed instructions in "Installation from Source" section below)

# Install goreleaser
go install github.com/goreleaser/goreleaser/v2@latest
```

### Building Packages

```bash
# Clone the repository
git clone https://github.com/PKUHPC/CraneSched-FrontEnd.git
cd CraneSched-FrontEnd

# Build packages
make package
```

The packages will be generated in `build/dist/`:
- `cranesched-frontend_<version>_amd64.rpm` / `cranesched-frontend_<version>_amd64.deb`
- `cranesched-plugin_<version>_amd64.rpm` / `cranesched-plugin_<version>_amd64.deb`

### Version Control

The package version is determined by the `VERSION` file in the repository root. To create a custom version, edit this file before running `make package`.

## Installation from Source (Alternative)

This method is recommended for development environments or when you need to customize the build. For production deployments, we recommend using the package-based installation above.

### 1. Install Golang
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

### 2. Install Protoc
```bash
PROTOC_ZIP=protoc-30.2-linux-x86_64.zip
# aarch64: protoc-30.2-linux-aarch_64.zip
curl -L https://github.com/protocolbuffers/protobuf/releases/download/v30.2/${PROTOC_ZIP} -o /tmp/protoc.zip
unzip /tmp/protoc.zip -d /usr/local
rm /tmp/protoc.zip /usr/local/readme.txt
```

### 3. Pull the frontend repository
```bash
git clone https://github.com/PKUHPC/CraneSched-FrontEnd.git
```

### 4. Build and install

The working directory is CraneSched-FrontEnd. In this directory, compile all Golang components and install.
```bash
cd CraneSched-FrontEnd
make
make install
```

By default, binaries are installed to `/usr/local/bin/` and services to `/usr/local/lib/systemd/system/`. You can customize the installation prefix by setting the `PREFIX` variable:

```bash
make install PREFIX=/opt/crane
```

### 5. Distribute and start services

!!! note
    This section applies to source installations. The binaries and service files are installed to `/usr/local/bin/` and `/usr/lib/systemd/system/` by default. If you installed via packages, the files are already in the correct system paths (`/usr/bin/`).

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

### 6. Install CLI aliases (optional)
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