# Packaging Guide

This guide covers how to build and install RPM and DEB packages for CraneSched.

## Overview

CraneSched provides pre-built packages for easy deployment on production systems. The packages are divided into **backend** (control and compute daemons) and **frontend** (CLI tools and plugins) components, each using different build systems optimized for their respective technology stacks.

### Package Overview

CraneSched provides four main packages:

| Package | Component | Description | Build System |
|---------|-----------|-------------|--------------|
| **cranectld** | Backend | Control daemon for management nodes | CPack (CMake) |
| **craned** | Backend | Execution daemon for compute nodes | CPack (CMake) |
| **cranesched-frontend** | Frontend | CLI tools and cfored daemon | GoReleaser |
| **cranesched-plugin** | Frontend | Plugin daemon and plugin libraries | GoReleaser |

### Installation Paths

All packages follow FHS (Filesystem Hierarchy Standard) conventions:

- **Binaries**: `/usr/bin/`
- **Libraries/Plugins**: `/usr/lib/` or `/usr/lib64/`
- **Systemd services**: `/usr/lib/systemd/system/`
- **Configuration**: `/etc/crane/`
- **Runtime data**: `/var/crane/`

## Backend Packages

The backend packages contain the core scheduling daemons written in C++.

### Building

#### Prerequisites

Before building backend packages, ensure you have:

1. **Built CraneSched backend** - Complete the build process as described in the deployment guides
2. **CMake 3.24+** - Required for package generation
3. **RPM tools** (for RPM packages):
   ```bash
   # Rocky/CentOS/Fedora
   dnf install -y rpm-build
   ```
4. **DEB tools** (for DEB packages):
   ```bash
   # Ubuntu/Debian
   apt-get install -y dpkg-dev
   ```

#### Build Process

Navigate to your build directory and ensure the project is properly configured:

```bash
cd CraneSched/build

# For CGroup v1 (default)
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release ..

# For CGroup v2
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DCRANE_ENABLE_CGROUP_V2=true ..

# Build the project
cmake --build .

# Generate packages
cpack -G "RPM;DEB"
```

After successful build, packages will be in your build directory:

```bash
ls -lh *.rpm *.deb
```

Expected output:
```
CraneSched-1.1.2-Linux-x86_64-cranectld.rpm
CraneSched-1.1.2-Linux-x86_64-craned.rpm
CraneSched-1.1.2-Linux-x86_64-cranectld.deb
CraneSched-1.1.2-Linux-x86_64-craned.deb
```

### Installing

#### cranectld Package

Install on control/management nodes:

**RPM-based systems:**
```bash
sudo rpm -ivh CraneSched-*-cranectld.rpm
# or for updates
sudo rpm -Uvh CraneSched-*-cranectld.rpm
```

**DEB-based systems:**
```bash
sudo dpkg -i CraneSched-*-cranectld.deb
```

**Package contents:**

- `/usr/bin/cranectld` - Control daemon binary
- `/usr/lib/systemd/system/cranectld.service` - Systemd service
- `/etc/crane/config.yaml.sample` - Configuration template
- `/etc/crane/database.yaml.sample` - Database configuration template

#### craned Package

Install on compute nodes:

**RPM-based systems:**
```bash
sudo rpm -ivh CraneSched-*-craned.rpm
# or for updates
sudo rpm -Uvh CraneSched-*-craned.rpm
```

**DEB-based systems:**
```bash
sudo dpkg -i CraneSched-*-craned.deb
```

**Package contents:**

- `/usr/bin/craned` - Execution daemon binary
- `/usr/libexec/csupervisor` - Per-step execution supervisor
- `/usr/lib/systemd/system/craned.service` - Systemd service
- `/etc/crane/config.yaml.sample` - Configuration template
- `/usr/lib64/security/pam_crane.so` - PAM authentication module

#### Post-Installation

Both packages automatically:

1. Create the `crane` system user (if not exists)
2. Create `/var/crane` directory with appropriate permissions
3. Create `/etc/crane` directory
4. Copy sample configuration files (if not exists)
5. Set appropriate file ownership and permissions

After installation, configure `/etc/crane/config.yaml` and `/etc/crane/database.yaml` (for cranectld), then start the services:

```bash
# On control node
systemctl enable --now cranectld

# On compute nodes
systemctl enable --now craned
```

## Frontend Packages

The frontend packages contain CLI tools and plugins written in Golang.

### Building

#### Prerequisites

To build frontend packages:

1. **Golang 1.22+** - Go programming language
2. **Protoc 30.2+** - Protocol buffer compiler
3. **GoReleaser v2** - Package generation tool

Install GoReleaser:
```bash
go install github.com/goreleaser/goreleaser/v2@latest
```

#### Build Process

```bash
# Clone the frontend repository
git clone https://github.com/PKUHPC/CraneSched-FrontEnd.git
cd CraneSched-FrontEnd

# Build packages
make package
```

The packages will be generated in `build/dist/`:
```bash
ls build/dist/*.rpm build/dist/*.deb
```

Expected output:
```
cranesched-frontend_1.1.2_amd64.rpm
cranesched-plugin_1.1.2_amd64.rpm
cranesched-frontend_1.1.2_amd64.deb
cranesched-plugin_1.1.2_amd64.deb
```

The package version is determined by the `VERSION` file in the repository root.

### Installing

#### cranesched-frontend Package

Install on login nodes and anywhere CLI tools are needed:

**RPM-based systems:**
```bash
sudo dnf install cranesched-frontend-*.rpm
```

**DEB-based systems:**
```bash
sudo apt install ./cranesched-frontend_*.deb
```

**Package contents:**

- CLI tools in `/usr/bin/`:
    - `cacct`, `cacctmgr`, `calloc`, `cbatch`, `ccancel`, `ccon`, `ccontrol`
    - `ceff`, `cfored`, `cinfo`, `cqueue`, `crun`, `cwrapper`
- `/usr/lib/systemd/system/cfored.service` - Frontend daemon service

Enable cfored on login nodes (for interactive jobs):
```bash
systemctl enable --now cfored
```

#### cranesched-plugin Package

Install on nodes that need plugin functionality (optional):

**RPM-based systems:**
```bash
sudo dnf install cranesched-plugin-*.rpm
```

**DEB-based systems:**
```bash
sudo apt install ./cranesched-plugin_*.deb
```

**Package contents:**

- `/usr/bin/cplugind` - Plugin daemon
- Plugin shared objects in `/usr/lib/crane/plugin/`:

    - `dummy.so` - Test plugin
    - `energy.so` - Power consumption monitoring
    - `event.so` - Node state event recording
    - `mail.so` - Job email notifications
    - `monitor.so` - Resource usage metrics collection
    - `powerControl.so` - Power management

- `/usr/lib/systemd/system/cplugind.service` - Plugin daemon service

Enable cplugind on nodes that need plugins:
```bash
systemctl enable --now cplugind
```

Configure plugins in `/etc/crane/plugin.yaml` and individual plugin configs (e.g., `/etc/crane/monitor.yaml`).

## Downloading Pre-built Packages

You can download pre-built packages from GitHub Action Artifacts. These CI-generated packages are intended for testing purposes only; for production environments we recommend building the packages yourself to ensure compatibility.

## Cluster Deployment

For deploying across a cluster, use cluster management tools:

```bash
# Copy packages to all nodes
pdcp -w crane[01-04] CraneSched-*-craned.rpm /tmp/
pdcp -w cranectld CraneSched-*-cranectld.rpm /tmp/
pdcp -w login01,crane[01-04] cranesched-frontend-*.rpm /tmp/
pdcp -w login01,crane[01-04] cranesched-plugin-*.rpm /tmp/

# Install on compute nodes
pdsh -w crane[01-04] "dnf install -y /tmp/CraneSched-*-craned.rpm"
pdsh -w crane[01-04] "dnf install -y /tmp/cranesched-frontend-*.rpm"

# Install on control node
pdsh -w cranectld "dnf install -y /tmp/CraneSched-*-cranectld.rpm"
pdsh -w cranectld "dnf install -y /tmp/cranesched-frontend-*.rpm"

# Install on login nodes
pdsh -w login01 "dnf install -y /tmp/cranesched-frontend-*.rpm"
pdsh -w login01 "dnf install -y /tmp/cranesched-plugin-*.rpm"
```
