# Packaging Guide

This guide covers how to build and install RPM and DEB packages for CraneSched.

## Overview

CraneSched provides pre-built packages for easy deployment on production systems. The packages are divided into **backend** (control and compute daemons) and **frontend** (CLI tools and plugins) components, each using different build systems optimized for their respective technology stacks.

### Package Overview

CraneSched provides the following component packages:

| Package | Component | Description | Build System |
|---------|-----------|-------------|--------------|
| **cranectld** | Backend | Control daemon for management nodes | CPack (CMake) |
| **craned** | Backend | Execution daemon for compute nodes | CPack (CMake) |
| **cranesched-client** | Frontend | CLI tools and cfored daemon | GoReleaser |
| **cranesched-plugin** | Go | Plugin daemon and official SO libraries | GoReleaser |
| **cranesched-slurm-compat** | Go | Optional standalone Slurm-compatible commands | GoReleaser |

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
   # Debian/Ubuntu
   apt-get install -y dpkg-dev
   ```

Lua is fetched at a pinned release and linked into the backend binaries by
default when `CRANE_FULL_DYNAMIC=OFF`. This avoids a runtime dependency on the
build host's Lua SONAME, which can differ between distributions. Configure with
`-DCRANE_USE_SYSTEM_LUA=ON` to use the system Lua development package instead.

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
CraneSched-2.0.0-Linux-x86_64-cranectld.rpm
CraneSched-2.0.0-Linux-x86_64-craned.rpm
CraneSched-2.0.0-Linux-x86_64-cranectld.deb
CraneSched-2.0.0-Linux-x86_64-craned.deb
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
- `/etc/crane/plugin.yaml.sample` - Plugin configuration template

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
- `/etc/crane/plugin.yaml.sample` - Plugin configuration template
- `/usr/lib64/security/pam_crane.so` - PAM authentication module

#### Post-Installation

Both packages automatically:

1. Create the `crane` system user (if not exists)
2. Create `/var/crane` directory with appropriate permissions
3. Create `/etc/crane` directory
4. Copy sample configuration files (if not exists)
5. Set appropriate file ownership and permissions

After installation, configure `/etc/crane/config.yaml`, `/etc/crane/plugin.yaml`, and `/etc/crane/database.yaml` (for cranectld), then start the services:

For jobs that need to lock a large amount of memory, configure unlimited memlock for `craned.service` on all compute nodes. Create a systemd drop-in before starting `craned`:

```bash
sudo mkdir -p /etc/systemd/system/craned.service.d
sudo tee /etc/systemd/system/craned.service.d/override.conf >/dev/null <<'EOF'
[Service]
LimitMEMLOCK=infinity
EOF
sudo systemctl daemon-reload
```

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

Run product builds in a container for the target distribution. Use the Go version required by `go/go.mod` with `GOTOOLCHAIN=local`, protoc 23.2, protoc-gen-go v1.36.5, protoc-gen-go-grpc v1.5.1 and GoReleaser 2.12.0. Build cplugind and all official SO plugins with the same compiler and options. The complete repository is required because the Go module reads the root Proto, VERSION, LICENSE and configuration inputs.

#### Build Process

```bash
# Clone the product repository
git clone https://github.com/PKUHPC/CraneSched.git
cd CraneSched/go

# Build packages
make package
```

The packages will be generated in `build/dist/`:
```bash
ls build/dist/*.rpm build/dist/*.deb
```

Expected output:
```
cranesched-client_2.0.0_amd64.rpm
cranesched-plugin_2.0.0_amd64.rpm
cranesched-client_2.0.0_amd64.deb
cranesched-plugin_2.0.0_amd64.deb
```

The package version is determined by the `VERSION` file in the repository root.

### Package Contents

#### cranesched-client Package

- CLI tools in `/usr/bin/`:
    - `cacct`, `cacctmgr`, `calloc`, `cbatch`, `ccancel`, `ccon`, `ccontrol`
    - `ceff`, `cfored`, `cinfo`, `cqueue`, `crun`, `cwrapper`
- `/usr/lib/systemd/system/cfored.service` - Frontend daemon service

#### cranesched-plugin Package

- `/usr/bin/cplugind` - Plugin daemon
- Plugin shared objects in `/usr/lib/crane/plugin/`:

    - `dummy.so` - Test plugin
    - `mail.so` - Job email notifications
    - `monitor.so` - Resource usage metrics collection
    - `trace.so` - TraceHook receiver and trace span writer
    - `powerControl.so` - Power management

- `/usr/lib/systemd/system/cplugind.service` - Plugin daemon service

After generating the packages, follow the [Frontend Components Deployment Guide](./frontend/frontend.md) to install them on the appropriate nodes and enable the `cfored` and `cplugind` services.

## Slurm-compatible commands

The optional `cranesched-slurm-compat` RPM/DEB installs `sinfo`. Install this package only where the Crane implementation of the Slurm command name is required. It is separate from `cranesched-client`. All packages are built from the same product version.

Service templates are read from the root `etc/`; CNI samples come from `etc/cni/` and package licenses from the root `LICENSE`. Run GoReleaser from `go/`; outputs remain in `go/build/dist/`. Official plugin YAML examples are in `etc/plugins/` and are supplied as deployment examples rather than automatically installed configuration.

## Next Steps

- Backend packages: follow the [Multi-node Deployment Guide](./configuration/multi-node.md) to distribute and install them on control and compute nodes.
- Frontend packages: follow the [Frontend Components Deployment Guide](./frontend/frontend.md) to install them on login nodes and nodes that need plugin functionality.
