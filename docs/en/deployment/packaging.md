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

Building the frontend packages requires Golang 1.24+, Protoc 30.2+, and GoReleaser v2. The commands below target x86-64. On ARM64, change the architecture in the downloaded file names to `arm64` or `aarch_64`.

Install Golang and the Protocol Buffers Go code generators:

```bash
GOLANG_TARBALL=go1.25.4.linux-amd64.tar.gz
curl -L https://go.dev/dl/${GOLANG_TARBALL} -o /tmp/go.tar.gz
rm -rf /usr/local/go
tar -C /usr/local -xzf /tmp/go.tar.gz
rm /tmp/go.tar.gz

cat > /etc/profile.d/go.sh <<'EOF'
export GOPATH=/root/go
export PATH=$GOPATH/bin:/usr/local/go/bin:$PATH
EOF
source /etc/profile.d/go.sh

go env -w GO111MODULE=on
go env -w GOPROXY=https://goproxy.cn,direct
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

Install Protoc:

```bash
PROTOC_ZIP=protoc-33.1-linux-x86_64.zip
curl -L https://github.com/protocolbuffers/protobuf/releases/download/v33.1/${PROTOC_ZIP} -o /tmp/protoc.zip
unzip /tmp/protoc.zip -d /usr/local
rm /tmp/protoc.zip /usr/local/readme.txt
```

Install GoReleaser:

```bash
go install github.com/goreleaser/goreleaser/v2@latest
```

Verify that all tools are available:

```bash
go version
protoc --version
goreleaser --version
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

### Package Contents

#### cranesched-frontend Package

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

## Downloading Pre-built Packages

You can download pre-built packages from GitHub Action Artifacts. These CI-generated packages are intended for testing purposes only; for production environments we recommend building the packages yourself to ensure compatibility.

## Next Steps

- Backend packages: follow the [Multi-node Deployment Guide](./configuration/multi-node.md) to distribute and install them on control and compute nodes.
- Frontend packages: follow the [Frontend Components Deployment Guide](./frontend/frontend.md) to install them on login nodes and nodes that need plugin functionality.
