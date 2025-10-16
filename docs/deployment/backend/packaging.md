# Packaging Guide

This guide covers how to build and install RPM and DEB packages for CraneSched.

## Overview

CraneSched uses CPack to generate packages for easy distribution and installation. The build system supports creating both RPM (for Red Hat-based systems) and DEB (for Debian-based systems) packages.

### Package Components

!!! tip
    Frontend components are not included in these packages. Please refer to the [Frontend Deployment Guide](../frontend/frontend.md) for frontend installation.

CraneSched is divided into two main package components:

- **cranectld** - Control daemon package (for control nodes)
- **craned** - Execution daemon package (for compute nodes)

Each package includes:

- Binary executables
- Systemd service files
- Configuration file templates
- PAM security module (for craned package)

## Prerequisites

Before building packages, ensure you have:

1. **Built CraneSched** - Complete the build process as described in the [Rocky Linux 9](./Rocky9.md) or [CentOS 7](./CentOS7.md) guides
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

## Building Packages

### 1. Configure and Build

Navigate to your build directory and ensure the project is properly configured:

```bash
cd CraneSched/build

# For CGroup v1 (default)
cmake -G Ninja ..

# For CGroup v2
cmake -G Ninja .. -DCRANE_ENABLE_CGROUP_V2=true

# Build the project
cmake --build .
```

!!! tip
    For production deployments, use Release build type:
    ```bash
    cmake -G Ninja -DCMAKE_BUILD_TYPE=Release ..
    ```

### 2. Generate Packages

CPack is configured to build both RPM and DEB packages simultaneously:

```bash
# Generate both RPM and DEB packages
cpack -G "RPM;DEB"

# Or generate only RPM packages
cpack -G RPM

# Or generate only DEB packages
cpack -G DEB
```

!!! info
    The default configuration (`cpack` without `-G`) generates both RPM and DEB packages.

### 3. Locate Generated Packages

After successful build, packages will be in your build directory:

```bash
ls -lh *.rpm *.deb
```

Expected output (example):
```
CraneSched-1.1.2-Linux-x86_64-cranectld.rpm
CraneSched-1.1.2-Linux-x86_64-craned.rpm
CraneSched-1.1.2-Linux-x86_64-cranectld.deb
CraneSched-1.1.2-Linux-x86_64-craned.deb
```

## Installing Packages

### RPM-based Systems

**Control Node:**
```bash
sudo rpm -ivh CraneSched-*-cranectld.rpm
```

**Compute Node:**
```bash
sudo rpm -ivh CraneSched-*-craned.rpm
```

**Update existing installation:**
```bash
sudo rpm -Uvh CraneSched-*-cranectld.rpm
sudo rpm -Uvh CraneSched-*-craned.rpm
```

### DEB-based Systems

**Control Node:**
```bash
sudo dpkg -i CraneSched-*-cranectld.deb
```

**Compute Node:**
```bash
sudo dpkg -i CraneSched-*-craned.deb
```

**Update existing installation:**
```bash
sudo dpkg -i CraneSched-*-cranectld.deb
sudo dpkg -i CraneSched-*-craned.deb
```

## Package Details

### cranectld Package

Contains files for the control node:

```
/usr/local/bin/cranectld                    # Control daemon binary
/etc/systemd/system/cranectld.service       # Systemd service file
/etc/crane/config.yaml.sample               # Cluster configuration template
/etc/crane/database.yaml.sample             # Database configuration template
```

### craned Package

Contains files for compute nodes:

```
/usr/local/bin/craned                       # Execution daemon binary
/usr/libexec/csupervisor                    # Per-step execution supervisor
/etc/systemd/system/craned.service          # Systemd service file
/etc/crane/config.yaml.sample               # Cluster configuration template
/usr/lib64/security/pam_crane.so            # PAM authentication module
```

### Post-Installation Actions

Both packages include a post-installation script that automatically:

1. Creates the `crane` system user (if not exists)
2. Creates `/var/crane` directory with appropriate permissions
3. Creates `/etc/crane` directory
4. Copies sample configuration files to `/etc/crane/config.yaml` (if not exists)
5. Copies database configuration to `/etc/crane/database.yaml` (if not exists, cranectld only)
6. Sets appropriate file ownership and permissions
