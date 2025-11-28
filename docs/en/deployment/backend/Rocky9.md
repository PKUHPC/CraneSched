# Deployment Guide for Rocky Linux 9
!!! tip
    This tutorial is primarily designed for **Rocky Linux 9**, but it should be compatible with any **RHEL-based** distribution (e.g., Rocky Linux 8, AlmaLinux).

    The instructions are tailored for the **x86-64** architecture. For other architectures, such as ARM64, ensure you modify the download links and commands as needed.

Please run all commands as the root user throughout this tutorial.

## 1. Environment Preparation

### 1.1 Add EPEL Repository

```bash
dnf install -y yum-utils epel-release
dnf config-manager --set-enabled crb
```

### 1.2 Enable Time Synchronization

```bash
dnf install -y chrony
systemctl restart systemd-timedated
timedatectl set-timezone Asia/Shanghai
timedatectl set-ntp true
```

### 1.3 Configure Firewall

!!! tip
    If you have multiple nodes, perform this step on **each node**. Otherwise, inter-node communication will fail.

    Please see the config file `/etc/crane/config.yaml` for port configuration details.

```bash
systemctl stop firewalld
systemctl disable firewalld
```

If your cluster requires the firewall to remain active, open the following ports:

```bash
firewall-cmd --add-port=10013/tcp --permanent --zone=public
firewall-cmd --add-port=10012/tcp --permanent --zone=public
firewall-cmd --add-port=10011/tcp --permanent --zone=public
firewall-cmd --add-port=10010/tcp --permanent --zone=public
firewall-cmd --add-port=873/tcp --permanent --zone=public
firewall-cmd --reload
```

### 1.4 Disable SELinux <small>(Optional)</small>

```bash
# Temporary (will be reset after reboot)
setenforce 0

# Permanent (survives reboot)
sed -i s#SELINUX=enforcing#SELINUX=disabled# /etc/selinux/config
```

### 1.5 Select CGroup Version <small>(Optional)</small>

Rocky 9 uses **CGroup v2** by default.

CraneSched supports both **CGroup v1** and **CGroup v2**. However, using GRES on a CGroup v2 system requires additional configuration; see the [eBPF guide](eBPF.md) for details.

#### 1.5.1 Configure CGroup v1

If you cannot build the eBPF components and still need GRES, you can switch back to CGroup v1:

```bash
# Set kernel boot parameters to switch to CGroup v1
grubby --update-kernel=/boot/vmlinuz-$(uname -r) \
  --args="systemd.unified_cgroup_hierarchy=0 systemd.legacy_systemd_cgroup_controller"

# Reboot to apply changes
reboot

# Verify version
mount | grep cgroup
```

#### 1.5.2 Configure CGroup v2

```bash
# Check if sub-cgroups have access to resources (expect to see cpu, io, memory, etc.)
cat /sys/fs/cgroup/cgroup.subtree_control

# Grant resource permissions to subgroups
echo '+cpuset +cpu +io +memory +pids' > /sys/fs/cgroup/cgroup.subtree_control
```

As mentioned earlier, using GRES with CGroup v2 requires the additional steps described in the [eBPF guide](eBPF.md).

## 2. Install Toolchain

The toolchain must meet the following version requirements:

* CMake ≥ **3.24**
* If using **clang++**, version ≥ **19**
* If using **g++**, version ≥ **14**

Use the following commands to install and enable the required toolchain:

```bash
dnf install -y \
    gcc-toolset-14 \
    cmake \
    patch \
    flex \
    bison \
    automake \
    ninja-build

echo 'source /opt/rh/gcc-toolset-14/enable' >> /etc/profile.d/extra.sh
source /etc/profile.d/extra.sh
```

## 3. Install Project Dependencies

```bash
dnf install -y \
    libstdc++-devel \
    libstdc++-static \
    openssl-devel \
    curl-devel \
    pam-devel \
    zlib-devel \
    libaio-devel \
    systemd-devel \
    libcurl-devel \
    elfutils-libelf-devel \
    shadow-utils-subid-devel
```

## 4. Install and Configure MongoDB

MongoDB is required on the **control node** only.

Please follow the [Database Configuration Guide](../configuration/database.md) for detailed instructions.

## 5. Install and Configure CraneSched

### 5.1 Build and Install

1. Configure and build CraneSched:
```bash
git clone https://github.com/PKUHPC/CraneSched.git
cd CraneSched
mkdir -p build && cd build

# For CGroup v1
cmake -G Ninja ..
cmake --build .

# For CGroup v2
cmake -G Ninja .. -DCRANE_ENABLE_CGROUP_V2=true
cmake --build .
```

2. Install the built binaries:

!!! tip
    We recommend deploying CraneSched using RPM packages. See the [Packaging Guide](../packaging.md) for installation instructions.
```bash
cmake --install .
```

For deploying CraneSched to multiple nodes, please follow the [Multi-node Deployment Guide](../configuration/multi-node.md).

### 5.2 Configure PAM Module

PAM module configuration is optional but recommended for production clusters to control user access.

Please follow the [PAM Module Configuration Guide](../configuration/pam.md) for detailed instructions.

### 5.3 Configure the Cluster

For cluster configuration details, see the [Cluster Configuration Guide](../configuration/config.md).

## 6. Start CraneSched

### Using systemd (Recommended)

**Control node only**: Create crane user (automatic with RPM packages):

```bash
sudo groupadd --system crane 2>/dev/null || true
sudo useradd --system --gid crane --shell /usr/sbin/nologin --create-home crane 2>/dev/null || true
```

Then start services:

```bash
systemctl daemon-reload
systemctl enable cranectld --now  # Control node
systemctl enable craned --now     # Compute node
```

### Running manually (foreground)

```bash
cranectld  # Control node
craned     # Compute node
```
