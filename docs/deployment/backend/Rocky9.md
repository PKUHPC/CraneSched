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

### 1.4 Disable SELinux

```bash
# Temporary (will be reset after reboot)
setenforce 0

# Permanent (survives reboot)
sed -i s#SELINUX=enforcing#SELINUX=disabled# /etc/selinux/config
```

### 1.5 Select CGroup Version (Optional)

Rocky 9 uses **CGroup v2** by default.
CraneSched uses **CGroup v1** by default.

If you prefer to enable CGroup v2 support, you’ll need [additional configuration](eBPF.md),
or you can switch the system to use CGroup v1.

#### 1.5.1 Configure CGroup v1

If your system is already using CGroup v1, skip this section.

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

Additionally, if you plan to use GRES with CGroup v2, please refer to the [eBPF guide](eBPF.md) for setup instructions.

## 2. Install Toolchain

The toolchain must meet the following version requirements:

* `cmake` ≥ **3.24**
* If using **clang**, version ≥ **19**
* If using **g++**, version ≥ **14**

### 2.1 Install Build Tools

```bash
dnf install -y \
    gcc-toolset-14 \
    cmake \
    patch \
    flex \
    bison \
    ninja-build

echo 'source /opt/rh/gcc-toolset-14/enable' >> /etc/profile.d/extra.sh
source /etc/profile.d/extra.sh
```

### 2.2 Install Common Utilities

```bash
dnf install -y tar curl unzip git
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
    automake
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
    If you prefer to use RPM packages, please see the [Packaging Guide](packaging.md) for instructions.

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

Run manually (foreground):

```bash
cranectld
craned
```

Or use systemd:

```bash
systemctl daemon-reload
systemctl enable cranectld --now
systemctl enable craned --now
```