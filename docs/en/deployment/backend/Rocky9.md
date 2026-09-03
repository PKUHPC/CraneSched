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

The default package build fetches and statically links the pinned Lua runtime.
Install `lua-devel` only when configuring with `-DCRANE_USE_SYSTEM_LUA=ON`.

## 4. Build CraneSched Backend

Configure and build CraneSched:

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

## 5. Build and Install RPM Packages

After the build finishes, follow the [Packaging Guide](../packaging.md) to generate the `cranectld` and `craned` RPM packages, then install them on the target nodes with the package manager.

!!! note "Source installation"
    For local validation or development debugging, you can skip packaging and install directly from the source build. See the "Source Installation" appendix at the end of this page.

## 6. Next Steps

After installing the RPM packages, continue with the following configuration and deployment work:

1. **Configure the database**: The control node needs a working database configuration. Follow the [Database Configuration Guide](../configuration/database.md) to prepare `/etc/crane/database.yaml`.
2. **Configure the cluster topology**: All nodes need the same `/etc/crane/config.yaml`. Follow the [Cluster Configuration Guide](../configuration/config.md) to configure the control node, compute nodes, partitions, and resources.
3. **Distribute configuration and start services**: Distribute packages and configuration to control and compute nodes, then start `cranectld` / `craned`. Follow the [Multi-node Deployment Guide](../configuration/multi-node.md).
4. **Install frontend tools**: Install the CLI tools and frontend services on login nodes or any nodes where users submit jobs. Follow the [Frontend Deployment Guide](../frontend/frontend.md).
5. **Optional: configure PAM**: Configure PAM access control only after the cluster is deployed and verified. Follow the [PAM Module Configuration Guide](../configuration/pam.md).

## Appendix: Source Installation

For local validation or development debugging, you can install the current build directory directly:

```bash
cmake --install .
```

Running binaries directly is recommended only for debugging:

```bash
cranectld  # Control node
craned     # Compute node
```
