# Deployment Guide for Ubuntu

!!! tip
    This guide is for **Ubuntu 20.04, 22.04, and 24.04**. It is recommended to use the latest LTS version for better compatibility and support.

## 1. Environment Preparation

### 1.1 Update Package Lists
It is recommended to change the package source to a domestic mirror for faster downloads.

```bash
apt update && apt upgrade -y 
```

### 1.2 Install Certificates

```bash
apt install ca-certificates -y
```

### 1.3 Enable Time Synchronization

```bash
apt install -y chrony
systemctl restart systemd-timedated
timedatectl set-timezone Asia/Shanghai
timedatectl set-ntp true
```

### 1.4 Configure Firewall
!!! tip
    If you have multiple nodes, perform this step on **each node**. Otherwise, inter-node communication will fail.

    Please see the config file `/etc/crane/config.yaml` for port configuration details.

```bash
systemctl stop ufw
systemctl disable ufw
```
If your cluster requires the firewall to remain active, open the following ports:

```bash
ufw allow 10013
ufw allow 10012
ufw allow 10011
ufw allow 10010
ufw allow 873
```

### 1.5 Disable SELinux

```bash
# Temporary (will be reset after reboot)
setenforce 0
# Permanent (survives reboot)
sed -i s#SELINUX=enforcing#SELINUX=disabled# /etc/selinux/config
```

### 1.6 Select CGroup Version (Optional)

Ubuntu 20.04 uses **CGroup v1** by default, while Ubuntu 22.04 and 24.04 uses **CGroup v2** by default.

CraneSched uses **CGroup v1** by default.

If you prefer to enable CGroup v2 support, you’ll need [additional configuration](eBPF.md) for GRES,
or you can switch the system to use CGroup v1.

#### 1.6.1 Configure CGroup v1

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

#### 1.6.2 Configure CGroup v2

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
apt install build-essential
apt install libmpfr-dev libgmp3-dev libmpc-dev -y
wget http://ftp.gnu.org/gnu/gcc/gcc-14.1.0/gcc-14.1.0.tar.gz
tar -xf gcc-14.1.0.tar.gz
cd gcc-14.1.0

./contrib/download_prerequisites
mkdir build && cd build
../configure --enable-checking=release --enable-languages=c,c++ --disable-multilib
make -j
make install

#For ubuntu 20.04
wget https://github.com/Kitware/CMake/releases/download/v3.26.4/cmake-3.26.4-linux-x86_64.sh
bash cmake-3.26.4-linux-x86_64.sh --prefix=/usr/local --skip-license
#For ubuntu 22.04 or newer
apt install -y cmake

apt install -y ninja-build pkg-config flex bison
clang --version
cmake --version
```

### 2.2 Install Common Utilities

```bash
apt install -y tar unzip git curl
```

## 3. Install Project Dependencies

```bash
apt install -y \
    libcgroup-dev \
    libssl-dev \
    libcurl-dev \
    libpam-dev \
    zlib1g-dev \
    libaio-dev \
    pkg-config \
    ninja \
    libelf-dev \
    bcc \
    linux-headers-$(uname -r)
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
cmake -G Ninja .. -DCMAKE_C_COMPILER=/usr/bin/gcc -DCMAKE_CXX_COMPILER=/usr/bin/g++ 
cmake --build .

# For CGroup v2
cmake -G Ninja .. -DCRANE_ENABLE_CGROUP_V2=true -DCMAKE_C_COMPILER=/usr/bin/gcc -DCMAKE_CXX_COMPILER=/usr/bin/g++ 
cmake --build .
```

2. Install the built binaries:

!!! tip
    We recommend deploying CraneSched using DEB packages. See the [Packaging Guide](packaging.md) for installation instructions.

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
