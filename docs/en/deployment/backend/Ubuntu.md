# Debian/Ubuntu Deployment Guide

!!! tip
    This guide has been tested on **Ubuntu 24.04 LTS**, but it also applies to **Ubuntu 20.04/22.04+** and **Debian 11/12+**.

    The instructions target x86-64 systems. For other architectures (such as ARM64), adjust download links and commands accordingly.

Run every command in this tutorial as the root user.

## 1. Environment preparation

### 1.1 Update package lists

Update existing packages:

```bash
apt update && apt upgrade -y 
```

### 1.2 Enable time synchronization

```bash
apt install -y chrony
systemctl restart systemd-timedated
timedatectl set-timezone Asia/Shanghai
timedatectl set-ntp true
```

### 1.3 Configure the firewall

!!! tip
    Run this step on **every node** in the cluster—otherwise communication between nodes fails.

    Refer to `/etc/crane/config.yaml` for port configuration details.

Debian/Ubuntu ships with UFW by default. Disable it with:

```bash
systemctl stop ufw
systemctl disable ufw
```

If the firewall must remain active, allow these ports:

```bash
ufw allow 10013
ufw allow 10012
ufw allow 10011
ufw allow 10010
ufw allow 873
```

### 1.4 Disable SELinux <small>(optional)</small>

```bash
# Temporarily disable (resets after reboot)
setenforce 0
# Permanently disable
sed -i s#SELINUX=enforcing#SELINUX=disabled# /etc/selinux/config
```

### 1.5 Choose the cgroup version <small>(optional)</small>

Ubuntu 20.04 uses **cgroup v1** by default, while Ubuntu 22.04 and 24.04 default to **cgroup v2**.

CraneSched supports both **cgroup v1** and **cgroup v2**. However, using GRES on a cgroup v2 system requires additional configuration; see the [eBPF guide](eBPF.md) for the required steps.

#### 1.5.1 Configure cgroup v1

If you cannot build the eBPF components and still need GRES, you can switch back to cgroup v1:

```bash
# Set kernel boot arguments to switch to cgroup v1
grubby --update-kernel=/boot/vmlinuz-$(uname -r) \
  --args="systemd.unified_cgroup_hierarchy=0 systemd.legacy_systemd_cgroup_controller"

# Reboot to apply the change
reboot

# Verify the version
mount | grep cgroup
```

#### 1.5.2 Configure cgroup v2

```bash
# Verify that child cgroups expose resource controllers (expect cpu, io, memory, etc.)
cat /sys/fs/cgroup/cgroup.subtree_control

# Enable controllers for child cgroups
echo '+cpuset +cpu +io +memory +pids' > /sys/fs/cgroup/cgroup.subtree_control
```

As noted earlier, see the [eBPF guide](eBPF.md) if you plan to use GRES on cgroup v2.

## 2. Install the toolchain

Your toolchain must meet these minimum versions:

* CMake ≥ **3.24**
* **clang++** ≥ **19**
* **g++** ≥ **14**

### 2.1 GCC/G++

!!! tip
    If your distribution already provides an up-to-date GCC (for example Ubuntu 24.04+ or the Ubuntu Toolchain PPA), install it directly.

=== "Ubuntu 25.04+/Debian 13+"

    ```bash
    apt install -y gcc g++
    ```

=== "Ubuntu 24.04"

    ```bash
    apt install -y gcc-14 g++-14

    # Configure update-alternatives
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-14 100 \
    --slave /usr/bin/g++ g++ /usr/bin/g++-14 \
    --slave /usr/bin/gcov gcov /usr/bin/gcov-14

    # Select gcc-14 as the default
    update-alternatives --config gcc
    ```

=== "Build from source"

    1. Build and install GCC 14:
    ```bash
    apt install build-essential
    wget https://ftp.gnu.org/gnu/gcc/gcc-14.3.0/gcc-14.3.0.tar.gz 
    tar -xf gcc-14.3.0.tar.gz
    cd gcc-14.3.0

    ./contrib/download_prerequisites
    mkdir build && cd build
    ../configure --prefix=/opt/gcc-14 --enable-checking=release --enable-languages=c,c++ --disable-multilib
    make -j$(nproc)
    make install
    ```

    2. Switch the default GCC with `update-alternatives`:
    ```bash
    update-alternatives --install /usr/bin/gcc gcc /opt/gcc-14/bin/gcc 100 \
    --slave /usr/bin/g++ g++ /opt/gcc-14/bin/g++ \
    --slave /usr/bin/gcov gcov /opt/gcc-14/bin/gcov

    update-alternatives --config gcc
    # Select gcc-14 as the default
    ```

### 2.2 CMake

=== "Ubuntu 24.04+/Debian 12+"

    ```bash
    apt install -y cmake
    ```

=== "Install from script"

    ```bash
    export CMAKE_SCRIPT=cmake-3.26.4-linux-x86_64.sh
    # For ARM64v8 use:
    # CMAKE_SCRIPT=cmake-3.26.4-linux-aarch64.sh

    wget https://github.com/Kitware/CMake/releases/download/v3.26.4/$CMAKE_SCRIPT
    bash $CMAKE_SCRIPT --prefix=/usr/local --skip-license
    ```

### 2.3 Other build tools

```bash
apt install -y \
    ninja-build \
    pkg-config \
    automake \
    flex \
    bison
```

## 3. Install project dependencies

```bash
apt install -y \
    libssl-dev \
    libcurl4-openssl-dev \
    libpam0g-dev \
    zlib1g-dev \
    libaio-dev \
    libsystemd-dev \
    libelf-dev \
    libsubid-dev
```

!!! info
    `libsubid-dev` is unavailable on Ubuntu 22.04 and older releases. Build and install shadow 4.0+ from [https://github.com/shadow-maint/shadow/releases/](https://github.com/shadow-maint/shadow/releases/).

## 4. Install and configure MongoDB

MongoDB is only required on the **control node**.

See the [Database Configuration Guide](../configuration/database.md) for step-by-step instructions.


## 5. Install and configure CraneSched

### 5.1 Build and install

**Configure and build CraneSched:**

```bash
git clone https://github.com/PKUHPC/CraneSched.git
cd CraneSched

# For cgroup v1
cmake -G Ninja -S . -B build
cmake --build build

# For cgroup v2
cmake -G Ninja -DCRANE_ENABLE_CGROUP_V2=true -S . -B build
cmake --build build

# For cgroup v2 with eBPF GRES support
cmake -G Ninja -DCRANE_ENABLE_CGROUP_V2=true -DCRANE_ENABLE_BPF=true -S . -B build
cmake --build build
```

**Install the built binaries:**

!!! tip
    We recommend deploying CraneSched with DEB packages. Refer to the [Packaging Guide](../packaging.md) for details.

```bash
cmake --install build
```

For multi-node installations, follow the [Multi-node Deployment Guide](../configuration/multi-node.md).

### 5.2 Configure the PAM module

Configuring PAM is optional but recommended in production clusters to control user access.

See the [PAM Module Configuration Guide](../configuration/pam.md) for details.

### 5.3 Configure the cluster

Refer to the [Cluster Configuration Guide](../configuration/config.md) for configuration options.

## 6. Start CraneSched

### Using systemd (Recommended)

**Control node only**: Create crane user (automatic with DEB packages):

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
