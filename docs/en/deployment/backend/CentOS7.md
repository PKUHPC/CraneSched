# Deployment Guide for CentOS 7


!!! warning
    This guide is for **CentOS 7**, which has reached **End of Life (EOL)**. Future updates of CraneSched rely on modern compilers, so this tutorial **may not work as intended** and is no longer guaranteed to be maintained.


## 1. Configure Build Environment

All commands below should be executed on the **build node** as the **root** user.


Install additional repositories:

```bash
yum install -y epel-release centos-release-scl-rh
yum install -y ninja-build patch devtoolset-11 rh-git218
```

Add to `~/.bash_profile`:

```bash
source scl_source enable devtoolset-11
source scl_source enable rh-git218
```

## 2. Environment Preparation

### 2.1 Disable SELinux

```bash
setenforce 0
sed -i s#SELINUX=enforcing#SELINUX=disabled# /etc/selinux/config
```

### 2.2 Install Certificates

```bash
yum -y install ca-certificates
```

### 2.3 Synchronize System Time


```bash
yum -y install ntp ntpdate
systemctl start ntpd
systemctl enable ntpd
timedatectl set-timezone Asia/Shanghai
```

### 2.4 Configure Firewall

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

## 3. Install Dependencies

```bash
yum install -y openssl-devel curl-devel pam-devel zlib-devel zlib-static libaio-devel libcurl-devel systemd-devel
```

Install libcgroup from source:

```bash
yum install -y tar bison flex automake

wget https://github.com/libcgroup/libcgroup/releases/download/v3.1.0/libcgroup-3.1.0.tar.gz
tar -zxvf libcgroup-3.1.0.tar.gz

cd libcgroup-3.1.0
./configure
make -j
make install
```

## 4. Install Toolchain

CraneSched requires the following toolchain versions:

* CMake ≥ 3.24
* libstdc++ ≥ 11
* clang ≥ 19 or g++ ≥ 14

### 4.1 CMake

```bash
sudo yum install -y wget
wget https://github.com/Kitware/CMake/releases/download/v3.26.4/cmake-3.26.4-linux-x86_64.sh
bash cmake-3.26.4-linux-x86_64.sh --prefix=/usr/local --skip-license
cmake --version
```

### 4.2 GCC 14

CraneSched requires **libstdc++ ≥ 11**. The default GCC 4.8 on CentOS 7 is too old, so we need to build and install GCC 14 from source.

```bash
yum install -y tar bzip2
wget https://ftp.gnu.org/gnu/gcc/gcc-14.1.0/gcc-14.1.0.tar.gz

tar -zxvf gcc-14.1.0.tar.gz
cd gcc-14.1.0

./contrib/download_prerequisites
mkdir build && cd build
../configure --enable-checking=release --enable-languages=c,c++ --disable-multilib
make -j
make install
```

If you are using clang, you can use libstdc++ from GCC 14 by adding the following flags to CMake (step 6):

```bash
-DCMAKE_C_FLAGS_INIT="--gcc-toolchain=/usr/local" \
-DCMAKE_CXX_FLAGS_INIT="--gcc-toolchain=/usr/local"
```

## 5. Build and Install

```bash
git clone https://github.com/PKUHPC/CraneSched.git
cd CraneSched

mkdir build && cd build
cmake -G Ninja -DCMAKE_C_COMPILER=/usr/bin/gcc \
               -DCMAKE_CXX_COMPILER=/usr/bin/g++ \
               -DCRANE_USE_SYSTEM_LIBCGROUP=ON \
               -DCRANE_ENABLE_CGROUP_V2=OFF ..
cmake --build .
ninja install
```

!!! info
    If you prefer to use RPM packages, please refer to the [Packaging Guide](../packaging.md) for instructions.

For deploying CraneSched to multiple nodes, please follow the [Multi-node Deployment Guide](../configuration/multi-node.md).

## 6. Install and Configure MongoDB

MongoDB is required on the **control node** only.

Please follow the [Database Configuration Guide](../configuration/database.md) for detailed instructions.

!!! info
    CentOS 7 supports up to MongoDB 7.0. See the database configuration guide for specific installation instructions.

## 7. PAM Module Setup

PAM module configuration is optional but recommended for production clusters.

Please follow the [PAM Module Configuration Guide](../configuration/pam.md) for detailed instructions.

## 8. Configure and Launch Services

For cluster configuration details, see the [Cluster Configuration Guide](../configuration/config.md).

After configuring, choose your startup method:

### Using systemd (Recommended)

**Control node only**: Create crane user (automatic with RPM packages):

```bash
sudo groupadd --system crane 2>/dev/null || true
sudo useradd --system --gid crane --shell /usr/sbin/nologin --create-home crane 2>/dev/null || true
```

Then start services:

```bash
systemctl start cranectld  # Control node
systemctl start craned     # Compute node
```

### Running binaries directly

```bash
cd build/src
CraneCtld/cranectld  # Control node
Craned/craned        # Compute node
```
