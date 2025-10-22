# CentOS 7 部署指南

!!! warning
    本指南适用于 **CentOS 7**，该系统已达到**生命周期终止（EOL）**。CraneSched 的未来更新依赖于现代编译器，因此本教程**可能无法按预期工作**，且不再保证得到维护。

## 1. 配置构建环境

以下所有命令应在**构建节点**上以 **root** 用户身份执行。

安装附加仓库：

```bash
yum install -y epel-release centos-release-scl-rh
yum install -y ninja-build patch devtoolset-11 rh-git218
```

添加到 `~/.bash_profile`：

```bash
source scl_source enable devtoolset-11
source scl_source enable rh-git218
```

## 2. 环境准备

### 2.1 禁用 SELinux

```bash
setenforce 0
sed -i s#SELINUX=enforcing#SELINUX=disabled# /etc/selinux/config
```

### 2.2 安装证书

```bash
yum -y install ca-certificates
```

### 2.3 同步系统时间

```bash
yum -y install ntp ntpdate
systemctl start ntpd
systemctl enable ntpd
timedatectl set-timezone Asia/Shanghai
```

### 2.4 配置防火墙

!!! tip
    如果您有多个节点，请在**每个节点**上执行此步骤。否则，节点间通信将失败。

    有关端口配置详细信息，请参阅配置文件 `/etc/crane/config.yaml`。

```bash
systemctl stop firewalld
systemctl disable firewalld
```

如果您的集群需要保持防火墙处于活动状态，请开放以下端口：

```bash
firewall-cmd --add-port=10013/tcp --permanent --zone=public
firewall-cmd --add-port=10012/tcp --permanent --zone=public
firewall-cmd --add-port=10011/tcp --permanent --zone=public
firewall-cmd --add-port=10010/tcp --permanent --zone=public
firewall-cmd --add-port=873/tcp --permanent --zone=public
firewall-cmd --reload
```

## 3. 安装依赖

```bash
yum install -y openssl-devel curl-devel pam-devel zlib-devel zlib-static libaio-devel automake libcurl-devel
```

从源码安装 libcgroup：

```bash
yum install -y tar bison flex systemd-devel

wget https://github.com/libcgroup/libcgroup/releases/download/v3.1.0/libcgroup-3.1.0.tar.gz
tar -zxvf libcgroup-3.1.0.tar.gz

cd libcgroup-3.1.0
./configure
make -j
make install
```

## 4. 安装工具链

CraneSched 需要以下工具链版本：

* CMake ≥ 3.24
* libstdc++ ≥ 11
* clang ≥ 19 或 g++ ≥ 14

### 4.1 CMake

```bash
sudo yum install -y wget
wget https://github.com/Kitware/CMake/releases/download/v3.26.4/cmake-3.26.4-linux-x86_64.sh
bash cmake-3.26.4-linux-x86_64.sh --prefix=/usr/local --skip-license
cmake --version
```

### 4.2 GCC 14

CraneSched 需要 **libstdc++ ≥ 11**。CentOS 7 上的默认 GCC 4.8 太旧，因此我们需要从源码构建并安装 GCC 14。

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

如果您使用 clang，可以通过向 CMake 添加以下标志（步骤 6）来使用 GCC 14 的 libstdc++：

```bash
-DCMAKE_C_FLAGS_INIT="--gcc-toolchain=/usr/local" \
-DCMAKE_CXX_FLAGS_INIT="--gcc-toolchain=/usr/local"
```

## 5. 构建和安装

```bash
git clone https://github.com/PKUHPC/CraneSched.git
cd CraneSched

mkdir build && cd build
cmake -G Ninja -DCMAKE_C_COMPILER=/usr/bin/gcc \
               -DCMAKE_CXX_COMPILER=/usr/bin/g++ ..
cmake --build .
ninja install
```

!!! info
    如果您希望使用 RPM 软件包，请参阅[打包指南](packaging.md)获取说明。

对于多节点部署 CraneSched，请按照[多节点部署指南](../configuration/multi-node.md)。

## 6. 安装和配置 MongoDB

MongoDB 仅在**控制节点**上需要。

请按照[数据库配置指南](../configuration/database.md)获取详细说明。

!!! info
    CentOS 7 最高支持 MongoDB 7.0。有关具体安装说明，请参阅数据库配置指南。

## 7. PAM 模块设置

PAM 模块配置是可选的，但建议用于生产集群。

请按照 [PAM 模块配置指南](../configuration/pam.md)获取详细说明。

## 8. 配置和启动服务

有关集群配置详细信息，请参阅[集群配置指南](../configuration/config.md)。

配置完成后，启动服务：

```bash
# 如果使用 systemd：
systemctl start cranectld
systemctl start craned

# 如果直接使用可执行文件：
cd build/src
CraneCtld/cranectld
Craned/craned
```
