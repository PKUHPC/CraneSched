# Ubuntu 部署指南

!!! tip
    本指南适用于 **Ubuntu 20.04、22.04 和 24.04**。建议使用最新的 LTS 版本以获得更好的兼容性和支持。

## 1. 环境准备

### 1.1 更新软件包列表
建议将软件包源更改为国内镜像以加快下载速度。

```bash
apt update && apt upgrade -y 
```

### 1.2 安装证书

```bash
apt install ca-certificates -y
```

### 1.3 启用时间同步

```bash
apt install -y chrony
systemctl restart systemd-timedated
timedatectl set-timezone Asia/Shanghai
timedatectl set-ntp true
```

### 1.4 配置防火墙
!!! tip
    如果您有多个节点，请在**每个节点**上执行此步骤。否则，节点间通信将失败。

    有关端口配置详细信息，请参阅配置文件 `/etc/crane/config.yaml`。

```bash
systemctl stop ufw
systemctl disable ufw
```
如果您的集群需要保持防火墙处于活动状态，请开放以下端口：

```bash
ufw allow 10013
ufw allow 10012
ufw allow 10011
ufw allow 10010
ufw allow 873
```

### 1.5 禁用 SELinux

```bash
# 临时禁用（重启后会恢复）
setenforce 0
# 永久禁用（重启后保持）
sed -i s#SELINUX=enforcing#SELINUX=disabled# /etc/selinux/config
```

### 1.6 选择 CGroup 版本（可选）

Ubuntu 20.04 默认使用 **CGroup v1**，而 Ubuntu 22.04 和 24.04 默认使用 **CGroup v2**。

CraneSched 默认使用 **CGroup v1**。

如果您希望启用 CGroup v2 支持，需要对 GRES 进行[额外配置](eBPF.md)，
或者您可以将系统切换为使用 CGroup v1。

#### 1.6.1 配置 CGroup v1

如果您的系统已经使用 CGroup v1，请跳过此部分。

```bash
# 设置内核启动参数以切换到 CGroup v1
grubby --update-kernel=/boot/vmlinuz-$(uname -r) \
  --args="systemd.unified_cgroup_hierarchy=0 systemd.legacy_systemd_cgroup_controller"

# 重启以应用更改
reboot

# 验证版本
mount | grep cgroup
```

#### 1.6.2 配置 CGroup v2

```bash
# 检查子 cgroup 是否有资源访问权限（预期看到 cpu、io、memory 等）
cat /sys/fs/cgroup/cgroup.subtree_control

# 为子组授予资源权限
echo '+cpuset +cpu +io +memory +pids' > /sys/fs/cgroup/cgroup.subtree_control
```

此外，如果您计划在 CGroup v2 上使用 GRES，请参阅 [eBPF 指南](eBPF.md)以获取设置说明。

## 2. 安装工具链

工具链必须满足以下版本要求：

* `cmake` ≥ **3.24**
* 如果使用 **clang**，版本 ≥ **19**
* 如果使用 **g++**，版本 ≥ **14**

### 2.1 安装构建工具

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

# 对于 ubuntu 20.04
wget https://github.com/Kitware/CMake/releases/download/v3.26.4/cmake-3.26.4-linux-x86_64.sh
bash cmake-3.26.4-linux-x86_64.sh --prefix=/usr/local --skip-license
# 对于 ubuntu 22.04 或更新版本
apt install -y cmake

apt install -y ninja-build pkg-config flex bison
clang --version
cmake --version
```

### 2.2 安装常用工具

```bash
apt install -y tar unzip git curl
```

## 3. 安装项目依赖

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

## 4. 安装和配置 MongoDB

MongoDB 仅在**控制节点**上需要。

请按照[数据库配置指南](../configuration/database.md)获取详细说明。


## 5. 安装和配置 CraneSched

### 5.1 构建和安装

1. 配置和构建 CraneSched：
```bash
git clone https://github.com/PKUHPC/CraneSched.git
cd CraneSched
mkdir -p build && cd build

# 对于 CGroup v1
cmake -G Ninja .. -DCMAKE_C_COMPILER=/usr/bin/gcc -DCMAKE_CXX_COMPILER=/usr/bin/g++
cmake --build .

# 对于 CGroup v2
cmake -G Ninja .. -DCRANE_ENABLE_CGROUP_V2=true -DCMAKE_C_COMPILER=/usr/bin/gcc -DCMAKE_CXX_COMPILER=/usr/bin/g++
cmake --build .
```

2. 安装构建的二进制文件：

!!! tip
    我们建议使用 DEB 软件包部署 CraneSched。有关安装说明，请参阅[打包指南](packaging.md)。
```bash
cmake --install .
```

对于多节点部署 CraneSched，请按照[多节点部署指南](../configuration/multi-node.md)。


### 5.2 配置 PAM 模块

PAM 模块配置是可选的，但建议用于生产集群以控制用户访问。

请按照 [PAM 模块配置指南](../configuration/pam.md)获取详细说明。

### 5.3 配置集群

有关集群配置详细信息，请参阅[集群配置指南](../configuration/config.md)。

## 6. 启动 CraneSched

手动运行（前台）：

```bash
cranectld
craned
```

或使用 systemd：

```bash
systemctl daemon-reload
systemctl enable cranectld --now
systemctl enable craned --now
```
