# Rocky Linux 9 部署指南

!!! tip
    本教程主要针对 **Rocky Linux 9** 设计，但应该兼容任何基于 **RHEL** 的发行版（例如 Rocky Linux 8、AlmaLinux）。

    这些说明针对 **x86-64** 架构量身定制。对于其他架构（如 ARM64），请确保相应修改下载链接和命令。

请在本教程中以 root 用户身份运行所有命令。

## 1. 环境准备

### 1.1 添加 EPEL 仓库

```bash
dnf install -y yum-utils epel-release
dnf config-manager --set-enabled crb
```

### 1.2 启用时间同步

```bash
dnf install -y chrony
systemctl restart systemd-timedated
timedatectl set-timezone Asia/Shanghai
timedatectl set-ntp true
```

### 1.3 配置防火墙

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

### 1.4 禁用 SELinux

```bash
# 临时禁用（重启后会恢复）
setenforce 0

# 永久禁用（重启后保持）
sed -i s#SELINUX=enforcing#SELINUX=disabled# /etc/selinux/config
```

### 1.5 选择 CGroup 版本（可选）

Rocky 9 默认使用 **CGroup v2**。
鹤思默认使用 **CGroup v1**。

如果您希望启用 CGroup v2 支持，需要[额外配置](eBPF.md)，
或者您可以将系统切换为使用 CGroup v1。

#### 1.5.1 配置 CGroup v1

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

#### 1.5.2 配置 CGroup v2

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

### 2.2 安装常用工具

```bash
dnf install -y tar curl unzip git
```

## 3. 安装项目依赖

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
    shadow-utils-subid-devel \
    automake
```

## 4. 安装和配置 MongoDB

MongoDB 仅在**控制节点**上需要。

请按照[数据库配置指南](../configuration/database.md)获取详细说明。

## 5. 安装和配置鹤思

### 5.1 构建和安装

1. 配置和构建鹤思:
```bash
git clone https://github.com/PKUHPC/CraneSched.git
cd CraneSched
mkdir -p build && cd build

# 对于 CGroup v1
cmake -G Ninja ..
cmake --build .

# 对于 CGroup v2
cmake -G Ninja .. -DCRANE_ENABLE_CGROUP_V2=true
cmake --build .
```

2. 安装构建的二进制文件：

!!! tip
    我们建议使用 RPM 软件包部署鹤思。有关安装说明，请参阅[打包指南](packaging.md)。
```bash
cmake --install .
```

对于多节点部署鹤思，请按照[多节点部署指南](../configuration/multi-node.md)。

### 5.2 配置 PAM 模块

PAM 模块配置是可选的，但建议用于生产集群以控制用户访问。

请按照 [PAM 模块配置指南](../configuration/pam.md)获取详细说明。

### 5.3 配置集群

有关集群配置详细信息，请参阅[集群配置指南](../configuration/config.md)。

## 6. 启动鹤思

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
