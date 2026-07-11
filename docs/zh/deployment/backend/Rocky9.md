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

### 1.4 禁用 SELinux<small>（可选）</small>

```bash
# 临时禁用（重启后会恢复）
setenforce 0

# 永久禁用（重启后保持）
sed -i s#SELINUX=enforcing#SELINUX=disabled# /etc/selinux/config
```

### 1.5 选择 CGroup 版本<small>（可选）</small>

Rocky 9 默认使用 **CGroup v2**。

鹤思默认支持 **CGroup v1** 和 **CGroup v2**。但是，在基于 CGroup v2 的系统上使用 GRES 功能时，需要进行额外配置，具体请参阅 [eBPF 指南](eBPF.md)。

#### 1.5.1 配置 CGroup v1

如果您无法构建 eBPF 相关组件，且需要使用 GRES 功能，可切换回 CGroup v1：

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

如前所述，如果您计划在 CGroup v2 上使用 GRES，需参阅 [eBPF 指南](eBPF.md) 进行额外配置。

## 2. 安装工具链

工具链必须满足以下版本要求：

* CMake ≥ **3.24**
* 如果使用 **clang++**，版本 ≥ **19**
* 如果使用 **g++**，版本 ≥ **14**

使用以下命令安装并启用所需的工具链：

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
    elfutils-libelf-devel \
    shadow-utils-subid-devel \
    lua-devel
```

## 4. 构建鹤思后端

配置并构建鹤思：

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

## 5. 打包和安装 RPM 软件包

构建完成后，按照[打包指南](../packaging.md)生成 `cranectld` 和 `craned` RPM 软件包，并通过包管理器安装到目标节点。

!!! note "源码安装"
    如果只是本机验证或开发调试，也可以跳过打包，直接使用源码安装。具体命令见本文末尾的“源码安装”附录。

## 6. 后续步骤

完成 RPM 安装后，继续完成以下配置和部署工作：

1. **配置数据库**：控制节点需要可用的数据库配置。请参考[数据库配置指南](../configuration/database.md)准备 `/etc/crane/database.yaml`。
2. **配置集群拓扑**：所有节点需要一致的 `/etc/crane/config.yaml`。请参考[集群配置指南](../configuration/config.md)配置控制节点、计算节点、分区和资源。
3. **分发配置并启动服务**：将软件包和配置分发到控制节点、计算节点，并启动 `cranectld` / `craned`。请参考[多节点部署指南](../configuration/multi-node.md)。
4. **安装前端工具**：在登录节点或需要提交作业的节点安装 CLI 和前端服务。请参考[前端部署指南](../frontend/frontend.md)。
5. **可选：配置 PAM**：集群完成部署并验证运行后，再配置 PAM 访问控制。请参考[PAM 模块配置指南](../configuration/pam.md)。

## 附录：源码安装

如果只是本机验证或开发调试，也可以直接安装当前构建目录中的产物：

```bash
cmake --install .
```

直接运行二进制仅建议用于调试：

```bash
cranectld  # 控制节点
craned     # 计算节点
```
