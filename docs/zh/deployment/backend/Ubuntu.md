# Debian/Ubuntu 部署指南

!!! tip
    本指南在 **Ubuntu 24.04 LTS** 上经过测试，但也大体适用于 **Ubuntu 20.04/22.04+** 以及 **Debian 11/12+** 系统。

    这些说明针对 x86-64 架构量身定制。对于其他架构（如 ARM64），请确保相应修改下载链接和命令。

请在本教程中以 root 用户身份运行所有命令。

## 1. 环境准备

### 1.1 更新软件包列表

更新系统已有的软件包：

```bash
apt update && apt upgrade -y 
```

### 1.2 启用时间同步

```bash
apt install -y chrony
systemctl restart systemd-timedated
timedatectl set-timezone Asia/Shanghai
timedatectl set-ntp true
```

### 1.3 配置防火墙

!!! tip
    如果您有多个节点，请在**每个节点**上执行此步骤。否则，节点间通信将失败。

    有关端口配置详细信息，请参阅配置文件 `/etc/crane/config.yaml`。

默认情况下，Debian/Ubuntu 使用 UFW 作为防火墙管理工具，可通过以下命令禁用 UFW：

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

### 1.4 禁用 SELinux<small>（可选）</small>

```bash
# 临时禁用（重启后会恢复）
setenforce 0
# 永久禁用（重启后保持）
sed -i s#SELINUX=enforcing#SELINUX=disabled# /etc/selinux/config
```

### 1.5 选择 CGroup 版本<small>（可选）</small>

Ubuntu 20.04 默认使用 **CGroup v1**，而 Ubuntu 22.04 和 24.04 默认使用 **CGroup v2**。

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
* 若使用 **clang++**，版本 ≥ **19**
* 若使用 **g++**，版本 ≥ **14**

### 2.1 GCC/G++

!!! tip
    如系统软件源或 Ubuntu Toolchain PPA 中已有符合要求的 gcc（如 Ubuntu 24.04+），则可直接安装。

=== "Ubuntu 25.04+/Debian 13+"

    ```bash
    apt install -y gcc g++
    ```

=== "Ubuntu 24.04"

    ```bash
    apt install -y gcc-14 g++-14

    # 配置 update-alternatives
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-14 100 \
    --slave /usr/bin/g++ g++ /usr/bin/g++-14 \
    --slave /usr/bin/gcov gcov /usr/bin/gcov-14

    # 选择 gcc-14 作为默认版本
    update-alternatives --config gcc
    ```

=== "从源码构建"

    1. 从源码编译安装 gcc 14：
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

    2. 修改默认 gcc 版本：
    ```bash
    update-alternatives --install /usr/bin/gcc gcc /opt/gcc-14/bin/gcc 100 \
    --slave /usr/bin/g++ g++ /opt/gcc-14/bin/g++ \
    --slave /usr/bin/gcov gcov /opt/gcc-14/bin/gcov

    update-alternatives --config gcc
    # 选择 gcc-14 作为默认版本
    ```

### 2.2 CMake

=== "Ubuntu 24.04+/Debian 12+"

    ```bash
    apt install -y cmake
    ```

=== "从脚本安装"

    ```bash
    export CMAKE_SCRIPT=cmake-3.26.4-linux-x86_64.sh
    # ARM64v8 架构请使用以下命令
    # CMAKE_SCRIPT=cmake-3.26.4-linux-aarch64.sh

    wget https://github.com/Kitware/CMake/releases/download/v3.26.4/$CMAKE_SCRIPT
    bash $CMAKE_SCRIPT --prefix=/usr/local --skip-license
    ```

### 2.3 其他构建工具

```bash
apt install -y \
    ninja-build \
    pkg-config \
    automake \
    flex \
    bison
```

## 3. 安装项目依赖

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
    在 Ubuntu 22.04 及以下版本中， `libsubid-dev` 不可用。请参考：[https://github.com/shadow-maint/shadow/releases/](https://github.com/shadow-maint/shadow/releases/) 构建并安装 shadow 4.0 或更高版本。

!!! info
    默认打包构建会获取并静态链接固定版本的 Lua。仅在
    `CRANE_FULL_DYNAMIC=ON` 或 `-DCRANE_STATIC_LUA=OFF` 时安装
    `liblua5.4-dev`（Ubuntu 20.04 使用 `liblua5.3-dev`）。

## 4. 构建鹤思后端

配置并构建鹤思：

```bash
git clone https://github.com/PKUHPC/CraneSched.git
cd CraneSched

# 对于 CGroup v1
cmake -G Ninja -S . -B build
cmake --build build

# 对于 CGroup v2
cmake -G Ninja -DCRANE_ENABLE_CGROUP_V2=true -S . -B build
cmake --build build

# 对于 CGroup v2 并启用 eBPF GRES 支持
cmake -G Ninja -DCRANE_ENABLE_CGROUP_V2=true -DCRANE_ENABLE_BPF=true -S . -B build
cmake --build build
```

## 5. 打包和安装 DEB 软件包

构建完成后，按照[打包指南](../packaging.md)生成 `cranectld` 和 `craned` DEB 软件包，并通过包管理器安装到目标节点。

!!! note "源码安装"
    如果只是本机验证或开发调试，也可以跳过打包，直接使用源码安装。具体命令见本文末尾的“源码安装”附录。

## 6. 后续步骤

完成 DEB 安装后，继续完成以下配置和部署工作：

1. **配置数据库**：控制节点需要可用的数据库配置。请参考[数据库配置指南](../configuration/database.md)准备 `/etc/crane/database.yaml`。
2. **配置集群拓扑**：所有节点需要一致的 `/etc/crane/config.yaml`。请参考[集群配置指南](../configuration/config.md)配置控制节点、计算节点、分区和资源。
3. **分发配置并启动服务**：将软件包和配置分发到控制节点、计算节点，并启动 `cranectld` / `craned`。请参考[多节点部署指南](../configuration/multi-node.md)。
4. **安装前端工具**：在登录节点或需要提交作业的节点安装 CLI 和前端服务。请参考[前端部署指南](../frontend/frontend.md)。
5. **可选：配置 PAM**：集群完成部署并验证运行后，再配置 PAM 访问控制。请参考[PAM 模块配置指南](../configuration/pam.md)。

## 源码安装

如果只是本机验证或开发调试，也可以直接安装当前构建目录中的产物：

```bash
cmake --install build
```

直接运行二进制仅建议用于调试：

```bash
cranectld  # 控制节点
craned     # 计算节点
```
