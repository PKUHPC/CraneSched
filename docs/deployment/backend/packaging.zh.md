# 打包指南

本指南介绍如何为 CraneSched 构建和安装 RPM 和 DEB 软件包。

## 概述

CraneSched 使用 CPack 生成软件包以便于分发和安装。构建系统支持创建 RPM（用于基于 Red Hat 的系统）和 DEB（用于基于 Debian 的系统）软件包。

### 软件包组件

!!! tip
    前端组件不包含在这些软件包中。有关前端安装，请参阅[前端部署指南](../frontend/frontend.md)。

CraneSched 分为两个主要软件包组件：

- **cranectld** - 控制守护进程软件包（用于控制节点）
- **craned** - 执行守护进程软件包（用于计算节点）

每个软件包包括：

- 二进制可执行文件
- Systemd 服务文件
- 配置文件模板
- PAM 安全模块（用于 craned 软件包）

## 先决条件

在构建软件包之前，请确保您具备：

1. **已构建 CraneSched** - 完成 [Rocky Linux 9](./Rocky9.md) 或 [CentOS 7](./CentOS7.md) 指南中描述的构建过程
2. **CMake 3.24+** - 软件包生成所需
3. **RPM 工具**（用于 RPM 软件包）：
   ```bash
   # Rocky/CentOS/Fedora
   dnf install -y rpm-build
   ```
4. **DEB 工具**（用于 DEB 软件包）：
   ```bash
   # Ubuntu/Debian
   apt-get install -y dpkg-dev
   ```

## 构建软件包

### 1. 配置和构建

导航到您的构建目录并确保项目已正确配置：

```bash
cd CraneSched/build

# 对于 CGroup v1（默认）
cmake -G Ninja ..

# 对于 CGroup v2
cmake -G Ninja .. -DCRANE_ENABLE_CGROUP_V2=true

# 构建项目
cmake --build .
```

!!! tip
    对于生产部署，使用 Release 构建类型：
    ```bash
    cmake -G Ninja -DCMAKE_BUILD_TYPE=Release ..
    ```

### 2. 生成软件包

CPack 配置为同时构建 RPM 和 DEB 软件包：

```bash
# 生成 RPM 和 DEB 软件包
cpack -G "RPM;DEB"

# 或仅生成 RPM 软件包
cpack -G RPM

# 或仅生成 DEB 软件包
cpack -G DEB
```

!!! info
    默认配置（不带 `-G` 的 `cpack`）会同时生成 RPM 和 DEB 软件包。

### 3. 定位生成的软件包

成功构建后，软件包将位于您的构建目录中：

```bash
ls -lh *.rpm *.deb
```

预期输出（示例）：
```
CraneSched-1.1.2-Linux-x86_64-cranectld.rpm
CraneSched-1.1.2-Linux-x86_64-craned.rpm
CraneSched-1.1.2-Linux-x86_64-cranectld.deb
CraneSched-1.1.2-Linux-x86_64-craned.deb
```

## 安装软件包

### 基于 RPM 的系统

**控制节点：**
```bash
sudo rpm -ivh CraneSched-*-cranectld.rpm
```

**计算节点：**
```bash
sudo rpm -ivh CraneSched-*-craned.rpm
```

**更新现有安装：**
```bash
sudo rpm -Uvh CraneSched-*-cranectld.rpm
sudo rpm -Uvh CraneSched-*-craned.rpm
```

### 基于 DEB 的系统

**控制节点：**
```bash
sudo dpkg -i CraneSched-*-cranectld.deb
```

**计算节点：**
```bash
sudo dpkg -i CraneSched-*-craned.deb
```

**更新现有安装：**
```bash
sudo dpkg -i CraneSched-*-cranectld.deb
sudo dpkg -i CraneSched-*-craned.deb
```

## 软件包详情

### cranectld 软件包

包含控制节点的文件：

```
/usr/bin/cranectld                          # 控制守护进程二进制文件
/usr/lib/systemd/system/cranectld.service   # Systemd 服务文件
/etc/crane/config.yaml.sample               # 集群配置模板
/etc/crane/database.yaml.sample             # 数据库配置模板
```

!!! warning "安装路径差异"
    文件路径因安装方法而异：

    **使用 RPM/DEB 软件包（`cpack`）时：**
    - 二进制文件安装到 `/usr/bin/`（遵循 FHS 标准）
    - 示例：`/usr/bin/cranectld`

    **使用直接安装（`cmake --install`）时：**
    - 二进制文件安装到 `/usr/local/bin/`（默认 `CMAKE_INSTALL_PREFIX`）
    - 示例：`/usr/local/bin/cranectld`
    - 您可以使用 `cmake --install --prefix=/custom/path` 自定义此设置

### craned 软件包

包含计算节点的文件：

```
/usr/bin/craned                             # 执行守护进程二进制文件
/usr/libexec/csupervisor                    # 每步执行监督器
/usr/lib/systemd/system/craned.service      # Systemd 服务文件
/etc/crane/config.yaml.sample               # 集群配置模板
/usr/lib64/security/pam_crane.so            # PAM 身份验证模块
```

!!! warning "安装路径差异"
    文件路径因安装方法而异：

    **使用 RPM/DEB 软件包（`cpack`）时：**
    - 二进制文件安装到 `/usr/bin/`（遵循 FHS 标准）
    - 监督器安装到 `/usr/libexec/`
    - 示例：`/usr/bin/craned`、`/usr/libexec/csupervisor`

    **使用直接安装（`cmake --install`）时：**
    - 二进制文件安装到 `/usr/local/bin/`（默认 `CMAKE_INSTALL_PREFIX`）
    - 监督器安装到 `/usr/local/libexec/`
    - 示例：`/usr/local/bin/craned`、`/usr/local/libexec/csupervisor`
    - 您可以使用 `cmake --install --prefix=/custom/path` 自定义此设置

### 安装后操作

两个软件包都包含一个安装后脚本，该脚本会自动：

1. 创建 `crane` 系统用户（如果不存在）
2. 创建具有适当权限的 `/var/crane` 目录
3. 创建 `/etc/crane` 目录
4. 将示例配置文件复制到 `/etc/crane/config.yaml`（如果不存在）
5. 将数据库配置复制到 `/etc/crane/database.yaml`（如果不存在，仅 cranectld）
6. 设置适当的文件所有权和权限
