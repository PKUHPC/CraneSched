# 打包指南

本指南介绍如何为鹤思构建和安装 RPM 和 DEB 软件包。

## 概述

鹤思使用 CPack 生成软件包以便于分发和安装。构建系统支持创建 RPM（用于基于 Red Hat 的系统）和 DEB（用于基于 Debian 的系统）软件包。

### 软件包组件

!!! tip
    鹤思作为独立的后端和前端软件包分发。本指南涵盖后端软件包（cranectld/craned）。有关前端软件包安装（CLI 工具和插件），请参阅[前端打包指南](../frontend/frontend.md#通过-rpmdeb-软件包安装推荐)。

鹤思后端分为两个主要软件包组件:

- **cranectld** - 控制守护进程软件包（用于控制节点）
- **craned** - 计算节点守护进程软件包（用于计算节点）

每个软件包包括：

- 二进制可执行文件
- Systemd 服务文件
- 配置文件模板
- PAM 安全模块（用于 craned 软件包）

## 先决条件

在构建软件包之前，请确保您具备：

1. **已构建鹤思** - 完成 [Rocky Linux 9](./Rocky9.md) 或 [CentOS 7](./CentOS7.md) 指南中描述的构建过程
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
/usr/bin/craned                             # 计算节点守护进程二进制文件
/usr/libexec/csupervisor                    # 作业步骤守护进程
/usr/lib/systemd/system/craned.service      # Systemd 服务文件
/etc/crane/config.yaml.sample               # 集群配置模板
/usr/lib64/security/pam_crane.so            # PAM 身份验证模块
```

!!! warning "安装路径差异"
    文件路径因安装方法而异：

    **使用 RPM/DEB 软件包（`cpack`）时：**
    - 二进制文件安装到 `/usr/bin/`（遵循 FHS 标准）
    - supervisor 安装到 `/usr/libexec/`
    - 示例：`/usr/bin/craned`、`/usr/libexec/csupervisor`

    **使用直接安装（`cmake --install`）时：**
    - 二进制文件安装到 `/usr/local/bin/`（默认 `CMAKE_INSTALL_PREFIX`）
    - supervisor 安装到 `/usr/local/libexec/`
    - 示例：`/usr/local/bin/craned`、`/usr/local/libexec/csupervisor`，你可能需要修改 `config.yaml` 中 supervisor 的 path
    - 您可以使用 `cmake --install --prefix=/custom/path` 自定义此设置

### 安装后操作

两个软件包都包含一个安装后脚本，该脚本会自动：

1. 创建 `crane` 系统用户（如果不存在）
2. 创建具有适当权限的 `/var/crane` 目录
3. 创建 `/etc/crane` 目录
4. 将示例配置文件复制到 `/etc/crane/config.yaml`（如果不存在）
5. 将数据库配置复制到 `/etc/crane/database.yaml`（如果不存在，仅 cranectld）
6. 设置适当的文件所有权和权限

## 前端软件包

鹤思前端组件与后端分开分发，并使用不同的构建系统（GoReleaser 而不是 CPack）。

### 可用的前端软件包

前端提供两个软件包：

| 软件包 | 描述 | 内容 |
|--------|------|------|
| **cranesched-frontend** | 核心 CLI 工具和前端守护进程 | CLI 工具（`cbatch`、`cqueue`、`cinfo` 等）、`cfored` 守护进程、systemd 服务 |
| **cranesched-plugin** | 插件守护进程和插件库 | `cplugind` 守护进程、插件 `.so` 文件（monitor、mail、energy、event 等）|

### 安装路径

前端软件包使用标准 FHS 路径：

- **二进制文件**：`/usr/bin/`（CLI 工具、cfored、cplugind）
- **插件**：`/usr/lib/crane/plugin/`（插件 .so 文件）
- **服务**：`/usr/lib/systemd/system/`（systemd 单元）

### 快速安装

**对于基于 RPM 的系统：**
```bash
# 核心前端工具
sudo dnf install cranesched-frontend-*.rpm

# 插件（可选）
sudo dnf install cranesched-plugin-*.rpm
```

**对于基于 DEB 的系统：**
```bash
# 核心前端工具
sudo apt install ./cranesched-frontend_*.deb

# 插件（可选）
sudo apt install ./cranesched-plugin_*.deb
```

### 构建前端软件包

前端软件包的构建过程与后端软件包不同：

```bash
# 克隆前端仓库
git clone https://github.com/PKUHPC/CraneSched-FrontEnd.git
cd CraneSched-FrontEnd

# 构建软件包（需要 goreleaser）
make package

# 软件包将位于 build/dist/ 中
ls build/dist/*.rpm build/dist/*.deb
```

有关完整的前端安装说明、配置详细信息和插件设置，请参阅[前端部署指南](../frontend/frontend.md)。
