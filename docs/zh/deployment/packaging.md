# 打包指南

本指南介绍如何为鹤思构建和安装 RPM 和 DEB 软件包。

## 概述

鹤思提供预构建软件包，便于在生产系统上轻松部署。软件包分为**后端**（控制和计算守护进程）和**前端**（CLI 工具和插件）组件，每个组件使用针对其技术栈优化的不同构建系统。

### 软件包概览

鹤思提供四个主要软件包：

| 软件包 | 组件 | 描述 | 构建系统 |
|--------|------|------|----------|
| **cranectld** | 后端 | 管理节点的控制守护进程 | CPack (CMake) |
| **craned** | 后端 | 计算节点的执行守护进程 | CPack (CMake) |
| **cranesched-frontend** | 前端 | CLI 工具和 cfored 守护进程 | GoReleaser |
| **cranesched-plugin** | 前端 | 插件守护进程和插件库 | GoReleaser |

### 安装路径

所有软件包遵循 FHS（文件系统层次结构标准）约定：

- **二进制文件**：`/usr/bin/`
- **库/插件**：`/usr/lib/` 或 `/usr/lib64/`
- **Systemd 服务**：`/usr/lib/systemd/system/`
- **配置文件**：`/etc/crane/`
- **运行时数据**：`/var/crane/`

## 后端软件包

后端软件包主要为使用 C++ 编写的核心组件。

### 构建

#### 前置条件

在构建后端软件包之前，请确保您具备：

1. **已构建鹤思后端** - 按照部署指南中描述的构建过程完成
2. **CMake 3.24+** - 软件包生成所需
3. **RPM 工具**（用于 RPM 软件包）：
   ```bash
   # Rocky/CentOS/Fedora
   dnf install -y rpm-build
   ```
4. **DEB 工具**（用于 DEB 软件包）：
   ```bash
   # Debian/Ubuntu
   apt-get install -y dpkg-dev
   ```

#### 构建过程

导航到您的构建目录并确保项目已正确配置：

```bash
cd CraneSched/build

# 对于 CGroup v1（默认）
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release ..

# 对于 CGroup v2
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DCRANE_ENABLE_CGROUP_V2=true ..

# 构建项目
cmake --build .

# 生成软件包
cpack -G "RPM;DEB"
```

成功构建后，软件包将位于您的构建目录中：

```bash
ls -lh *.rpm *.deb
```

预期输出：
```
CraneSched-1.1.2-Linux-x86_64-cranectld.rpm
CraneSched-1.1.2-Linux-x86_64-craned.rpm
CraneSched-1.1.2-Linux-x86_64-cranectld.deb
CraneSched-1.1.2-Linux-x86_64-craned.deb
```

### 安装

#### cranectld 软件包

在控制/管理节点上安装：

**基于 RPM 的系统：**
```bash
sudo rpm -ivh CraneSched-*-cranectld.rpm
# 或更新
sudo rpm -Uvh CraneSched-*-cranectld.rpm
```

**基于 DEB 的系统：**
```bash
sudo dpkg -i CraneSched-*-cranectld.deb
```

**软件包内容：**

- `/usr/bin/cranectld` - 控制守护进程二进制文件
- `/usr/lib/systemd/system/cranectld.service` - Systemd 服务
- `/etc/crane/config.yaml.sample` - 配置模板
- `/etc/crane/database.yaml.sample` - 数据库配置模板

#### craned 软件包

在计算节点上安装：

**基于 RPM 的系统：**
```bash
sudo rpm -ivh CraneSched-*-craned.rpm
# 或更新
sudo rpm -Uvh CraneSched-*-craned.rpm
```

**基于 DEB 的系统：**
```bash
sudo dpkg -i CraneSched-*-craned.deb
```

**软件包内容：**

- `/usr/bin/craned` - 执行守护进程二进制文件
- `/usr/libexec/csupervisor` - 作业步骤执行守护进程
- `/usr/lib/systemd/system/craned.service` - Systemd 服务
- `/etc/crane/config.yaml.sample` - 配置模板
- `/usr/lib64/security/pam_crane.so` - PAM 身份验证模块

#### 安装后操作

两个软件包都会自动：

1. 创建 `crane` 系统用户（如果不存在）
2. 创建具有适当权限的 `/var/crane` 目录
3. 创建 `/etc/crane` 目录
4. 复制示例配置文件（如果不存在）
5. 设置适当的文件所有权和权限

安装后，配置 `/etc/crane/config.yaml` 和 `/etc/crane/database.yaml`（用于 cranectld），然后启动服务：

```bash
# 在控制节点上
systemctl enable --now cranectld

# 在计算节点上
systemctl enable --now craned
```

## 前端软件包

前端软件包主要为 Golang 编写的 CLI 工具和插件。

### 构建

#### 前置条件

要构建前端软件包：

1. **Golang 1.22+** - Go 编程语言
2. **Protoc 30.2+** - Protocol buffer 编译器
3. **GoReleaser v2** - 软件包生成工具

安装 GoReleaser：
```bash
go install github.com/goreleaser/goreleaser/v2@latest
```

#### 构建过程

```bash
# 克隆前端仓库
git clone https://github.com/PKUHPC/CraneSched-FrontEnd.git
cd CraneSched-FrontEnd

# 构建软件包
make package
```

软件包将在 `build/dist/` 中生成：
```bash
ls build/dist/*.rpm build/dist/*.deb
```

预期输出：
```
cranesched-frontend_1.1.2_amd64.rpm
cranesched-plugin_1.1.2_amd64.rpm
cranesched-frontend_1.1.2_amd64.deb
cranesched-plugin_1.1.2_amd64.deb
```

软件包版本由仓库根目录中的 `VERSION` 文件确定。

### 安装

#### cranesched-frontend 软件包

在登录节点和需要 CLI 工具的任何位置安装：

**基于 RPM 的系统：**
```bash
sudo dnf install cranesched-frontend-*.rpm
```

**基于 DEB 的系统：**
```bash
sudo apt install ./cranesched-frontend_*.deb
```

**软件包内容：**

- `/usr/bin/` 中的 CLI 工具：
    - `cacct`、`cacctmgr`、`calloc`、`cbatch`、`ccancel`、`ccon`、`ccontrol`
    - `ceff`、`cfored`、`cinfo`、`cqueue`、`crun`、`cwrapper`

- `/usr/lib/systemd/system/cfored.service` - 前端守护进程服务

在登录节点上启用 cfored（用于交互式作业）：
```bash
systemctl enable --now cfored
```

#### cranesched-plugin 软件包

在需要插件功能的节点上安装（可选）：

**基于 RPM 的系统：**
```bash
sudo dnf install cranesched-plugin-*.rpm
```

**基于 DEB 的系统：**
```bash
sudo apt install ./cranesched-plugin_*.deb
```

**软件包内容：**

- `/usr/bin/cplugind` - 插件守护进程

- `/usr/lib/crane/plugin/` 中的插件共享对象：

    - `dummy.so` - 测试插件
    - `mail.so` - 作业电子邮件通知
    - `monitor.so` - 资源使用指标收集
    - `powerControl.so` - 电源管理

- `/usr/lib/systemd/system/cplugind.service` - 插件守护进程服务

在需要插件的节点上启用 cplugind：
```bash
systemctl enable --now cplugind
```

在 `/etc/crane/plugin.yaml` 和各个插件配置文件（例如 `/etc/crane/monitor.yaml`）中配置插件。

## 下载预构建软件包

您可以从 GitHub Action Artifacts 中下载预构建的软件包。但是，CI 预构建的软件包仅作为测试目的，我们建议在生产环境中自行构建以确保兼容性。

## 集群部署

要在集群中部署，请使用集群管理工具：

```bash
# 将软件包复制到所有节点
pdcp -w crane[01-04] CraneSched-*-craned.rpm /tmp/
pdcp -w cranectld CraneSched-*-cranectld.rpm /tmp/
pdcp -w login01,crane[01-04] cranesched-frontend-*.rpm /tmp/
pdcp -w login01,crane[01-04] cranesched-plugin-*.rpm /tmp/

# 在计算节点上安装
pdsh -w crane[01-04] "dnf install -y /tmp/CraneSched-*-craned.rpm"
pdsh -w crane[01-04] "dnf install -y /tmp/cranesched-frontend-*.rpm"

# 在控制节点上安装
pdsh -w cranectld "dnf install -y /tmp/CraneSched-*-cranectld.rpm"
pdsh -w cranectld "dnf install -y /tmp/cranesched-frontend-*.rpm"

# 在登录节点上安装
pdsh -w login01 "dnf install -y /tmp/cranesched-frontend-*.rpm"
pdsh -w login01 "dnf install -y /tmp/cranesched-plugin-*.rpm"
```