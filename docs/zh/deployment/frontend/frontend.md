# 前端组件部署指南

!!! tip
    本教程已在 **Rocky Linux 9** 上验证。它应该可以在其他基于 systemd 的发行版上运行，例如 **Debian、Ubuntu、AlmaLinux 和 Fedora**。

    本教程专为 **x86-64** 架构设计。对于其他架构，如 **ARM64**，请确保相应修改下载链接和命令。

本指南假设有一个演示集群，具有以下节点：

- **login01**：用户登录和作业提交节点。
- **cranectld**：控制节点。
- **crane[01-04]**：计算节点。

请在本教程中以 root 用户身份运行所有命令。确保在继续之前完成后端环境安装。

## 概述

您将安装和运行的主要前端组件的简要概述：

- CLI 工具（`cbatch`、`cqueue`、`cinfo`...）：
    - 面向用户的命令行实用程序，用于作业提交、查询队列和作业状态、账务和作业控制。
    - 设计为轻量级并分发到用户登录节点。它们与控制节点（`cranectld`）通信。

- `cfored`（交互式作业守护进程）：
    - 为交互式作业提供支持（由 `crun`、`calloc` 使用）。
    - 通常在提交交互式作业的登录节点上运行。由 systemd 作为 `cfored.service` 管理。

- `cplugind`（插件守护进程）：
    - 加载和管理插件（mail、monitor、energy、event 等）并向鹤思组件公开插件服务。
    - 必须在需要插件功能的节点上运行。插件 `.so` 文件和插件配置在 `/etc/crane/config.yaml` 中注册。

## 安装方式

鹤思前端组件可以通过两种方式安装：

1. **通过 RPM/DEB 软件包**（推荐）：用于生产环境快速部署的预构建二进制包。此方法不需要构建依赖项，并提供自动的 systemd 集成。

2. **从源代码构建**：用于开发或需要自定义时的手动编译和安装。此方法需要 Golang 和其他构建工具。

对于大多数生产部署，我们推荐使用下面介绍的基于软件包的安装方法。

## 通过 RPM/DEB 软件包安装（推荐）

前端分为两个独立的软件包：

- **cranesched-frontend**：核心 CLI 工具和 cfored 守护进程
- **cranesched-plugin**：可选的插件守护进程（cplugind）和插件共享对象

### 前置条件

不需要构建依赖项。您只需要一个软件包管理器：
- 基于 RPM 的系统：`dnf` 或 `yum`
- 基于 DEB 的系统：`apt` 或 `dpkg`

### 获取软件包

您可以通过两种方式获取软件包：

1. **从 GitHub Releases 下载**：访问 [CraneSched-FrontEnd 发布页面](https://github.com/PKUHPC/CraneSched-FrontEnd/releases) 并下载适合您发行版的软件包。

2. **从源代码构建**：请参阅下面的[从源代码构建软件包](#从源代码构建软件包)。

### 安装前端软件包

对于基于 RPM 的系统（Rocky Linux、CentOS、Fedora、AlmaLinux）：
```bash
sudo dnf install cranesched-frontend-*.rpm
```

对于基于 DEB 的系统（Debian、Ubuntu）：
```bash
sudo apt install ./cranesched-frontend_*.deb
```

这将把以下 CLI 工具安装到 `/usr/bin/`：
- `cacct`、`cacctmgr`、`calloc`、`cbatch`、`ccancel`、`ccon`、`ccontrol`
- `ceff`、`cfored`、`cinfo`、`cqueue`、`crun`、`cwrapper`

该软件包还将 `cfored.service` systemd 单元安装到 `/usr/lib/systemd/system/`。

### 安装插件软件包（可选）

如果您需要插件功能（监控、邮件通知、电源管理等）：

对于基于 RPM 的系统：
```bash
sudo dnf install cranesched-plugin-*.rpm
```

对于基于 DEB 的系统：
```bash
sudo apt install ./cranesched-plugin_*.deb
```

这将安装：
- `cplugind` 守护进程到 `/usr/bin/`
- 插件共享对象到 `/usr/lib/crane/plugin/`：
    - `dummy.so`、`energy.so`、`event.so`、`mail.so`、`monitor.so`、`powerControl.so`
- `cplugind.service` systemd 单元到 `/usr/lib/systemd/system/`

### 分发软件包到集群节点

在登录节点上安装软件包后，将它们分发到其他节点：

```bash
# 将软件包复制到所有节点
pdcp -w login01,cranectld,crane[01-04] cranesched-frontend-*.rpm /tmp/
pdcp -w login01,cranectld,crane[01-04] cranesched-plugin-*.rpm /tmp/

# 在所有节点上安装（RPM 示例）
pdsh -w login01,cranectld,crane[01-04] "dnf install -y /tmp/cranesched-frontend-*.rpm"

# 在需要的节点上安装插件
pdsh -w login01,cranectld,crane[01-04] "dnf install -y /tmp/cranesched-plugin-*.rpm"
```

### 启用服务

安装后，启用并启动所需的服务：

```bash
# 在登录节点上启用 cfored（用于交互式作业）
pdsh -w login01 systemctl enable --now cfored

# 在需要插件功能的节点上启用 cplugind
pdsh -w login01,cranectld,crane[01-04] systemctl enable --now cplugind
```

### 验证

验证安装：

```bash
# 检查已安装的二进制文件
which cbatch cqueue cinfo

# 检查服务状态
systemctl status cfored
systemctl status cplugind  # 如果安装了插件
```

### 重要说明

!!! note "安装路径"
    软件包安装使用符合 FHS 标准的路径：

    - 二进制文件：`/usr/bin/`
    - 插件：`/usr/lib/crane/plugin/`
    - 服务：`/usr/lib/systemd/system/`

    这些与默认使用 `/usr/local/` 前缀的源代码安装不同。配置插件时，请确保根据安装方法使用正确的路径。

## 从源代码构建软件包

如果您需要自己构建 RPM/DEB 软件包：

### 前置条件

安装所需的构建工具：

```bash
# 安装 Golang（详细说明请参见下面的"从源代码安装"部分）
# 安装 Protoc（详细说明请参见下面的"从源代码安装"部分）

# 安装 goreleaser
go install github.com/goreleaser/goreleaser/v2@latest
```

### 构建软件包

```bash
# 克隆仓库
git clone https://github.com/PKUHPC/CraneSched-FrontEnd.git
cd CraneSched-FrontEnd

# 构建软件包
make package
```

软件包将在 `build/dist/` 中生成：
- `cranesched-frontend_<version>_amd64.rpm` / `cranesched-frontend_<version>_amd64.deb`
- `cranesched-plugin_<version>_amd64.rpm` / `cranesched-plugin_<version>_amd64.deb`

### 版本控制

软件包版本由仓库根目录中的 `VERSION` 文件确定。要创建自定义版本，请在运行 `make package` 之前编辑此文件。

## 从源代码安装（替代方法）

此方法建议用于开发环境或需要自定义构建时。对于生产部署，我们推荐使用上面的基于软件包的安装。

### 1. 安装 Golang
```bash
GOLANG_TARBALL=go1.22.0.linux-amd64.tar.gz
# ARM 架构：wget https://dl.google.com/go/go1.22.0.linux-arm64.tar.gz
curl -L https://go.dev/dl/${GOLANG_TARBALL} -o /tmp/go.tar.gz

# 删除旧的 Golang 环境
rm -rf /usr/local/go

tar -C /usr/local -xzf /tmp/go.tar.gz && rm /tmp/go.tar.gz
echo 'export GOPATH=/root/go' >> /etc/profile.d/go.sh
echo 'export PATH=$GOPATH/bin:/usr/local/go/bin:$PATH' >> /etc/profile.d/go.sh
echo 'go env -w GO111MODULE=on' >> /etc/profile.d/go.sh
echo 'go env -w GOPROXY=https://goproxy.cn,direct' >> /etc/profile.d/go.sh

source /etc/profile.d/go.sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

### 2. 安装 Protoc
```bash
PROTOC_ZIP=protoc-30.2-linux-x86_64.zip
# aarch64：protoc-30.2-linux-aarch_64.zip
curl -L https://github.com/protocolbuffers/protobuf/releases/download/v30.2/${PROTOC_ZIP} -o /tmp/protoc.zip
unzip /tmp/protoc.zip -d /usr/local
rm /tmp/protoc.zip /usr/local/readme.txt
```

### 3. 拉取前端仓库
```bash
git clone https://github.com/PKUHPC/CraneSched-FrontEnd.git
```

### 4. 构建和安装

工作目录是 CraneSched-FrontEnd。在此目录中，编译所有 Golang 组件并安装。
```bash
cd CraneSched-FrontEnd
make
make install
```

默认情况下，二进制文件安装到 `/usr/local/bin/`，服务安装到 `/usr/local/lib/systemd/system/`。您可以通过设置 `PREFIX` 变量来自定义安装前缀：

```bash
make install PREFIX=/opt/crane
```

### 5. 分发和启动服务

!!! note
    本节适用于源代码安装。二进制文件和服务文件默认安装到 `/usr/local/bin/` 和 `/usr/lib/systemd/system/`。如果您通过软件包安装，文件已经在正确的系统路径中（`/usr/bin/`）。

```bash
pdcp -w login01,cranectld,crane[01-04] build/bin/* /usr/local/bin/
pdcp -w login01,cranectld,crane[01-04] etc/* /usr/lib/systemd/system/

# 如果您需要提交交互式作业（crun、calloc），请启用 Cfored：
pdsh -w login01 systemctl daemon-reload
pdsh -w login01 systemctl enable cfored
pdsh -w login01 systemctl start cfored

# 如果您配置了插件，请启用 cplugind
pdsh -w login01,crane[01-04] systemctl daemon-reload
pdsh -w login01,cranectld,crane[01-04] systemctl enable cplugind
pdsh -w login01,cranectld,crane[01-04] systemctl start cplugind
```

### 6. 安装 CLI 别名（可选）
您可以使用以下命令为 Crane 安装 Slurm 风格的别名，允许您使用 Slurm 命令形式使用 Crane：

```bash
cat > /etc/profile.d/cwrapper.sh << 'EOF'
alias sbatch='cwrapper sbatch'
alias sacct='cwrapper sacct'
alias sacctmgr='cwrapper sacctmgr'
alias scancel='cwrapper scancel'
alias scontrol='cwrapper scontrol'
alias sinfo='cwrapper sinfo'
alias squeue='cwrapper squeue'
alias srun='cwrapper srun'
alias salloc='cwrapper salloc'
EOF

pdcp -w login01,crane[01-04] /etc/profile.d/cwrapper.sh /etc/profile.d/cwrapper.sh
pdsh -w login01,crane[01-04] chmod 644 /etc/profile.d/cwrapper.sh
```
