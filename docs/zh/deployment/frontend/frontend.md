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

- CLI 工具（`cbatch`、`cqueue`、`cinfo` 等）：
  
    - 面向用户的命令行实用程序，用于作业提交、查询队列和作业状态、账务和作业控制。
    - 设计为轻量级并分发到用户登录节点。它们与控制节点（`cranectld`）通信。

- `cfored`（交互式作业守护进程）：
  
    - 为交互式作业提供支持（由 `crun`、`calloc` 使用）。
    - 通常在提交交互式作业的登录节点上运行。由 systemd 作为 `cfored.service` 管理。

- `cplugind`（插件守护进程）：
  
    - 加载和管理插件（mail、monitor、energy、event 等）并向鹤思组件公开插件服务。
    - 必须在需要插件功能的节点上运行。插件 `.so` 文件和插件配置在 `/etc/crane/config.yaml` 中注册。

## 部署策略

当前尚无官方发布的前端 RPM/DEB 软件包。您需要从源代码构建并部署组件，可根据需求选择以下方式：

- **从源代码直接安装**：在需要运行前端的节点上执行 `make install`，适合快速验证和开发环境。
- **构建自用 RPM/DEB 软件包**：使用 GoReleaser 生成软件包，然后通过包管理器安装，适合需要标准化交付的生产环境。
- **GitHub Action 构建产物**：CI 会在每次构建后生成实验性质的软件包，仅供测试。由于未经过充分验证，不建议在生产环境使用。

以下章节首先介绍如何准备构建环境，然后分别说明直接安装和软件包安装流程。

## 构建环境准备

### 安装 Golang

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

### 安装 Protoc

```bash
PROTOC_ZIP=protoc-30.2-linux-x86_64.zip
# aarch64：protoc-30.2-linux-aarch_64.zip
curl -L https://github.com/protocolbuffers/protobuf/releases/download/v30.2/${PROTOC_ZIP} -o /tmp/protoc.zip
unzip /tmp/protoc.zip -d /usr/local
rm /tmp/protoc.zip /usr/local/readme.txt
```

### 安装 GoReleaser（仅在构建软件包时需要）

```bash
go install github.com/goreleaser/goreleaser/v2@latest
```

## 获取与构建

### 克隆前端仓库

```bash
git clone https://github.com/PKUHPC/CraneSched-FrontEnd.git
cd CraneSched-FrontEnd
```

### 构建二进制

```bash
make
```

构建完成后，二进制位于 `build/bin/`，systemd 单元位于 `etc/`。

### 安装到当前节点

```bash
make install
```

默认情况下，二进制安装到 `/usr/local/bin/`，服务文件安装到 `/usr/local/lib/systemd/system/`。通过 `PREFIX` 可以修改安装前缀，例如：

```bash
make install PREFIX=/opt/crane
```

## 构建 RPM/DEB 软件包（可选）

如果需要使用包管理器部署，请在仓库根目录执行：

```bash
make package
```

软件包会生成在 `build/dist/` 中：

- `cranesched-frontend_<version>_amd64.rpm` / `cranesched-frontend_<version>_amd64.deb`
- `cranesched-plugin_<version>_amd64.rpm` / `cranesched-plugin_<version>_amd64.deb`

版本号由仓库根目录的 `VERSION` 文件决定，可在构建前根据需要修改。

在目标节点上安装自构建的软件包：

```bash
# RPM 系统示例
sudo dnf install /tmp/cranesched-frontend-*.rpm
sudo dnf install /tmp/cranesched-plugin-*.rpm

# DEB 系统示例
sudo apt install ./cranesched-frontend_*.deb
sudo apt install ./cranesched-plugin_*.deb
```

## 分发与启用

### 部署已编译的二进制（未使用软件包时）

```bash
pdcp -w login01,cranectld,crane[01-04] build/bin/* /usr/local/bin/
pdcp -w login01,cranectld,crane[01-04] etc/* /usr/local/lib/systemd/system/
```

如果使用了自定义 `PREFIX`，请根据实际路径调整同步目标。复制完成后，在每个节点上执行 `systemctl daemon-reload` 以重新加载单元文件。

### 启用所需服务

```bash
# 交互式作业所需的 cfored 仅运行在登录节点
pdsh -w login01 systemctl enable --now cfored

# 插件守护进程在需要插件功能的节点启用
pdsh -w login01,cranectld,crane[01-04] systemctl enable --now cplugind
```

### 验证部署

```bash
which cbatch cqueue cinfo
systemctl status cfored
systemctl status cplugind
```

### 插件路径提示

!!! note "安装路径"
    源代码安装默认使用 `/usr/local/` 前缀，而软件包安装将文件放置在 `/usr/bin/`、`/usr/lib/crane/plugin/` 和 `/usr/lib/systemd/system/`。在 `/etc/crane/plugin.yaml` 中注册插件时，请根据实际安装方式填写 `.so` 路径。

## 可选：安装 CLI 别名

```bash
cat > /etc/profile.d/cwrapper.sh << 'EOCWRAPPER'
alias sbatch='cwrapper sbatch'
alias sacct='cwrapper sacct'
alias sacctmgr='cwrapper sacctmgr'
alias scancel='cwrapper scancel'
alias scontrol='cwrapper scontrol'
alias sinfo='cwrapper sinfo'
alias squeue='cwrapper squeue'
alias srun='cwrapper srun'
alias salloc='cwrapper salloc'
EOCWRAPPER

pdcp -w login01,crane[01-04] /etc/profile.d/cwrapper.sh /etc/profile.d/cwrapper.sh
pdsh -w login01,crane[01-04] chmod 644 /etc/profile.d/cwrapper.sh
```

## GitHub Action 构建产物（测试用途）

项目的 GitHub Action 会在每次主干构建后上传 RPM/DEB 工件。这些工件未经过完整测试，仅适用于快速验证或 CI。若确需使用，可在对应工作流程页面下载并在临时环境中安装；在生产环境中请始终使用自行构建的软件包或二进制。
