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

## 1. 安装 Golang
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

## 2. 安装 Protoc
```bash
PROTOC_ZIP=protoc-30.2-linux-x86_64.zip
# aarch64：protoc-30.2-linux-aarch_64.zip
curl -L https://github.com/protocolbuffers/protobuf/releases/download/v30.2/${PROTOC_ZIP} -o /tmp/protoc.zip
unzip /tmp/protoc.zip -d /usr/local
rm /tmp/protoc.zip /usr/local/readme.txt
```

## 3. 拉取前端仓库
```bash
git clone https://github.com/PKUHPC/CraneSched-FrontEnd.git
```

## 4. 构建和安装

工作目录是 CraneSched-FrontEnd。在此目录中，编译所有 Golang 组件并安装。
```bash
cd CraneSched-FrontEnd
make
make install
```

## 5. 分发和启动服务

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

## 6. 安装 CLI 别名（可选）
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
