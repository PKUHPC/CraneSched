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
    - 必须在需要插件功能的节点上运行。插件 `.so` 文件和插件配置在 `/etc/crane/plugin.yaml` 中注册。

## 部署策略

前端组件默认通过 RPM/DEB 软件包部署。

如果还没有 `cranesched-frontend` 和 `cranesched-plugin` 软件包，请先按照[打包指南](../packaging.md)完成构建依赖安装并生成软件包。

`cranesched-frontend` 应安装在登录节点以及其他需要 CLI 工具的节点。`cranesched-plugin` 仅安装在需要运行插件的节点。

!!! note "其他安装方式"
    项目的 GitHub Action 会在每次 master 构建后上传 RPM/DEB 工件。这些工件未经过完整测试，仅适用于快速验证。

    此外，您也可以使用本页末尾的源码安装方式，无需生成 RPM/DEB 软件包。

## 安装前端软件包

将生成的软件包分发到目标节点后，使用对应的包管理器安装：

```bash
# RPM 系统：在登录节点安装 CLI 工具和 cfored
sudo dnf install /tmp/cranesched-frontend-*.rpm

# RPM 系统：仅在需要插件功能的节点安装
sudo dnf install /tmp/cranesched-plugin-*.rpm

# DEB 系统：在登录节点安装 CLI 工具和 cfored
sudo apt install ./cranesched-frontend_*.deb

# DEB 系统：仅在需要插件功能的节点安装
sudo apt install ./cranesched-plugin_*.deb
```

## 启用和验证服务

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
    软件包将文件安装到 `/usr/bin/`、`/usr/lib/crane/plugin/` 和 `/usr/lib/systemd/system/`。在 `/etc/crane/plugin.yaml` 中注册插件时，请使用 `/usr/lib/crane/plugin/` 下的实际 `.so` 路径。

## 可选：Slurm 命令兼容

!!! tip
    为了方便熟悉 Slurm 的用户迁移到鹤思（CraneSched），我们实现了 cwrapper 工具，管理员可以通过以下教程提供 Slurm 命令的别名。

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

请注意，cwrapper 别名仅提供**基本的兼容性**，如需体验高级特性，请使用鹤思的命令行工具。

## 可选：源码安装

源码安装适用于开发和快速验证。构建依赖与工具的安装方法参见[打包指南](../packaging.md)。准备好环境后执行：

```bash
git clone https://github.com/PKUHPC/CraneSched-FrontEnd.git
cd CraneSched-FrontEnd
make
make install
```

默认情况下，二进制安装到 `/usr/local/bin/`，systemd 单元安装到 `/usr/local/lib/systemd/system/`。可以通过 `make install PREFIX=/opt/crane` 修改安装前缀。将文件分发到目标节点后，执行 `systemctl daemon-reload`，再按照上文启用所需服务。
