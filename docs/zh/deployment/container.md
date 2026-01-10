# 容器功能部署

本指南说明如何在鹤思集群中启用和配置容器功能，允许用户通过 CRI（容器运行时接口）在集群中运行容器化作业。

!!! warning "实验性功能"
    容器功能目前处于实验阶段。建议在生产环境部署前充分测试。

## 前置条件

### 容器运行时

鹤思通过 CRI 接口与容器运行时交互，支持 **containerd** 和 **CRI-O** 等兼容 CRI 的运行时。

=== "containerd（推荐）"

    ```bash
    # Rocky Linux 9 / RHEL 9
    sudo dnf install -y containerd
    sudo systemctl enable --now containerd

    # Debian / Ubuntu
    sudo apt install -y containerd
    sudo systemctl enable --now containerd
    ```

    验证安装：

    ```bash
    sudo ctr version
    ```

=== "CRI-O"

    ```bash
    # Rocky Linux 9 / RHEL 9
    sudo dnf install -y cri-o
    sudo systemctl enable --now crio

    # Debian / Ubuntu
    sudo apt install -y cri-o
    sudo systemctl enable --now crio
    ```

    验证安装：

    ```bash
    sudo crictl version
    ```

### 运行时套接字

确保容器运行时的 CRI 套接字可访问：

| 运行时 | 默认套接字路径 |
|:-------|:---------------|
| containerd | `/run/containerd/containerd.sock` |
| CRI-O | `/run/crio/crio.sock` |

检查套接字存在：

```bash
ls -la /run/containerd/containerd.sock
# 或
ls -la /run/crio/crio.sock
```

### 可选依赖

若需启用用户命名空间映射功能（BindFs），需要额外安装：

```bash
# Rocky Linux 9 / RHEL 9
sudo dnf install -y bindfs fuse3

# Debian / Ubuntu
sudo apt install -y bindfs fuse3
```

## 配置说明

在 `/etc/crane/config.yaml` 中配置容器相关选项。以下是完整配置示例及字段说明：

```yaml
Container:
  # 启用容器功能
  Enabled: true
  
  # 容器临时数据目录（相对于 CraneBaseDir）
  TempDir: supervisor/containers/
  
  # CRI 运行时服务套接字
  RuntimeEndpoint: /run/containerd/containerd.sock
  
  # CRI 镜像服务套接字（通常与 RuntimeEndpoint 相同）
  ImageEndpoint: /run/containerd/containerd.sock
  
  # BindFs 配置（可选，用于用户命名空间映射）
  BindFs:
    Enabled: false
    BindfsBinary: /usr/bin/bindfs
    FusermountBinary: /usr/bin/fusermount3
    MountBaseDir: /mnt/crane
```

### 配置字段详解

#### 核心配置

| 字段 | 类型 | 默认值 | 说明 |
|:-----|:-----|:-------|:-----|
| `Enabled` | bool | `false` | 是否启用容器功能。设为 `true` 以启用 |
| `TempDir` | string | `supervisor/containers/` | 容器运行期间的临时数据目录，相对于 `CraneBaseDir`。存储容器元数据、日志等 |
| `RuntimeEndpoint` | string | — | **必填**。CRI 运行时服务的 Unix 套接字路径，用于容器生命周期管理（创建、启动、停止等） |
| `ImageEndpoint` | string | 同 `RuntimeEndpoint` | CRI 镜像服务的 Unix 套接字路径，用于镜像拉取与管理。大多数情况下与 `RuntimeEndpoint` 相同 |

#### BindFs 配置

BindFs 用于实现宿主机目录到容器内的用户 ID 映射挂载，解决用户命名空间下的权限问题。

| 字段 | 类型 | 默认值 | 说明 |
|:-----|:-----|:-------|:-----|
| `BindFs.Enabled` | bool | `false` | 是否启用 BindFs 功能 |
| `BindFs.BindfsBinary` | string | `bindfs` | bindfs 可执行文件路径 |
| `BindFs.FusermountBinary` | string | `fusermount3` | fusermount 可执行文件路径（用于卸载 FUSE 文件系统） |
| `BindFs.MountBaseDir` | string | `/mnt/crane` | BindFs 挂载点的基础目录。可以是绝对路径，或相对于 `CraneBaseDir` 的相对路径 |

!!! tip "何时需要 BindFs"
    当使用用户命名空间（`--userns`）运行容器，且需要挂载宿主机目录时，BindFs 可以自动处理 UID/GID 映射，确保容器内用户能正确访问挂载的文件。

## 部署步骤

### 1. 安装容器运行时

在所有计算节点上安装并启动容器运行时（参见前置条件）。

### 2. 验证运行时连接

使用 `crictl` 或 `ctr` 测试运行时是否正常工作：

```bash
# containerd
sudo crictl --runtime-endpoint unix:///run/containerd/containerd.sock version

# CRI-O
sudo crictl --runtime-endpoint unix:///run/crio/crio.sock version
```

预期输出：

```
Version:  0.1.0
RuntimeName:  containerd
RuntimeVersion:  v1.7.x
RuntimeApiVersion:  v1
```

### 3. 准备配置文件

编辑 `/etc/crane/config.yaml`，添加容器配置：

```yaml
Container:
  Enabled: true
  TempDir: supervisor/containers/
  RuntimeEndpoint: /run/containerd/containerd.sock
  ImageEndpoint: /run/containerd/containerd.sock
```

### 4. 分发配置

将配置文件同步到所有节点：

```bash
pdcp -w compute[01-10] /etc/crane/config.yaml /etc/crane/
```

### 5. 重启服务

重启计算节点上的 craned 服务以加载新配置：

```bash
pdsh -w compute[01-10] "systemctl restart craned"
```

### 6. 验证容器功能

在任意可提交作业的节点上运行测试容器：

```bash
# 提交简单容器作业
ccon run -p CPU alpine:latest -- echo "Hello from container"

# 查看作业状态
cqueue
```

成功时应输出：

```
Hello from container
```

## 镜像管理

### 镜像来源

容器镜像可从以下位置获取：

- **公共仓库**：Docker Hub、GHCR、Quay.io 等
- **私有仓库**：企业内部 Registry
- **本地镜像**：通过 `ctr` 或 `crictl` 预先导入

### 预拉取镜像

为减少作业启动延迟，建议在计算节点预拉取常用镜像：

```bash
# 使用 containerd
pdsh -w compute[01-10] "ctr -n k8s.io images pull docker.io/library/python:3.11"

# 使用 crictl
pdsh -w compute[01-10] "crictl pull python:3.11"
```

### 私有仓库认证

若需访问私有仓库，用户可通过 `ccon login` 配置认证：

```bash
ccon login registry.example.com
```

## 故障排除

### 常见问题

#### craned 启动失败：RuntimeEndpoint 未配置

**错误信息**：`RuntimeEndpoint is not configured.`

**解决方法**：确保 `config.yaml` 中 `Container.Enabled` 为 `true` 时，`RuntimeEndpoint` 已正确配置。

#### 无法连接到容器运行时

**错误信息**：`Failed to get CRI version: ...`

**排查步骤**：

1. 检查容器运行时服务状态：
   ```bash
   systemctl status containerd
   ```

2. 检查套接字权限：
   ```bash
   ls -la /run/containerd/containerd.sock
   ```

3. 手动测试 CRI 连接：
   ```bash
   sudo crictl --runtime-endpoint unix:///run/containerd/containerd.sock version
   ```

#### 镜像拉取失败

**排查步骤**：

1. 检查网络连接：
   ```bash
   ping docker.io
   ```

2. 检查 DNS 解析：
   ```bash
   nslookup registry-1.docker.io
   ```

3. 查看 craned 日志：
   ```bash
   journalctl -u craned -f
   ```

#### BindFs 挂载失败

**排查步骤**：

1. 确认 bindfs 和 fuse3 已安装：
   ```bash
   which bindfs fusermount3
   ```

2. 检查 `MountBaseDir` 是否存在并有正确权限：
   ```bash
   ls -la /mnt/crane
   ```

3. 确保 FUSE 内核模块已加载：
   ```bash
   lsmod | grep fuse
   ```

### 日志位置

容器相关日志位于：

- **craned 日志**：`/var/crane/craned/craned.log`（或 `CraneBaseDir` 下的配置路径）
- **Supervisor 日志**：`/var/crane/supervisor/`
- **容器临时数据**：`/var/crane/supervisor/containers/`（或配置的 `TempDir`）

查看 craned 容器相关日志：

```bash
grep -i "cri\|container" /var/crane/craned/craned.log
```

## 回滚容器功能

若需禁用容器功能：

1. 编辑 `/etc/crane/config.yaml`：
   ```yaml
   Container:
     Enabled: false
   ```

2. 同步配置并重启服务：
   ```bash
   pdcp -w compute[01-10] /etc/crane/config.yaml /etc/crane/
   pdsh -w compute[01-10] "systemctl restart craned"
   ```

3. 验证容器功能已禁用：
   ```bash
   ccon run -p CPU alpine:latest -- echo "test"
   # 应返回错误：容器功能未启用
   ```

## 相关文档

- [容器功能概览](../reference/container/index.md)：了解容器功能的整体定位与优势
- [核心概念](../reference/container/concepts.md)：理解容器作业、Pod、资源模型等概念
- [快速上手](../reference/container/quickstart.md)：快速体验容器作业提交
- [集群配置](./configuration/config.md)：完整的配置文件说明
