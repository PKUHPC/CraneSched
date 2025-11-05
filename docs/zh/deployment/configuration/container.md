# 容器支持配置

CraneSched 包含通过容器运行时接口 (CRI) 运行容器化工作负载的实验性支持。此功能允许您在由 containerd 或 CRI-O 等兼容 CRI 的运行时管理的容器中运行任务。

!!! warning "实验性功能"
    容器支持目前处于实验阶段。在部署到生产环境之前，请在非生产环境中进行充分测试。

## 前置要求

在启用容器支持之前，请确保您具备：

1. 在计算节点上安装了兼容 CRI 的容器运行时（例如 containerd、CRI-O）
2. 可以访问计算节点上的运行时套接字
3. 具有与容器运行时交互的适当用户权限

### 安装 containerd（推荐）

在每个计算节点上：

```bash
# 安装 containerd
sudo apt-get update
sudo apt-get install -y containerd

# 配置 containerd
sudo mkdir -p /etc/containerd
sudo containerd config default | sudo tee /etc/containerd/config.toml

# 启动并启用 containerd
sudo systemctl enable containerd
sudo systemctl start containerd

# 验证安装
sudo ctr version
```

对于其他 Linux 发行版，请参考 [containerd 安装指南](https://github.com/containerd/containerd/blob/main/docs/getting-started.md)。

## 配置

在 `/etc/crane/config.yaml` 中启用和配置容器支持：

```yaml
Container:
  # 启用容器支持
  Enabled: true
  
  # 容器操作的临时目录（相对于 CraneBaseDir）
  TempDir: supervisor/containers/
  
  # 容器运行时套接字路径
  RuntimeEndpoint: /run/containerd/containerd.sock
  
  # 镜像服务套接字路径（通常与 RuntimeEndpoint 相同）
  ImageEndpoint: /run/containerd/containerd.sock
```

### 配置选项

| 选项 | 描述 | 默认值 | 必需 |
|------|------|--------|------|
| `Enabled` | 启用容器支持 | `false` | 是 |
| `TempDir` | 容器数据临时目录（相对于 CraneBaseDir） | `supervisor/containers/` | 否 |
| `RuntimeEndpoint` | CRI 运行时套接字路径 | 无 | 是（如果启用） |
| `ImageEndpoint` | 镜像服务套接字路径 | 与 RuntimeEndpoint 相同 | 否 |

!!! tip "套接字路径"
    常见运行时套接字路径：
    - **containerd**: `/run/containerd/containerd.sock`
    - **CRI-O**: `/var/run/crio/crio.sock`

## 使用方法

启用容器支持后，用户可以使用任务提交命令的 `--container` 选项提交容器化任务。

### 基本容器作业

```bash
cbatch --container=docker.io/library/ubuntu:22.04 job_script.sh
```

### 交互式容器会话

```bash
calloc --container=docker.io/library/python:3.11 -N 1 -c 4
```

### 带 GPU 的容器

```bash
cbatch --container=nvcr.io/nvidia/cuda:12.0-base \
       --gres=gpu:1 \
       gpu_job.sh
```

## 容器镜像管理

### 镜像拉取策略

CraneSched 支持三种镜像拉取策略：

1. **Always**：始终拉取镜像，即使本地已存在
2. **IfNotPresent**：仅在镜像本地不存在时拉取（默认）
3. **Never**：从不拉取镜像；如果本地不存在则失败

在作业脚本或通过提交命令指定拉取策略。

### 私有仓库

要使用私有仓库的镜像，请在作业提交中配置身份验证：

```bash
cbatch --container=registry.example.com/private/image:tag \
       --container-user=username \
       --container-password=password \
       job.sh
```

!!! warning "安全性"
    避免以明文存储凭据。考虑使用环境变量或安全凭据管理系统。

## 容器生命周期

提交容器化作业时：

1. **Pod 沙箱创建**：CraneSched 为作业创建 pod 沙箱
2. **镜像拉取**：根据拉取策略拉取容器镜像
3. **容器创建**：在 pod 沙箱中创建容器
4. **容器启动**：启动容器并执行作业
5. **清理**：作业完成或取消后，删除容器和 pod

## 故障排除

### 容器启动失败

**检查运行时状态：**

```bash
sudo systemctl status containerd
```

**验证套接字权限：**

```bash
ls -l /run/containerd/containerd.sock
```

**检查 CraneSched 日志：**

```bash
tail -f /var/crane/craned/craned.log
```

### 镜像拉取失败

**手动测试镜像拉取：**

```bash
sudo ctr image pull docker.io/library/ubuntu:22.04
```

**检查网络连接：**

```bash
ping registry-1.docker.io
```

**验证代理设置（如果在代理后面）：**

编辑 `/etc/containerd/config.toml` 并配置 HTTP 代理设置。

### 权限被拒绝错误

确保 CraneSched 用户有权访问容器运行时套接字：

```bash
sudo usermod -aG docker crane  # 如果使用 Docker 套接字
sudo chmod 666 /run/containerd/containerd.sock  # 或调整套接字权限
```

## 限制

实验性容器支持的当前限制：

- 容器中的多步骤作业尚未完全支持
- 某些高级 CRI 功能可能未公开
- 容器网络限于主机网络模式
- 卷挂载当前受限

## 性能考虑

- 与本机进程执行相比，容器启动会增加开销
- 对于大型镜像，镜像拉取可能耗时
- 考虑对常用镜像使用本地镜像仓库
- 在计算节点上预拉取镜像以减少作业启动时间

## 安全最佳实践

1. **使用特定的镜像标签**而不是 `latest` 以确保可重现性
2. **扫描镜像漏洞**后再部署
3. **通过运行时策略限制容器权限**
4. **在可能的情况下使用只读根文件系统**
5. **定期更新基础镜像**以包含安全补丁

## 未来增强

容器支持的计划改进：

- 容器中的完整多步骤作业支持
- 高级网络配置
- 灵活的卷挂载
- 与 Kubernetes CRI 集成
- 容器资源监控和指标

如有问题或疑问，请参考 [CraneSched 文档](https://pkuhpc.github.io/CraneSched/) 或在 GitHub 上提出问题。