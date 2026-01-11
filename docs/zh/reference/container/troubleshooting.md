# 容器功能故障排除

本文档汇总容器功能使用过程中的常见问题、错误信息及解决方案。

## 日志与诊断

当容器功能出现问题时，建议收集以下日志和诊断信息以协助排查：

### 查看 Supervisor 日志

```bash
ls /var/crane/supervisor/
cat /var/crane/supervisor/JOBID.STEPID.log
```

### 验证 CRI 连接

!!! note
    仅管理员可用。

```bash
# 使用 crictl 测试
sudo crictl --runtime-endpoint unix:///run/containerd/containerd.sock version
sudo crictl --runtime-endpoint unix:///run/containerd/containerd.sock images
```

### 检查容器运行时状态

```bash
systemctl status containerd
journalctl -u containerd -f
```

## 常见错误与解决方案

### 容器功能未启用

**错误现象**

```
Error: Failed to pull image "myimage:latest" for ...
```

**原因**

集群管理员未在 `config.yaml` 中启用容器功能。

**解决方案**

联系管理员参照 [容器功能部署](../../deployment/container.md) 启用容器功能：

```yaml
Container:
  Enabled: true
  RuntimeEndpoint: /run/containerd/containerd.sock
```

---

### 作业步骤 ID 无效

**错误现象**

```
Error: Invalid container ID format
```

**可能原因**

容器 ID 格式错误。

**解决方案**

使用正确的格式 `JOBID.STEPID`：

!!! note
    使用 `ccon ps -a` 查看容器 ID 列表。Step ID 0 为 Daemon Step (Pod)，用户无法操作。

```bash
# 查看容器 ID
ccon ps -a

# 正确格式
ccon logs 123.1
ccon attach 123.1
```

---

### 无法连接到容器

**错误现象**

```
Error: Failed to attach to container 123.1: connection refused
```

**可能原因**

1. 容器已退出
2. 排队时间过长，CLI 端连接超时
2. CRI 运行时（如 containerd）未配置允许远程连接
3. 网络问题

**解决方案**

1. 检查容器状态：
   ```bash
   ccon ps -a
   ```
   如容器已退出，请检查退出原因。如作业仍在排队，请等待调度成功后再行尝试。

2. 确认 Containerd 已启用远程连接到容器功能。

---

### 无法查看容器日志

**错误现象**

```bash
ccon logs 123.1
# 无输出
```

**可能原因**

1. 容器内进程被设置为输出到文件
2. 容器尚未产生输出
3. 日志已被手动清理
4. 日志所在文件夹不在共享存储上，当前节点无法访问

**解决方案**

1. 确认容器是否启动失败
2. 确认容器的运行节点上是否存在日志文件
3. 确认容器内的进程本身是否有输出，以及输出是否被手动清理

---

### exec 命令执行失败

**错误现象**

```
Error: Command not found in container
```

**原因**

容器镜像中不存在指定的命令。

**解决方案**

以 Bash 为例，部分容器镜像中没有 Bash，只有 Sh：

1. 检查可用的 shell：
   ```bash
   ccon exec 123.1 /bin/sh -c "which bash sh"
   ```

2. 使用 `/bin/sh` 替代 `/bin/bash`：
   ```bash
   ccon exec -it 123.1 /bin/sh
   ```

---

### 镜像拉取失败

**错误现象**

```
Error: Failed to pull image "myimage:latest" for ...
```

**可能原因**

1. 镜像名称拼写错误
2. 网络无法访问镜像仓库
3. 私有仓库需要认证

**解决方案**

1. 对于私有仓库，使用 `ccon login` 认证：
   ```bash
   ccon login registry.example.com
   ```

2. 使用 `--pull-policy Never` 跳过拉取（如节点本地已有镜像）：
   ```bash
   ccon -p CPU run --pull-policy Never myimage:latest -- cmd
   ```

管理员可使用 ctr/crictl 工具检查 CRI 运行时的镜像拉取情况，例如：

```bash
crictl images
```

---

### 容器内权限拒绝

**错误现象**

```
Permission denied: '/data/file'
```

**可能原因**

用户命名空间映射失败导致容器内用户无法访问挂载的宿主机文件。

**解决方案**

1. 使用 `--userns=false` 禁用用户命名空间：
   ```bash
   ccon -p CPU run --userns=false -v /data:/data myimage -- cmd
   ```

2. 系统内核版本过低，或挂载目录所在的文件系统不支持 ID-Mapped Mounts。
    - 请管理员升级内核。
    - 确保挂载目录位于支持 ID-Mapped Mounts 的文件系统，或请管理员配置使用 BindFs 方案。

---

### GPU 设备不可用

**错误现象**

作业步提交时申请了 GPU 资源，容器外的裸机上可以使用 GPU，但是容器内无法访问 GPU 设备。

**可能原因**

管理员未正确配置容器运行时以启用 GPU 支持。

**解决方案**

请参考 [容器功能部署](../../deployment/container.md) ，并联系 GPU 供应商以获取正确的配置方法（例如 NVIDIA Container Toolkit, Ascend Docker Toolkit 等）。

---

### 端口绑定失败

**错误现象**

```
Error: Port 8080 is already in use
```

**原因**

指定的宿主机端口已被占用。

**解决方案**

使用不同端口：

```bash
ccon -p CPU run -p 8081:80 nginx:latest
```

## 错误码对照

容器相关操作可能返回以下错误码，完整错误码列表参见 [错误码参考](../error_code.md)。

| 错误码 | 说明 | 可能原因 | 解决方案 |
|:-------|:-----|:---------|:---------|
| `ERR_CRI_GENERIC` | CRI 运行时返回错误 | CRI 运行时（containerd/CRI-O）内部错误 | 查看容器运行时日志，检查镜像是否存在、容器配置是否正确 |
| `ERR_CRI_DISABLED` | 容器功能未启用 | 集群未配置容器支持 | 联系管理员启用容器功能（参见 [容器功能部署](../../deployment/container.md)）|
| `ERR_CRI_CONTAINER_NOT_READY` | 容器未就绪 | 作业处于 Pending 状态或容器尚未启动完成 | 请等待作业进入 Running 状态，使用 `ccon ps` 检查容器状态 |
| `ERR_CRI_MULTIPLE_NODES` | 多节点操作不支持 | 在多节点作业步上执行了不支持的容器操作 | 某些容器操作仅支持单节点作业步 |

## 获取帮助

如果以上方案未能解决问题：

1. **收集诊断信息**：
   ```bash
   ccon --debug-level trace run ...
   ```

2. **查看详细错误**：
   ```bash
   ccon inspect 123.1
   ccon inspectp 123
   ```

3. **检查系统状态**：
   ```bash
   cinfo -N
   cqueue -j <job_id>
   ```

4. **联系管理员**，提供以下信息：
    - 使用的命令
    - 作业 ID
    - 完整错误输出
    - 相关的 Supervisor 日志片段

## 另请参阅

- [容器功能部署](../../deployment/container.md) - 管理员配置指南
- [核心概念](concepts.md) - 理解 Pod 和容器步骤
- [ccon 命令手册](../../command/ccon.md) - 完整命令参考
- [错误码参考](../error_code.md) - 系统错误码说明
