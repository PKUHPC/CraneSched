# 容器功能故障排除

本文档汇总容器功能使用过程中的常见问题、错误信息及解决方案。

## 常见错误与解决方案

### 容器功能未启用

**错误现象**

```
Error: Container feature is not enabled on this cluster
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

### 镜像拉取失败

**错误现象**

```
Error: Failed to pull image "myimage:latest": rpc error: ...
```

**可能原因**

1. 镜像名称拼写错误
2. 网络无法访问镜像仓库
3. 私有仓库需要认证

**解决方案**

1. 检查镜像名称是否正确：
   ```bash
   # 验证镜像是否存在
   ctr images pull docker.io/library/alpine:latest
   ```

2. 检查网络连接：
   ```bash
   ping docker.io
   curl -I https://registry-1.docker.io/v2/
   ```

3. 对于私有仓库，使用 `ccon login` 认证：
   ```bash
   ccon login registry.example.com
   ```

4. 使用 `--pull-policy Never` 跳过拉取（需预先导入镜像）：
   ```bash
   ccon -p CPU run --pull-policy Never myimage:latest -- cmd
   ```

---

### 容器启动超时

**错误现象**

```
Error: Container failed to start within timeout
```

**可能原因**

1. 镜像过大，拉取时间超时
2. 容器入口点失败
3. 资源不足

**解决方案**

1. 预拉取镜像到计算节点：
   ```bash
   pdsh -w compute[01-10] "ctr -n k8s.io images pull myimage:latest"
   ```

2. 检查入口点命令是否正确：
   ```bash
   ccon -p CPU run -it myimage:latest -- /bin/sh
   ```

3. 增加资源配额：
   ```bash
   ccon -p CPU --mem 4G -c 2 run myimage:latest -- cmd
   ```

---

### 权限拒绝（容器内）

**错误现象**

```
Permission denied: '/data/file'
```

**可能原因**

用户命名空间映射导致容器内用户无法访问挂载的宿主机文件。

**解决方案**

1. 使用 `--userns=false` 禁用用户命名空间：
   ```bash
   ccon -p CPU run --userns=false -v /data:/data myimage -- cmd
   ```

2. 或确保管理员已启用 BindFs 功能（参见 [容器功能部署](../../deployment/container.md)）

3. 调整宿主机目录权限：
   ```bash
   chmod -R o+rx /data/input
   ```

---

### 无法连接到容器

**错误现象**

```
Error: Failed to attach to container 123.1: connection refused
```

**可能原因**

1. 容器已退出
2. cfored 服务未运行
3. 网络问题

**解决方案**

1. 检查容器状态：
   ```bash
   ccon ps -a
   ```

2. 确认 cfored 服务运行：
   ```bash
   systemctl status cfored
   ```

3. 指定目标节点：
   ```bash
   ccon attach -n compute01 123.1
   ```

---

### 端口绑定失败

**错误现象**

```
Error: Port 8080 is already in use
```

**原因**

指定的宿主机端口已被占用。

**解决方案**

1. 使用不同端口：
   ```bash
   ccon -p CPU run -p 8081:80 nginx:latest
   ```

2. 让系统自动分配端口（仅指定容器端口）：
   ```bash
   ccon -p CPU run -p 80 nginx:latest
   ```

---

### GPU 设备不可用

**错误现象**

```
Error: Requested GPU resources not available
```

**可能原因**

1. 节点无可用 GPU
2. GPU 资源已被占用
3. GRES 配置错误

**解决方案**

1. 检查可用 GPU 资源：
   ```bash
   cinfo -N
   ```

2. 指定正确的 GPU 类型：
   ```bash
   ccon -p GPU --gres gpu:a100:1 run pytorch/pytorch:latest -- python train.py
   ```

3. 确认分区有 GPU 节点：
   ```bash
   cinfo -p GPU
   ```

---

### 容器日志为空

**错误现象**

```bash
ccon logs 123.1
# 无输出
```

**可能原因**

1. 容器输出到文件而非标准输出
2. 容器尚未产生输出
3. 日志已被清理

**解决方案**

1. 确认程序输出到 stdout/stderr：
   ```bash
   ccon exec 123.1 cat /app/logs/output.log
   ```

2. 使用 `-f` 跟踪实时日志：
   ```bash
   ccon logs -f 123.1
   ```

---

### 作业步骤 ID 无效

**错误现象**

```
Error: Invalid container ID format
```

**原因**

容器 ID 格式错误。

**解决方案**

使用正确的格式 `JOBID.STEPID`：

```bash
# 查看容器 ID
ccon ps -a

# 正确格式
ccon logs 123.1
ccon attach 123.0
```

---

### exec 命令执行失败

**错误现象**

```
Error: Command not found in container
```

**原因**

容器镜像中不存在指定的命令。

**解决方案**

1. 检查可用的 shell：
   ```bash
   ccon exec 123.1 /bin/sh -c "which bash sh"
   ```

2. 使用 `/bin/sh` 替代 `/bin/bash`：
   ```bash
   ccon exec -it 123.1 /bin/sh
   ```

---

## 日志与诊断

### 查看 craned 日志

```bash
# 查看容器相关日志
grep -i "cri\|container\|pod" /var/crane/craned/craned.log

# 实时跟踪
tail -f /var/crane/craned/craned.log | grep -i container
```

### 查看 Supervisor 日志

```bash
ls /var/crane/supervisor/
cat /var/crane/supervisor/supervisor.log
```

### 验证 CRI 连接

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

## 错误码对照

容器相关操作可能返回以下错误码，完整错误码列表参见 [错误码参考](../error_code.md)。

| 错误码 | 说明 | 解决方案 |
|:-------|:-----|:---------|
| `ERR_NO_RESOURCE` | 资源不足 | 减少资源请求或等待资源释放 |
| `ERR_INVALID_PARAM` | 参数无效 | 检查命令参数格式 |
| `ERR_SUPERVISOR` | Supervisor 内部错误 | 查看 Supervisor 日志 |
| `ERR_SYSTEM_ERR` | 系统错误 | 检查系统日志 |
| `ERR_RPC_FAILURE` | RPC 调用失败 | 检查网络连接和服务状态 |

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

4. **联系管理员**提供以下信息：
   - 完整错误输出
   - 使用的命令
   - 作业 ID
   - 相关日志片段

## 另请参阅

- [容器功能部署](../../deployment/container.md) - 管理员配置指南
- [核心概念](concepts.md) - 理解 Pod 和容器步骤
- [ccon 命令手册](../../command/ccon.md) - 完整命令参考
- [错误码参考](../error_code.md) - 系统错误码说明
