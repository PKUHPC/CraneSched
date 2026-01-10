# Container Troubleshooting

This document summarizes common issues, error messages, and solutions for Container Support.

## Common Errors and Solutions

### Container Feature Not Enabled

**Symptom**

```
Error: Container feature is not enabled on this cluster
```

**Cause**

Cluster administrator has not enabled container feature in `config.yaml`.

**Solution**

Contact administrator to enable container feature following [Container Deployment](../../deployment/container.md):

```yaml
Container:
  Enabled: true
  RuntimeEndpoint: /run/containerd/containerd.sock
```

---

### Image Pull Failed

**Symptom**

```
Error: Failed to pull image "myimage:latest": rpc error: ...
```

**Possible Causes**

1. Image name typo
2. Cannot access image registry
3. Private registry requires authentication

**Solutions**

1. Verify image name:
   ```bash
   # Test if image exists
   ctr images pull docker.io/library/alpine:latest
   ```

2. Check network connectivity:
   ```bash
   ping docker.io
   curl -I https://registry-1.docker.io/v2/
   ```

3. For private registries, authenticate with `ccon login`:
   ```bash
   ccon login registry.example.com
   ```

4. Use `--pull-policy Never` to skip pulling (requires pre-imported image):
   ```bash
   ccon -p CPU run --pull-policy Never myimage:latest -- cmd
   ```

---

### Container Start Timeout

**Symptom**

```
Error: Container failed to start within timeout
```

**Possible Causes**

1. Image too large, pull timeout
2. Container entrypoint failed
3. Insufficient resources

**Solutions**

1. Pre-pull images to compute nodes:
   ```bash
   pdsh -w compute[01-10] "ctr -n k8s.io images pull myimage:latest"
   ```

2. Verify entrypoint command:
   ```bash
   ccon -p CPU run -it myimage:latest -- /bin/sh
   ```

3. Increase resource quota:
   ```bash
   ccon -p CPU --mem 4G -c 2 run myimage:latest -- cmd
   ```

---

### Permission Denied (Inside Container)

**Symptom**

```
Permission denied: '/data/file'
```

**Possible Cause**

User namespace mapping prevents container user from accessing mounted host files.

**Solutions**

1. Disable user namespace with `--userns=false`:
   ```bash
   ccon -p CPU run --userns=false -v /data:/data myimage -- cmd
   ```

2. Ensure administrator has enabled BindFs feature (see [Container Deployment](../../deployment/container.md))

3. Adjust host directory permissions:
   ```bash
   chmod -R o+rx /data/input
   ```

---

### Cannot Connect to Container

**Symptom**

```
Error: Failed to attach to container 123.1: connection refused
```

**Possible Causes**

1. Container has exited
2. cfored service not running
3. Network issues

**Solutions**

1. Check container status:
   ```bash
   ccon ps -a
   ```

2. Verify cfored service is running:
   ```bash
   systemctl status cfored
   ```

3. Specify target node:
   ```bash
   ccon attach -n compute01 123.1
   ```

---

### Port Binding Failed

**Symptom**

```
Error: Port 8080 is already in use
```

**Cause**

Specified host port is already occupied.

**Solutions**

1. Use a different port:
   ```bash
   ccon -p CPU run -p 8081:80 nginx:latest
   ```

2. Let system auto-assign port (specify only container port):
   ```bash
   ccon -p CPU run -p 80 nginx:latest
   ```

---

### GPU Device Unavailable

**Symptom**

```
Error: Requested GPU resources not available
```

**Possible Causes**

1. No available GPU on node
2. GPU resources occupied
3. GRES configuration error

**Solutions**

1. Check available GPU resources:
   ```bash
   cinfo -N
   ```

2. Specify correct GPU type:
   ```bash
   ccon -p GPU --gres gpu:a100:1 run pytorch/pytorch:latest -- python train.py
   ```

3. Verify partition has GPU nodes:
   ```bash
   cinfo -p GPU
   ```

---

### Empty Container Logs

**Symptom**

```bash
ccon logs 123.1
# No output
```

**Possible Causes**

1. Container outputs to file instead of stdout
2. Container hasn't produced output yet
3. Logs have been cleaned up

**Solutions**

1. Verify program outputs to stdout/stderr:
   ```bash
   ccon exec 123.1 cat /app/logs/output.log
   ```

2. Use `-f` to follow real-time logs:
   ```bash
   ccon logs -f 123.1
   ```

---

### Invalid Step ID

**Symptom**

```
Error: Invalid container ID format
```

**Cause**

Container ID format is incorrect.

**Solution**

Use correct format `JOBID.STEPID`:

```bash
# View container IDs
ccon ps -a

# Correct format
ccon logs 123.1
ccon attach 123.0
```

---

### exec Command Failed

**Symptom**

```
Error: Command not found in container
```

**Cause**

Specified command doesn't exist in container image.

**Solutions**

1. Check available shells:
   ```bash
   ccon exec 123.1 /bin/sh -c "which bash sh"
   ```

2. Use `/bin/sh` instead of `/bin/bash`:
   ```bash
   ccon exec -it 123.1 /bin/sh
   ```

---

## Logs and Diagnostics

### View craned Logs

```bash
# View container-related logs
grep -i "cri\|container\|pod" /var/crane/craned/craned.log

# Real-time follow
tail -f /var/crane/craned/craned.log | grep -i container
```

### View Supervisor Logs

```bash
ls /var/crane/supervisor/
cat /var/crane/supervisor/supervisor.log
```

### Verify CRI Connection

```bash
# Test with crictl
sudo crictl --runtime-endpoint unix:///run/containerd/containerd.sock version
sudo crictl --runtime-endpoint unix:///run/containerd/containerd.sock images
```

### Check Container Runtime Status

```bash
systemctl status containerd
journalctl -u containerd -f
```

## Error Code Reference

Container operations may return these error codes. See [Error Code Reference](../error_code.md) for the complete list.

| Error Code | Description | Solution |
|:-----------|:------------|:---------|
| `ERR_NO_RESOURCE` | Insufficient resources | Reduce resource request or wait for release |
| `ERR_INVALID_PARAM` | Invalid parameter | Check command parameter format |
| `ERR_SUPERVISOR` | Supervisor internal error | Check Supervisor logs |
| `ERR_SYSTEM_ERR` | System error | Check system logs |
| `ERR_RPC_FAILURE` | RPC call failed | Check network and service status |

## Getting Help

If the above solutions don't resolve your issue:

1. **Collect diagnostic information**:
   ```bash
   ccon --debug-level trace run ...
   ```

2. **View detailed errors**:
   ```bash
   ccon inspect 123.1
   ccon inspectp 123
   ```

3. **Check system status**:
   ```bash
   cinfo -N
   cqueue -j <job_id>
   ```

4. **Contact administrator** with:
   - Complete error output
   - Command used
   - Job ID
   - Relevant log excerpts

## See Also

- [Container Deployment](../../deployment/container.md) - Administrator configuration guide
- [Core Concepts](concepts.md) - Understanding Pods and container steps
- [ccon Command Reference](../../command/ccon.md) - Complete command reference
- [Error Code Reference](../error_code.md) - System error codes
