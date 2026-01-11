# Container Troubleshooting

This document summarizes common issues, error messages, and solutions for Container Support.

## Logs and Diagnostics

When encountering issues with Container Support, it is recommended to collect the following logs and diagnostic information to assist troubleshooting:

### View Supervisor Logs

```bash
ls /var/crane/supervisor/
cat /var/crane/supervisor/JOBID.STEPID.log
```

### Verify CRI Connection

!!! note
    Administrator access only.

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

## Common Errors and Solutions

### Container Feature Not Enabled

**Symptom**

```
Error: Failed to pull image "myimage:latest" for ...
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

### Invalid Step ID

**Symptom**

```
Error: Invalid container ID format
```

**Possible Cause**

Container ID format is incorrect.

**Solution**

Use correct format `JOBID.STEPID`:

!!! note
    Use `ccon ps -a` to view container ID list. Step ID 0 is for Daemon Step (Pod), which cannot be operated by users.

```bash
# View container IDs
ccon ps -a

# Correct format
ccon logs 123.1
ccon attach 123.1
```

---

### Cannot Connect to Container

**Symptom**

```
Error: Failed to attach to container 123.1: connection refused
```

**Possible Causes**

1. Container has exited
2. Job has been queued too long, CLI connection timeout
3. CRI runtime (e.g., containerd) not configured to allow remote connections
4. Network issues

**Solutions**

1. Check container status:
   ```bash
   ccon ps -a
   ```
   If container has exited, check exit reason. If job is still queued, wait for successful scheduling before retrying.

2. Verify Containerd has enabled remote container connection feature.

---

### Empty Container Logs

**Symptom**

```bash
ccon logs 123.1
# No output
```

**Possible Causes**

1. Process inside container is configured to output to file
2. Container hasn't produced output yet
3. Logs have been manually cleaned
4. Log folder is not on shared storage, current node cannot access

**Solutions**

1. Check if container failed to start
2. Check if log files exist on the container's running node
3. Verify if the process inside container produces output, and if output has been manually cleaned

---

### exec Command Failed

**Symptom**

```
Error: Command not found in container
```

**Cause**

Specified command doesn't exist in container image.

**Solution**

For example with Bash, some container images don't have Bash, only Sh:

1. Check available shells:
   ```bash
   ccon exec 123.1 /bin/sh -c "which bash sh"
   ```

2. Use `/bin/sh` instead of `/bin/bash`:
   ```bash
   ccon exec -it 123.1 /bin/sh
   ```

---

### Image Pull Failed

**Symptom**

```
Error: Failed to pull image "myimage:latest" for ...
```

**Possible Causes**

1. Image name typo
2. Network cannot access image registry
3. Private registry requires authentication

**Solutions**

1. For private registries, authenticate with `ccon login`:
   ```bash
   ccon login registry.example.com
   ```

2. Use `--pull-policy Never` to skip pulling (if image already exists locally on node):
   ```bash
   ccon -p CPU run --pull-policy Never myimage:latest -- cmd
   ```

Administrators can use ctr/crictl tools to check CRI runtime's image pull status, for example:

```bash
crictl images
```

---

### Permission Denied Inside Container

**Symptom**

```
Permission denied: '/data/file'
```

**Possible Cause**

User namespace mapping failure prevents container user from accessing mounted host files.

**Solutions**

1. Disable user namespace with `--userns=false`:
   ```bash
   ccon -p CPU run --userns=false -v /data:/data myimage -- cmd
   ```

2. System kernel version too low, or mounted directory's filesystem doesn't support ID-Mapped Mounts.
    - Ask administrator to upgrade kernel.
    - Ensure mounted directory is on a filesystem that supports ID-Mapped Mounts, or ask administrator to configure BindFs solution.

---

### GPU Device Unavailable

**Symptom**

Step submission requested GPU resources, GPU works on bare metal outside container, but GPU devices are not accessible inside container.

**Possible Cause**

Administrator has not correctly configured container runtime to enable GPU support.

**Solution**

Please refer to [Container Deployment](../../deployment/container.md) and contact GPU vendor for correct configuration method (e.g., NVIDIA Container Toolkit, Ascend Docker Toolkit, etc.).

---

### Port Binding Failed

**Symptom**

```
Error: Port 8080 is already in use
```

**Cause**

Specified host port is already occupied.

**Solution**

Use a different port:

```bash
ccon -p CPU run -p 8081:80 nginx:latest
```

## Error Code Reference

Container operations may return these error codes. See [Error Code Reference](../error_code.md) for the complete list.

| Error Code | Description | Possible Causes | Solution |
|:-----------|:------------|:----------------|:---------|
| `ERR_CRI_GENERIC` | CRI runtime error | Internal error from CRI runtime (containerd/CRI-O) | Check container runtime logs, verify image exists and container config is correct |
| `ERR_CRI_DISABLED` | Container support disabled | Cluster has not configured container support | Contact administrator to enable container support (see [Container Deployment](../../deployment/container.md)) |
| `ERR_CRI_CONTAINER_NOT_READY` | Container not ready | Job is pending or container has not finished starting | Please wait for job to enter Running state, check container status with `ccon ps` |
| `ERR_CRI_MULTIPLE_NODES` | Multi-node operation unsupported | Attempted unsupported container operation on multi-node step | Some container operations only support single-node steps |

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
    - Command used
    - Job ID
    - Complete error output
    - Relevant Supervisor log excerpts

## See Also

- [Container Deployment](../../deployment/container.md) - Administrator configuration guide
- [Core Concepts](concepts.md) - Understanding Pods and container steps
- [ccon Command Manual](../../command/ccon.md) - Complete command reference
- [Error Code Reference](../error_code.md) - System error codes

````