# Container Deployment

This guide explains how to enable and configure Container Support in CraneSched clusters, allowing users to run containerized jobs through the CRI (Container Runtime Interface).

!!! warning "Experimental Feature"
    Container Support is currently experimental. Thorough testing is recommended before production deployment.

## Prerequisites

### Container Runtime

CraneSched interacts with container runtimes via the CRI interface, supporting CRI-compatible runtimes like **containerd** and **CRI-O**.

=== "containerd (Recommended)"

    ```bash
    # Rocky Linux 9 / RHEL 9
    sudo dnf install -y containerd
    sudo systemctl enable --now containerd

    # Debian / Ubuntu
    sudo apt install -y containerd
    sudo systemctl enable --now containerd
    ```

    Verify installation:

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

    Verify installation:

    ```bash
    sudo crictl version
    ```

### Runtime Socket

Ensure the container runtime's CRI socket is accessible:

| Runtime | Default Socket Path |
|:--------|:-------------------|
| containerd | `/run/containerd/containerd.sock` |
| CRI-O | `/run/crio/crio.sock` |

Check socket existence:

```bash
ls -la /run/containerd/containerd.sock
# or
ls -la /run/crio/crio.sock
```

### Optional Dependencies

For user namespace mapping (BindFs) functionality, install additional packages:

```bash
# Rocky Linux 9 / RHEL 9
sudo dnf install -y bindfs fuse3

# Debian / Ubuntu
sudo apt install -y bindfs fuse3
```

## Configuration

Configure container options in `/etc/crane/config.yaml`. Here's a complete example with field descriptions:

```yaml
Container:
  # Enable container feature
  Enabled: true
  
  # Container temporary data directory (relative to CraneBaseDir)
  TempDir: supervisor/containers/
  
  # CRI runtime service socket
  RuntimeEndpoint: /run/containerd/containerd.sock
  
  # CRI image service socket (usually same as RuntimeEndpoint)
  ImageEndpoint: /run/containerd/containerd.sock
  
  # BindFs configuration (optional, for user namespace mapping)
  BindFs:
    Enabled: false
    BindfsBinary: bindfs
    FusermountBinary: fusermount3
    MountBaseDir: /mnt/crane
```

### Configuration Fields

#### Core Configuration

| Field | Type | Default | Description |
|:------|:-----|:--------|:------------|
| `Enabled` | bool | `false` | Whether to enable container feature. Set to `true` to enable |
| `TempDir` | string | `supervisor/containers/` | Temporary data directory during container runtime, relative to `CraneBaseDir`. Stores container metadata, logs, etc. |
| `RuntimeEndpoint` | string | — | **Required**. Unix socket path for CRI runtime service, used for container lifecycle management (create, start, stop, etc.) |
| `ImageEndpoint` | string | Same as `RuntimeEndpoint` | Unix socket path for CRI image service, used for image pulling and management. Usually same as `RuntimeEndpoint` |

#### BindFs Configuration

BindFs implements user ID mapping mounts from host directories to containers, resolving permission issues under user namespaces.

| Field | Type | Default | Description |
|:------|:-----|:--------|:------------|
| `BindFs.Enabled` | bool | `false` | Whether to enable BindFs functionality |
| `BindFs.BindfsBinary` | string | `bindfs` | bindfs executable path |
| `BindFs.FusermountBinary` | string | `fusermount3` | fusermount executable path (for unmounting FUSE filesystems) |
| `BindFs.MountBaseDir` | string | `/mnt/crane` | Base directory for BindFs mount points. Can be absolute path or relative to `CraneBaseDir` |

!!! tip "When BindFs is Needed"
    When running containers with user namespace (`--userns`) and mounting host directories, BindFs automatically handles UID/GID mapping, ensuring container users can properly access mounted files.

## Deployment Steps

### 1. Install Container Runtime

Install and start the container runtime on all compute nodes (see Prerequisites).

### 2. Verify Runtime Connection

Test runtime functionality using `crictl` or `ctr`:

```bash
# containerd
sudo crictl --runtime-endpoint unix:///run/containerd/containerd.sock version

# CRI-O
sudo crictl --runtime-endpoint unix:///run/crio/crio.sock version
```

Expected output:

```
Version:  0.1.0
RuntimeName:  containerd
RuntimeVersion:  v1.7.x
RuntimeApiVersion:  v1
```

### 3. Prepare Configuration

Edit `/etc/crane/config.yaml`, add container configuration:

```yaml
Container:
  Enabled: true
  TempDir: supervisor/containers/
  RuntimeEndpoint: /run/containerd/containerd.sock
  ImageEndpoint: /run/containerd/containerd.sock
```

### 4. Distribute Configuration

Sync configuration file to all nodes:

```bash
pdcp -w compute[01-10] /etc/crane/config.yaml /etc/crane/
```

### 5. Restart Services

Restart craned service on compute nodes to load new configuration:

```bash
pdsh -w compute[01-10] "systemctl restart craned"
```

### 6. Verify Container Feature

Run a test container from any job submission node:

```bash
# Submit simple container job
ccon run -p CPU alpine:latest -- echo "Hello from container"

# Check job status
cqueue
```

On success, should output:

```
Hello from container
```

## Image Management

### Image Sources

Container images can be obtained from:

- **Public Registries**: Docker Hub, GHCR, Quay.io, etc.
- **Private Registries**: Enterprise internal Registry
- **Local Images**: Pre-imported via `ctr` or `crictl`

### Pre-pull Images

To reduce job startup latency, pre-pull common images on compute nodes:

```bash
# Using containerd
pdsh -w compute[01-10] "ctr -n k8s.io images pull docker.io/library/python:3.11"

# Using crictl
pdsh -w compute[01-10] "crictl pull python:3.11"
```

### Private Registry Authentication

For private registry access, users can configure authentication via `ccon login`:

```bash
ccon login registry.example.com
```

## Troubleshooting

### Common Issues

#### craned Startup Failure: RuntimeEndpoint Not Configured

**Error Message**: `RuntimeEndpoint is not configured.`

**Solution**: Ensure `RuntimeEndpoint` is properly configured when `Container.Enabled` is `true` in `config.yaml`.

#### Cannot Connect to Container Runtime

**Error Message**: `Failed to get CRI version: ...`

**Troubleshooting Steps**:

1. Check container runtime service status:
   ```bash
   systemctl status containerd
   ```

2. Check socket permissions:
   ```bash
   ls -la /run/containerd/containerd.sock
   ```

3. Manually test CRI connection:
   ```bash
   sudo crictl --runtime-endpoint unix:///run/containerd/containerd.sock version
   ```

#### Image Pull Failed

**Troubleshooting Steps**:

1. Check network connectivity:
   ```bash
   ping docker.io
   ```

2. Check DNS resolution:
   ```bash
   nslookup registry-1.docker.io
   ```

3. View craned logs:
   ```bash
   journalctl -u craned -f
   ```

#### BindFs Mount Failed

**Troubleshooting Steps**:

1. Verify bindfs and fuse3 are installed:
   ```bash
   which bindfs fusermount3
   ```

2. Check if `MountBaseDir` exists with correct permissions:
   ```bash
   ls -la /mnt/crane
   ```

3. Ensure FUSE kernel module is loaded:
   ```bash
   lsmod | grep fuse
   ```

### Log Locations

Container-related logs are located at:

- **craned logs**: `/var/crane/craned/craned.log` (or configured path under `CraneBaseDir`)
- **Supervisor logs**: `/var/crane/supervisor/`
- **Container temp data**: `/var/crane/supervisor/containers/` (or configured `TempDir`)

View craned container-related logs:

```bash
grep -i "cri\|container" /var/crane/craned/craned.log
```

## Rolling Back Container Feature

To disable container feature:

1. Edit `/etc/crane/config.yaml`:
   ```yaml
   Container:
     Enabled: false
   ```

2. Sync configuration and restart services:
   ```bash
   pdcp -w compute[01-10] /etc/crane/config.yaml /etc/crane/
   pdsh -w compute[01-10] "systemctl restart craned"
   ```

3. Verify container feature is disabled:
   ```bash
   ccon run -p CPU alpine:latest -- echo "test"
   # Should return error: container feature not enabled
   ```

## Related Documentation

- [Container Support Overview](../reference/container/index.md): Understand overall positioning and advantages of container features
- [Core Concepts](../reference/container/concepts.md): Understand container jobs, Pods, resource models, and other concepts
- [Quick Start](../reference/container/quickstart.md): Quick experience with container job submission
- [Cluster Configuration](./configuration/config.md): Complete configuration file documentation
