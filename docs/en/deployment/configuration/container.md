# Container Support Configuration


CraneSched includes experimental support for running containerized workloads through the Container Runtime Interface (CRI). This feature allows you to run tasks in containers managed by CRI-compatible runtimes like containerd or CRI-O.


!!! warning "Experimental Feature"
    Container support is currently experimental. Please test thoroughly in a non-production environment before deploying to production.


## Prerequisites


Before enabling container support, ensure you have:


1. A CRI-compatible container runtime installed on compute nodes (e.g., containerd, CRI-O)
2. The runtime socket accessible on the compute nodes
3. Appropriate user permissions to interact with the container runtime


### Installing containerd (Recommended)


On each compute node:


```bash
# Install containerd
sudo apt-get update
sudo apt-get install -y containerd

# Configure containerd
sudo mkdir -p /etc/containerd
sudo containerd config default | sudo tee /etc/containerd/config.toml

# Start and enable containerd
sudo systemctl enable containerd
sudo systemctl start containerd

# Verify installation
sudo ctr version
```

For other Linux distributions, refer to the [containerd installation guide](https://github.com/containerd/containerd/blob/main/docs/getting-started.md).

## Configuration

Enable and configure container support in `/etc/crane/config.yaml`:

```yaml
Container:
  # Enable container support
  Enabled: true
  
  # Temporary directory for container operations (relative to CraneBaseDir)
  TempDir: supervisor/containers/
  
  # Path to the container runtime socket
  RuntimeEndpoint: /run/containerd/containerd.sock
  
  # Path to the image service socket (usually same as RuntimeEndpoint)
  ImageEndpoint: /run/containerd/containerd.sock
```

### Configuration Options

| Option | Description | Default | Required |
|--------|-------------|---------|----------|
| `Enabled` | Enable container support | `false` | Yes |
| `TempDir` | Temporary directory for container data (relative to CraneBaseDir) | `supervisor/containers/` | No |
| `RuntimeEndpoint` | Path to CRI runtime socket | N/A | Yes (if enabled) |
| `ImageEndpoint` | Path to image service socket | Same as RuntimeEndpoint | No |

!!! tip "Socket Paths"
    Common runtime socket paths:
    
    - **containerd**: `/run/containerd/containerd.sock`
    - **CRI-O**: `/var/run/crio/crio.sock`

## Usage

Once container support is enabled, users can submit containerized tasks using the `--container` option with task submission commands.

### Basic Container Job

```bash
cbatch --container=docker.io/library/ubuntu:22.04 job_script.sh
```

### Interactive Container Session

```bash
calloc --container=docker.io/library/python:3.11 -N 1 -c 4
```

### Container with GPU

```bash
cbatch --container=nvcr.io/nvidia/cuda:12.0-base \
       --gres=gpu:1 \
       gpu_job.sh
```

## Container Image Management

### Image Pull Policies

CraneSched supports three image pull policies:

1. **Always**: Always pull the image, even if it exists locally
2. **IfNotPresent**: Pull only if the image is not present locally (default)
3. **Never**: Never pull the image; fail if not present locally

Specify the pull policy in your job script or through submission commands.

### Private Registries

To use images from private registries, configure authentication in your job submission:

```bash
cbatch --container=registry.example.com/private/image:tag \
       --container-user=username \
       --container-password=password \
       job.sh
```

!!! warning "Security"
    Avoid storing credentials in plain text. Consider using environment variables or secure credential management systems.

## Container Lifecycle

When a containerized job is submitted:

1. **Pod Sandbox Creation**: CraneSched creates a pod sandbox for the job
2. **Image Pull**: The container image is pulled according to the pull policy
3. **Container Creation**: A container is created within the pod sandbox
4. **Container Start**: The container is started and the job executes
5. **Cleanup**: Upon job completion or cancellation, the container and pod are removed

## Troubleshooting

### Container fails to start

**Check runtime status:**

```bash
sudo systemctl status containerd
```

**Verify socket permissions:**

```bash
ls -l /run/containerd/containerd.sock
```

**Check CraneSched logs:**

```bash
tail -f /var/crane/craned/craned.log
```

### Image pull failures

**Test image pull manually:**

```bash
sudo ctr image pull docker.io/library/ubuntu:22.04
```

**Check network connectivity:**

```bash
ping registry-1.docker.io
```

**Verify proxy settings (if behind a proxy):**

Edit `/etc/containerd/config.toml` and configure HTTP proxy settings.

### Permission denied errors

Ensure the CraneSched user has permission to access the container runtime socket:

```bash
sudo usermod -aG docker crane  # If using Docker socket
sudo chmod 666 /run/containerd/containerd.sock  # Or adjust socket permissions
```

## Limitations

Current limitations of the experimental container support:

- Multi-step jobs in containers are not yet fully supported
- Some advanced CRI features may not be exposed
- Container networking is limited to host network mode
- Volume mounting is currently restricted

## Performance Considerations

- Container startup adds overhead compared to native process execution
- Image pulls can be time-consuming for large images
- Consider using a local image registry for frequently-used images
- Pre-pull images on compute nodes to reduce job startup time

## Security Best Practices

1. **Use specific image tags** instead of `latest` for reproducibility
2. **Scan images for vulnerabilities** before deployment
3. **Limit container privileges** through runtime policies
4. **Use read-only root filesystems** where possible
5. **Regularly update base images** to include security patches

## Future Enhancements

Planned improvements for container support:

- Full multi-step job support in containers
- Advanced networking configurations
- Flexible volume mounting
- Integration with Kubernetes CRI
- Container resource monitoring and metrics

For questions or issues, please refer to the [CraneSched documentation](https://pkuhpc.github.io/CraneSched/) or open an issue on GitHub.