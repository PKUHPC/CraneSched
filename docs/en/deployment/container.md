# Container Feature Deployment

This guide explains how to enable and configure the container feature in CraneSched clusters, allowing users to run containerized jobs through the CRI (Container Runtime Interface).

## Environment Preparation

### Container Runtime

CraneSched interacts with container runtimes via the CRI interface, supporting CRI-compatible runtimes like **containerd** and **CRI-O**.

#### Install Container Runtime

=== "containerd (Recommended)"

    We recommend referring to the [containerd official installation guide](https://github.com/containerd/containerd/blob/main/docs/getting-started.md) for the latest steps.

    Some Linux distributions can install containerd via package managers, but the versions may be older. The following commands are for reference only:

    ```bash
    # Rocky Linux 9 / RHEL 9
    dnf install -y containerd
    systemctl enable --now containerd

    # Debian / Ubuntu
    apt install -y containerd
    systemctl enable --now containerd
    ```

=== "CRI-O"

    We recommend referring to the [CRI-O official installation guide](https://github.com/cri-o/cri-o/blob/main/install.md) for the latest steps.

    Some Linux distributions can install CRI-O via package managers, but the versions may be older. The following commands are for reference only:

    ```bash
    # Rocky Linux 9 / RHEL 9
    dnf install -y cri-o
    systemctl enable --now crio

    # Debian / Ubuntu
    apt install -y cri-o
    systemctl enable --now crio
    ```

#### Enable CRI Support

Container runtimes expose their interface through UNIX sockets. Most runtimes enable the CRI service by default:

| Runtime | Default Socket Path |
|:-------|:-------------------|
| containerd | `/run/containerd/containerd.sock` |
| CRI-O | `/run/crio/crio.sock` |

Use the `crictl` tool to check the CRI interface status:

=== "containerd"

    ```bash
    crictl --runtime-endpoint unix:///run/containerd/containerd.sock version
    ```

=== "CRI-O"

    ```bash
    crictl --runtime-endpoint unix:///run/crio/crio.sock version
    ```

Expected output (containerd example):

```
Version:  0.1.0
RuntimeName:  containerd
RuntimeVersion:  2.0.7
RuntimeApiVersion:  v1
```

If errors occur, enable CRI support according to the runtime configuration documentation.

#### Enable Remote Connections

CraneSched allows users to connect to their running container jobs from any node in the cluster. Therefore, configure the container runtime to allow remote connections from other nodes in the cluster.

!!! warning
    Enabling remote connections may introduce security risks. Use firewalls and TLS certificates to restrict access.

=== "containerd"
    Generate the default containerd configuration (if none exists):

    ```bash
    containerd config default
    ```

    Find the following section in `/etc/containerd/config.toml`:

    ```toml
    [plugins.'io.containerd.grpc.v1.cri']
      disable_tcp_service = false       # Enable TCP service
      stream_server_address = '0.0.0.0' # Change to an address reachable within the cluster
      enable_tls_streaming = false      # Enable TLS if needed

    # Configure TLS certificate paths as needed
    [plugins.'io.containerd.grpc.v1.cri'.x509_key_pair_streaming]
      tls_cert_file = ''
      tls_key_file = ''
    ```

    After configuration, restart the containerd service.

    Refer to the [containerd configuration guide](https://github.com/containerd/containerd/blob/main/docs/cri/config.md) for more details.

=== "CRI-O"
    This section is TBD.

#### Enable GPU Support

!!! note
    Enable this only when GPU/NPU resources are configured on cluster nodes. This feature depends on the container runtime and vendor plugins.

=== "NVIDIA GPU"

    Refer to the [NVIDIA Container Toolkit installation guide](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html) to install the NVIDIA Container Toolkit.

    After installation, follow the "Configuring containerd" or "Configuring CRI-O" sections in that document to enable GPU support.

=== "Huawei Ascend NPU"

    Refer to [MindCluster - Containerized Support Feature Guide](https://www.hiascend.com/document/detail/zh/mindcluster/72rc1/clustersched/dlug/dlruntime_ug_002.html) to install the Ascend NPU container runtime plugin.

    After installation, follow the [Using in Containerd Client](https://www.hiascend.com/document/detail/zh/mindcluster/72rc1/clustersched/dlug/dlruntime_ug_006.html) tutorial to enable NPU support.

=== "AMD GPU"

    Refer to the [AMD Container Toolkit installation guide](https://instinct.docs.amd.com/projects/container-toolkit/en/latest/index.html) to install AMD Container Toolkit.

    **Note:** CraneSched container support for AMD GPU is still under testing and evaluation.

### Container Network Plugins

CraneSched uses the Container Network Interface (CNI) to provide container network isolation and communication. Install a suitable CNI plugin based on the container runtime you choose.

CraneSched provides the Crane Meta CNI plugin to flexibly integrate existing CNI plugins (such as Calico). The CNI plugins that have passed compatibility tests currently include Calico.

#### Crane Meta CNI

In the CraneSched-FrontEnd root directory, run `make tool` to build the Meta CNI plugin. The build output is located at `build/tool/meta-cni`.
The Meta CNI plugin must be configured before use.

!!! warning
    Crane Meta CNI does not perform network configuration itself. It serves as a bridge between CraneSched and the actual CNI plugin. Different clusters and different CNI plugins require different Meta CNI configurations.

The example configuration file is located at `tool/meta-cni/config/00-meta.example.conf`. Edit it to match your environment.

After editing, place the file in `/etc/cni/net.d/` (the exact location is determined by the container runtime; keep the path consistent). Multiple configuration files may exist in this directory; the file with the lexicographically earliest name takes precedence.

#### Example: Calico

!!! warning
    This section is still being improved. More details will be added later.

**[Calico](https://github.com/projectcalico/calico)** is a popular CNI plugin that supports network policy and high-performance networking. CraneSched has completed compatibility testing with Calico.

The following is a Meta CNI + Calico + Port Mapping + Bandwidth configuration. Write it to `/etc/cni/net.d/00-crane-calico.conf`. If there is no higher-priority configuration file, it will take effect on the next container startup.

??? "Example configuration"
      ```json
      {
      "cniVersion": "1.0.0",
      "name": "crane-meta",
      "type": "meta-cni",
      "logLevel": "debug",
      "timeoutSeconds": 10,
      "resultMode": "chained",
      "runtimeOverride": {
         "args": [
            "-K8S_POD_NAMESPACE",
            "-K8S_POD_NAME",
            "-K8S_POD_INFRA_CONTAINER_ID",
            "-K8S_POD_UID"
         ],
         "envs": []
      },
      "delegates": [
         {
            "name": "calico",
            "conf": {
            "type": "calico",
            "log_level": "info",
            "datastore_type": "etcdv3",
            "etcd_endpoints": "http://192.168.24.2:2379",
            "etcd_key_file": "",
            "etcd_cert_file": "",
            "etcd_ca_cert_file": "",
            "ipam": {
               "type": "calico-ipam"
            },
            "policy": {
               "type": "none"
            },
            "container_settings": {
               "allow_ip_forwarding": true
            },
            "capabilities": {
               "portMappings": true
            }
            }
         },
         {
            "name": "portmap",
            "conf": {
            "type": "portmap",
            "snat": true,
            "capabilities": {
               "portMappings": true
            }
            }
         },
         {
            "name": "bandwidth",
            "conf": {
            "type": "bandwidth",
            "capabilities": {
               "bandwidth": true
            }
            }
         }
      ]
      }
      ```
    Some fields in this configuration file depend on your actual cluster deployment. Adjust them as needed.

### Advanced Features

CraneSched container features include some advanced capabilities that require additional configuration:

- **Fake Root (User Namespace)**: Use user namespaces to provide "fake root" isolation inside containers; this requires **SubID**. If you need to mount directories and the underlying filesystem does not support ID Mapped Mounts, install **BindFs**.

#### SubID

!!! note
    CraneSched supports automatic management of SubUID/SubGID ranges ("Managed" mode; see the "Container Configuration" section). If you want to manage SubID yourself, follow the instructions below.

In "Managed" mode, CraneSched calculates each user's SubUID/SubGID range start with `BaseOffset + ID * RangeSize` and maintains system files such as `/etc/subuid`. If this mode does not meet your requirements, you can manage SubID directly or using external services.

In "Unmanaged" mode, CraneSched only reads SubID information from the system via the `shadow-utils` APIs. Therefore, administrators must ensure that users on all cluster nodes have consistent, non-overlapping SubID ranges. You can manually edit `/etc/subuid` and `/etc/subgid`, or use LDAP for centralized management.

Some systems can obtain SubID information from an LDAP server via sssd. Refer to documentation for FreeIPA, OpenLDAP, and similar solutions for configuration guidance.

To switch SubID management modes or adjust parameters for Managed mode, see the [Container Configuration](#container-configuration) section below.

#### BindFs

!!! note
    This is required only when running containers with user namespaces (`--userns`, "Fake Root") and the underlying filesystem of the mount directory does not support ID Mapped Mounts.

!!! warning
    Since BindFs is based on FUSE, it may introduce performance overhead. Enable it only when necessary.

[BindFs](https://bindfs.org/) is a FUSE-based filesystem tool that maps real user IDs to container user IDs on filesystems that do not support ID Mapped Mounts. Install it with the following commands:

=== "RHEL/Fedora/CentOS"

    ```bash
    dnf install -y bindfs fuse3
    ```

=== "Debian / Ubuntu"

    ```bash
    apt install -y bindfs fuse3
    ```

After installation, enable BindFs in the [Container Configuration](#container-configuration) section below.

## Deployment Steps

### Modify Configuration File

Edit `/etc/crane/config.yaml` and add the following container configuration:

!!! note
    For the complete container configuration options, see [Container Configuration](#container-configuration).

```yaml
Container:
  Enabled: true
  TempDir: supervisor/containers/
  RuntimeEndpoint: /run/containerd/containerd.sock
  ImageEndpoint: /run/containerd/containerd.sock
  Dns:
    ClusterDomain: "cluster.local"
    Servers: ["127.0.0.1"]
    Searches: []
    Options: []
```

After editing, save and distribute the configuration file to all nodes.

### Restart CraneSched Services

Restart CraneSched services on all nodes to apply the new configuration:

```bash
pdsh -w crane01 "systemctl restart cranectld"
pdsh -w crane[02-04] "systemctl restart craned"
```

### Verify Container Feature

Run a test container on any node that can submit jobs:

```bash
# Submit a simple container job
ccon run -p CPU alpine:latest -- echo "Hello from container"

# Check job status
ccon ps -a
```

## Container Configuration

Configure container-related options in `/etc/crane/config.yaml`. Below is a complete example and field description:

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

  # DNS configuration
  Dns:
    ClusterDomain: "cluster.local"
    Servers: ["127.0.0.1"]
    Searches: []
    Options: []

  # SubUID/SubGID configuration
  SubId:
    # Whether to automatically manage SubID ranges
    Managed: true
    # Size of SubUID/SubGID range per user
    RangeSize: 65536
    # Base offset for SubUID/SubGID ranges
    BaseOffset: 100000

  # BindFs configuration (optional, for user namespace mapping)
  BindFs:
    Enabled: false
    BindfsBinary: /usr/bin/bindfs
    FusermountBinary: /usr/bin/fusermount3
    MountBaseDir: /mnt/crane
```

### Core Configuration

| Field | Type | Default | Description |
|:-----|:-----|:-------|:-----|
| `Enabled` | bool | `false` | Whether to enable the container feature. Set to `true` to enable |
| `TempDir` | string | `supervisor/containers/` | Temporary data directory during container runtime, relative to `CraneBaseDir`. Stores container metadata, logs, etc. |
| `RuntimeEndpoint` | string | - | **Required**. Unix socket path for the CRI runtime service, used for container lifecycle management (create, start, stop, etc.) |
| `ImageEndpoint` | string | Same as `RuntimeEndpoint` | Unix socket path for the CRI image service, used for image pulling and management. Usually the same as `RuntimeEndpoint` |

### DNS Configuration

Container DNS is used to provide domain name resolution services for containers in the cluster. CraneSched generates a unique hostname for each container, and container DNS is required to resolve that hostname in other containers.

| Field | Type | Default | Description |
|:-----|:-----|:-------|:-----|
| `Dns.ClusterDomain` | string | `cluster.local` | Cluster domain. It will be **prepended** as the first search domain in `searches` |
| `Dns.Servers` | string[] | `['127.0.0.1']` | DNS server list (IPv4 only) |
| `Dns.Searches` | string[] | `[]` | Additional search domains |
| `Dns.Options` | string[] | `[]` | DNS options (e.g., `ndots:5`) |

### SubID Configuration

SubID (subordinate user/group IDs) configuration is used for secure container user namespace isolation.

| Field | Type | Default | Description |
|:-----|:-----|:-------|:-----|
| `Managed` | bool | `true` | Whether CraneSched automatically manages SubID ranges.<br>- `true`: automatically adds and validates SubID ranges<br>- `false`: managed by the administrator |
| `RangeSize` | int | `65536` | Size of SubUID/SubGID range per user. Must be greater than 0; recommended value is 65536 |
| `BaseOffset` | int | `100000` | Base offset for SubID ranges. Used to calculate each user's range: `start = BaseOffset + uid * RangeSize` |

### BindFs Configuration

BindFs implements user ID mapping mounts from host directories to containers, resolving permission issues under user namespaces.

| Field | Type | Default | Description |
|:-----|:-----|:-------|:-----|
| `Enabled` | bool | `false` | Whether to enable BindFs |
| `BindfsBinary` | string | `bindfs` | Path to the bindfs executable |
| `FusermountBinary` | string | `fusermount3` | Path to the fusermount executable (used to unmount FUSE filesystems) |
| `MountBaseDir` | string | `/mnt/crane` | Base directory for BindFs mount points. Can be an absolute path or relative to `CraneBaseDir` |

## Image Management

CraneSched does not directly manage image storage. The container runtime is responsible for pulling and storing images.

Container images can be obtained from:

- **Public registries**: Docker Hub, GHCR, Quay.io, etc.
- **Private registries**: Enterprise internal registry
- **Local images**: Pre-imported via `ctr` or `crictl`

We recommend deploying an image accelerator or private registry in the cluster to improve pull speed.

## Troubleshooting

Refer to the [Container Troubleshooting Guide](../reference/container/troubleshooting.md) for common issues and solutions.

## Related Documentation

- [Container Feature Overview](../reference/container/index.md): Understand the overall positioning and advantages of the container feature
- [Core Concepts](../reference/container/concepts.md): Understand container jobs, Pods, resource models, and other concepts
- [Quick Start](../reference/container/quickstart.md): Quickly try container job submission
- [Cluster Configuration](./configuration/config.md): Complete configuration file documentation
