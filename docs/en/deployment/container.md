# Container Feature Deployment

This guide explains how to enable and configure the container feature in CraneSched clusters, allowing users to run containerized jobs through the CRI (Container Runtime Interface).

## Environment Preparation

### Container Runtime

CraneSched interacts with container runtimes via the CRI interface, supporting CRI-compatible runtimes like **containerd** and **CRI-O**.

!!! warning "Version Requirement"
    The container runtime must support **CRI v1 API**. For containerd, version **≥ 1.7** is required (2.0+ recommended).

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

    Alternatively, you can also use CraneSched's "Container Device Interface" feature to inject various types of devices such as GPU/NPU/RDMA NICs into containers.

=== "NVIDIA GPU"

    Refer to the [NVIDIA Container Toolkit installation guide](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html) to install the NVIDIA Container Toolkit.

    After installation, follow the "Configuring containerd" or "Configuring CRI-O" sections in that document to enable GPU support.

=== "Huawei Ascend NPU"

    Refer to [MindCluster - Containerized Support Feature Guide](https://www.hiascend.com/document/detail/zh/mindcluster/72rc1/clustersched/dlug/dlruntime_ug_002.html) to install the Ascend NPU container runtime plugin.

    After installation, follow the [Using in Containerd Client](https://www.hiascend.com/document/detail/zh/mindcluster/72rc1/clustersched/dlug/dlruntime_ug_006.html) tutorial to enable NPU support.

=== "AMD GPU"

    Refer to the [AMD Container Toolkit installation guide](https://instinct.docs.amd.com/projects/container-toolkit/en/latest/index.html) to install AMD Container Toolkit.

    **Note:** CraneSched container support for AMD GPU is still under testing and evaluation.

### Container Device Interface

In addition to the vendor-specific runtime hooks described above, CraneSched also supports injecting devices into containers via the **Container Device Interface (CDI)** standard. CDI is a vendor-agnostic device injection specification that uses JSON specification files (CDI Specs) to describe devices and the device nodes, mounts, and environment variables required for injection. It is particularly suitable for:

- RDMA/InfiniBand and other non-GPU devices
- Scenarios where one physical device corresponds to multiple device file nodes
- Devices that require additional mounts or environment variables to be injected into containers

The container runtime reads CDI Specs when creating containers and automatically injects devices according to the specification. When submitting container jobs, CraneSched passes the CDI Fully Qualified Device Names of allocated devices to the container runtime via the CRI interface, which then completes the device injection.

#### Enable CDI Support

=== "containerd"

    Edit `/etc/containerd/config.toml` to enable CDI and configure CDI Spec directories:

    ```toml
    [plugins.'io.containerd.grpc.v1.cri']
      enable_cdi = true
      cdi_spec_dirs = ["/etc/cdi", "/var/run/cdi"]
    ```

    After configuration, restart the containerd service:

    ```bash
    systemctl restart containerd
    ```

=== "CRI-O"

    CRI-O enables CDI support by default since v1.23. No additional configuration is needed. CDI Specs are searched in `/etc/cdi` and `/var/run/cdi` by default.

#### Generate CDI Configuration

CDI Spec files are in JSON format and should be placed in the `/etc/cdi/` directory. Different hardware vendors provide different generation tools. For example:

| Device Type | Generation Tool | Reference |
|:-----------|:---------------|:----------|
| NVIDIA GPU | `nvidia-ctk cdi generate` | [NVIDIA CDI Documentation](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/cdi-support.html) |
| RDMA/InfiniBand | `rdma-cdi` or manual creation | [RDMA NIC CDI Generation Tool](https://github.com/Nativu5/rdma-cdi) |

After generating the configuration with the tool, verify with:

```bash
# Inspect the spec file directory
ls /etc/cdi/
```

#### Configure GRES to Use CDI

In the Gres configuration of `/etc/crane/config.yaml`, use the `DeviceCDIRegex` or `DeviceCDIList` fields to specify the CDI Fully Qualified Device Name for each device. CDI names correspond to device files (DeviceFileRegex / DeviceFileList) in order, one-to-one.

??? "Example Configuration"
    ```yaml
    Nodes:
    - name: "node01"
        cpu: 64
        memory: 256G
        gres:
        - name: rdma
            type: cx5vf
            DeviceFileList:
            - /dev/infiniband/uverbs3,/dev/infiniband/rdma_cm
            - /dev/infiniband/uverbs4,/dev/infiniband/rdma_cm
            DeviceCDIList:
            - rdma/mlx5_3=0000:17:00.2
            - rdma/mlx5_4=0000:17:00.3
    ```

For the complete Gres configuration documentation, see [Cluster Configuration - Gres Configuration](./configuration/config.md#gres-configuration).

### Container Network Plugins

CraneSched uses the Container Network Interface (CNI) to provide container network isolation and communication. Install a suitable CNI plugin based on the container runtime you choose.

To flexibly integrate existing CNI plugins, CraneSched provides a set of **Crane CNI tools**, currently including:

- **Meta CNI**: A bridge between CraneSched and other CNI plugins, responsible for passing job information to CNI plugins.
- **DNS CNI (dns-register)**: Registers container FQDN and IP with CoreDNS during container creation, enabling containers within a cluster to access each other by hostname.

In practice, you can choose compatible third-party CNI plugins and combine them with the Crane CNI tools to implement container networking.

The third-party CNI plugins that have passed compatibility testing include: **Calico**. Therefore, we recommend deploying Meta CNI + DNS CNI + Calico following the tutorial below.

#### Crane CNI Tools

In the CraneSched-FrontEnd root directory, run `make tool` to build the Crane CNI tools. The build outputs are located in `build/tool/`.

Below are the descriptions of each component in the Crane CNI tools:

=== "Meta CNI"
    Crane Meta CNI does not perform network configuration itself. It serves as a bridge between CraneSched and the actual CNI plugins. Meta CNI is the unified entry point for all CNI plugins in CraneSched; other CNI plugins are orchestrated and invoked by Meta CNI. Different clusters and different plugins require different Meta CNI configurations.

    Refer to the [Meta CNI README](https://github.com/PKUHPC/CraneSched-FrontEnd/blob/master/tool/meta-cni/README.md) for complete configuration instructions and examples.

=== "DNS CNI"
    Crane DNS CNI plugin (dns-register) registers the container's FQDN and IP in etcd during container creation, for CoreDNS to query.

    Before using dns-register, you need to deploy [CoreDNS + etcd plugin](https://coredns.io/plugins/etcd/) and ensure the etcd `path` in the CoreDNS Corefile is set to `/coredns` (the prefix used by dns-register for writing records).

    Refer to the [dns-register README](https://github.com/PKUHPC/CraneSched-FrontEnd/blob/master/tool/dns-register/README.md) for complete configuration instructions.

#### Example: Calico

!!! warning
    This section is still being improved. More details will be added later.

**[Calico](https://github.com/projectcalico/calico)** is a popular CNI plugin that supports network policy and high-performance networking. CraneSched has completed compatibility testing with Calico.

The following is a Meta CNI + Calico + dns-register + Port Mapping + Bandwidth configuration. Write it to `/etc/cni/net.d/00-crane-calico.conf`. If there is no higher-priority configuration file, it will take effect on the next container startup.

??? "Example configuration"
    Refer to the [example configuration file](https://github.com/PKUHPC/CraneSched-FrontEnd/blob/master/tool/meta-cni/config/00-crane-calico.conf).

    Some fields in this configuration file depend on your actual cluster deployment. Adjust them as needed.

#### Example: RDMA VF Network

For resources such as RDMA VFs that require both device injection and network
interface configuration, **CDI** and **Meta CNI** should be used together:

- CDI injects device nodes such as `uverbs` and `rdma_cm` into the container.
- Meta CNI moves the allocated VF NICs into the container network namespace and
  creates interfaces such as `rdma0` and `rdma1`.

In other words, start with the CDI-related steps in
[`Configure GRES to Use CDI`](./container.md#configure-gres-to-use-cdi) and
configure `DeviceCDIRegex` or `DeviceCDIList`. Then add the
`DeviceCniPipeline` field in `/etc/crane/config.yaml` and provide the
corresponding Meta CNI configuration.

??? "GRES Configuration Example (/etc/crane/config.yaml)"
    The example below extends the `Configure GRES to Use CDI` configuration by
    adding `DeviceCniPipeline: rdma`:

    ```yaml
    Nodes:
      - name: "node01"
        gres:
          - name: rdma
            type: cx5vf
            DeviceCniPipeline: rdma
            DeviceFileList:
              - /dev/infiniband/uverbs3,/dev/infiniband/umad3,/dev/infiniband/rdma_cm
              - /dev/infiniband/uverbs4,/dev/infiniband/umad4,/dev/infiniband/rdma_cm
            DeviceCDIList:
              - rdma/pci-0000-17-00-2=0000:17:00.2
              - rdma/pci-0000-17-00-3=0000:17:00.3
    ```

    The fields have the following roles:

    - `DeviceFileList`: Declares the RDMA device files to inject into the container.
    - `DeviceCDIList`: Provides CDI fully qualified names for those device files.
    - `DeviceCniPipeline`: Declares that these GRES devices also need network configuration through Meta CNI.

    The value of `DeviceCniPipeline` must match the `name` of the corresponding
    network pipeline in the Meta CNI configuration.

??? "Meta CNI Configuration Example"
    For an RDMA VF network example, refer to:
    [tool/meta-cni/config/00-crane-calico-sriov.conf](https://github.com/PKUHPC/CraneSched-FrontEnd/blob/master/tool/meta-cni/config/00-crane-calico-sriov.conf).

    This example contains two classes of network interfaces:

    - `ethernet`: The container's primary network interface, typically used for regular IP communication.
    - `rdma`: A dedicated RDMA VF network interface, created dynamically according to the number of GRES devices allocated to the job.

    The RDMA-related key points are:

    - `name: rdma`: Must match `DeviceCniPipeline: rdma`.
    - `ifNamePrefix: rdma`: Generates container interface names such as `rdma0` and `rdma1`.
    - `confFromArgs: {"deviceID": "$gres.device"}`: Passes the CDI device identifier for the GRES allocation to the `sriov` plugin.

If you only need to inject RDMA device files into the container and do not need
independent RDMA network interfaces inside the container, you do not need to
configure `DeviceCniPipeline`.

### Advanced Features

CraneSched container features include some advanced capabilities that require additional configuration:

- **Fake Root (User Namespace)**: Use user namespaces to provide "fake root" isolation inside containers; this requires **SubID**. If you need to mount directories and the underlying filesystem does not support ID Mapped Mounts, install **BindFs**.

#### SubID

!!! note
    CraneSched supports automatic management of SubUID/SubGID ranges ("Managed" mode; see the "Container Configuration" section). If you want to manage SubID yourself, follow the instructions below.

In "Managed" mode, CraneSched calculates each user's SubUID/SubGID range start with `BaseOffset + (ID - UidShift) * RangeSize` and maintains system files such as `/etc/subuid`. Here `ID` refers to the user's UID or primary GID, and `UidShift` defaults to `0`. If this mode does not meet your requirements, you can manage SubID directly or using external services.

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
    # Offset subtracted from UID/primary GID in Managed mode
    UidShift: 0

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

!!! warning
    Some LDAP/SSSD deployments assign users and groups from a high UID/GID starting point such as `65536`. If Managed mode uses those raw IDs directly, it can skip a large portion of the available SubID space and may even exceed the container runtime's 32-bit ID mapping limit. In that case, set `UidShift` to the directory service's starting offset, for example `65536`, so that Managed mode allocates SubIDs from a smaller contiguous index. `UidShift` applies to both the UID and the primary GID, so only enable it when your cluster's UID/GID allocation shares the same starting offset.

| Field | Type | Default | Description |
|:-----|:-----|:-------|:-----|
| `Managed` | bool | `true` | Whether CraneSched automatically manages SubID ranges.<br>- `true`: automatically adds and validates SubID ranges<br>- `false`: managed by the administrator |
| `RangeSize` | int | `65536` | Size of SubUID/SubGID range per user. Must be greater than 0; recommended value is 65536 |
| `BaseOffset` | int | `100000` | Base offset for SubID ranges. In Managed mode the formula is `start = BaseOffset + (id - UidShift) * RangeSize` |
| `UidShift` | int | `0` | Managed mode only. Subtracted from the user's UID and primary GID before calculating the SubID range. Useful for LDAP/SSSD environments that allocate IDs from a high starting offset such as `65536` |

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
