# Cluster Configuration

This guide explains how to configure CraneSched through the `/etc/crane/config.yaml` file to set up your cluster topology, partitions, and scheduling policies.

!!! info
    The configuration file must be **identical on all nodes** (control and compute nodes). Any changes require restarting the affected services.

## Quick Start Example

A minimal configuration for a 4-node cluster:

```yaml
# Cluster identification
ControlMachine: crane01
ClusterName: my_cluster

# Database configuration
DbConfigPath: /etc/crane/database.yaml

# Node definitions
Nodes:
  - name: "crane[01-04]"
    cpu: 4
    memory: 8G

# Partition definitions
Partitions:
  - name: compute
    nodes: "crane[01-04]"
    priority: 5

DefaultPartition: compute
```

## Essential Configuration

### Cluster Settings

Define basic cluster information:

```yaml
# Hostname of the node running cranectld (control node)
ControlMachine: crane01

# Name of this cluster
ClusterName: my_cluster

# Path to database configuration file
DbConfigPath: /etc/crane/database.yaml

# Base directory for CraneSched data and logs
CraneBaseDir: /var/crane/
```

- **ControlMachine**: Must be the actual hostname of your control node
- **ClusterName**: Used for identification in multi-cluster environments
- **CraneBaseDir**: All relative paths are based on this directory

### Node Definitions

Specify compute node resources:

```yaml
Nodes:
  # Node range notation
  - name: "crane[01-04]"
    cpu: 4
    memory: 8G

  # Individual nodes
  - name: "crane05"
    cpu: 8
    memory: 16G

  # Nodes with GPUs
  - name: "crane[06-07]"
    cpu: 8
    memory: 32G
    gres:
      - name: gpu
        type: a100
        DeviceFileRegex: /dev/nvidia[0-3]
```

**Node Parameters:**

- **name**: Hostname or range (e.g., `node[01-10]`)
- **cpu**: Number of CPU cores
- **memory**: Total memory (supports K, M, G, T suffixes)
- **gres**: Generic resources like GPUs (optional)

**Node Range Notation:**

- `crane[01-04]` expands to: crane01, crane02, crane03, crane04
- `cn[1-3,5]` expands to: cn1, cn2, cn3, cn5

### Partition Configuration

Organize nodes into partitions:

```yaml
Partitions:
  # CPU partition
  - name: CPU
    nodes: "crane[01-04]"
    priority: 5

  # GPU partition  
  - name: GPU
    nodes: "crane[05-08]"
    priority: 3
    DefaultMemPerCpu: 4096  # 4GB per CPU (in MB)
    MaxMemPerCpu: 8192      # 8GB maximum per CPU

# Default partition for job submission
DefaultPartition: CPU
```

**Partition Parameters:**

- **name**: Partition identifier
- **nodes**: Node range belonging to this partition
- **priority**: Higher values = higher priority (affects scheduling)
- **DefaultMemPerCpu**: Default memory per CPU in MB (0 = let scheduler decide)
- **MaxMemPerCpu**: Maximum memory per CPU in MB (0 = no limit)

### Scheduling Policy

Configure job scheduling behavior:

```yaml
# Scheduling algorithm
# Options: priority/basic, priority/multifactor
PriorityType: priority/multifactor

# Favor smaller jobs in scheduling
PriorityFavorSmall: true

# Maximum age for priority calculation (days-hours format)
PriorityMaxAge: 14-0

# Priority factor weights
PriorityWeightAge: 500           # Job wait time weight
PriorityWeightFairShare: 10000   # Fair share weight
PriorityWeightJobSize: 0         # Job size weight (0=disabled)
PriorityWeightPartition: 1000    # Partition priority weight
PriorityWeightQoS: 1000000       # QoS priority weight
```

## Network Settings

### Control Node (cranectld)

```yaml
# Listening address and ports for cranectld
CraneCtldListenAddr: 0.0.0.0
CraneCtldListenPort: 10011
CraneCtldForInternalListenPort: 10013
```

### Compute Nodes (craned)

```yaml
# Listening address and port for craned
CranedListenAddr: 0.0.0.0
CranedListenPort: 10010

# Health check settings
Craned:
  PingInterval: 15        # Ping cranectld every 15 seconds
  CraneCtldTimeout: 5     # Timeout for cranectld connection
```

## Advanced Options

### TLS Encryption

Enable encrypted communication between nodes:

```yaml
TLS:
  Enabled: true
  InternalCertFilePath: /etc/crane/tls/internal.pem
  InternalKeyFilePath: /etc/crane/tls/internal.key
  ExternalCertFilePath: /etc/crane/tls/external.pem
  ExternalKeyFilePath: /etc/crane/tls/external.key
  CaFilePath: /etc/crane/tls/ca.pem
  AllowedNodes: "crane[01-10]"
  DomainSuffix: crane.local
```

### Gres Configuration

> Device resource related configuration

Define generic resources like GPUs, NPUs, and other accelerators:

```yaml
Nodes:
  - name: "gpu[01-02]"
    cpu: 16
    memory: 64G
    gres:
      - name: gpu
        type: a100
        # Regex matching device files
        DeviceFileRegex: /dev/nvidia[0-3]
        # Additional device files per GPU
        DeviceFileList:
          - /dev/dri/renderer[0-3]
        # Environment injector for runtime
        EnvInjector: nvidia
```

**Gres Parameters:**

- **name**: Generally the resource type such as: `GPU`, `NPU`, etc.
- **type**: Generally the resource model such as: `A100`, `3090`, etc.
- **DeviceFileRegex**: The device files under the /dev directory corresponding to the resource, suitable for resources where one physical device corresponds to one device file, **each file corresponds to one Gres resource in the system**, supports Regex format. Common device corresponding device files. Such as Nvidia, AMD, Hygon DCU, Ascend, etc.
- **DeviceFileList**: Suitable for **Gres resources where one physical device corresponds to multiple device files under the /dev directory**, each group of files corresponds to one Gres resource in the system, supports Regex format.

Choose one between DeviceFileRegex and DeviceFileList, the above device files must exist, **otherwise Craned will report an error and exit during startup**

- **EnvInjector**: Environment variables that the device needs to inject

    - Optional values: corresponding environment variables
    
    - `nvidia`: `CUDA_VISIBLE_DEVICES`
    - `hip`: `HIP_VISIBLE_DEVICES`
    - `ascend`: `ASCEND_RT_VISIBLE_DEVICES`

- Common vendor device file paths and related configurations

| Vendor | Device File Path | EnvInjector |
| :---------- | :---------------------- | :---------- |
| Nvidia | /dev/nvidia0 ... | nvidia |
| AMD/Hygon DCU | /dev/dri/renderer128... | hip |
| Ascend | /dev/davinci0 ... | ascend |
| Iluvatar | /dev/iluvatar0 ... | nvidia |

### Queue Limits

Control job queue size and scheduling behavior:

```yaml
# Maximum pending jobs in queue (max: 900000)
PendingQueueMaxSize: 900000

# Jobs to schedule per cycle (max: 200000)
ScheduledBatchSize: 100000

# Reject jobs when queue is full
RejectJobsBeyondCapacity: false
```

### Partition Access Control

Restrict partition access by account:

```yaml
Partitions:
  - name: restricted
    nodes: "special[01-04]"
    priority: 10
    # Only these accounts can use this partition
    AllowedAccounts: project1,project2
    
  - name: public
    nodes: "compute[01-20]"
    priority: 5
    # All accounts except these can use this partition
    DeniedAccounts: banned_account
```

!!! warning
    `AllowedAccounts` and `DeniedAccounts` are mutually exclusive. If `AllowedAccounts` is set, `DeniedAccounts` is ignored.

### Logging and Debugging

Configure log levels and locations:

```yaml
# Log levels: trace, debug, info, warn, error
CraneCtldDebugLevel: info
CranedDebugLevel: info

# Log file paths (relative to CraneBaseDir)
CraneCtldLogFile: cranectld/cranectld.log
CranedLogFile: craned/craned.log

# Run in foreground (useful for debugging)
CraneCtldForeground: false
CranedForeground: false
```


### Supervisor Configuration

Supervisor is CraneSched's job execution management component, responsible for controlling job steps on compute nodes.

```yaml
Supervisor:
  # Path to supervisor executable
  Path: /usr/libexec/csupervisor
  
  # Supervisor log level: trace, debug, info, warn, error
  DebugLevel: trace
  
  # Log directory (relative to CraneBaseDir)
  LogDir: supervisor
```

**Supervisor Parameters:**

- **Path**: Full path to the supervisor executable. The default path is `/usr/libexec/csupervisor`, **which is typically set correctly during installation**.
- **DebugLevel**: Controls the verbosity of supervisor logs. Available values include `trace` (most verbose), `debug`, `info`, `warn`, `error` (least verbose). For production environments, `info` or `warn` is recommended.
- **LogDir**: Directory for supervisor log files, relative to the `CraneBaseDir` setting. Log files are helpful for diagnosing job execution issues.

!!! tip
    When troubleshooting job execution problems, you can temporarily set `DebugLevel` to `debug` or `trace` for more detailed log information.

## Container Support

CraneSched supports running jobs in containers through CRI (Container Runtime Interface):

```yaml
Container:
  # Enable container support (experimental)
  Enabled: false
  
  # Temporary directory for container data (relative to CraneBaseDir)
  TempDir: supervisor/containers/
  
  # Path to container runtime socket
  RuntimeEndpoint: /run/containerd/containerd.sock
  
  # Path to image service socket (usually same as RuntimeEndpoint)
  ImageEndpoint: /run/containerd/containerd.sock
```

!!! info "Experimental Feature"
    Container support is currently experimental. There may be limitations and problems.

**Requirements:**

- CRI-compatible runtime (containerd or CRI-O) installed on compute nodes
- Runtime socket accessible with appropriate permissions
- Container images available or accessible from registries


## Applying Changes

After modifying the configuration:

1. **Distribute to all nodes**:
   ```bash
   # Using pdsh
   pdcp -w crane[01-04] /etc/crane/config.yaml /etc/crane/
   ```

2. **Restart services**:
   ```bash
   # Control node
   systemctl restart cranectld
   
   # Compute nodes
   pdsh -w crane[01-04] systemctl restart craned
   ```

3. **Verify changes**:
   ```bash
   cinfo  # Check node and partition status
   ```

## Troubleshooting

**Nodes not appearing**: Check ControlMachine hostname matches actual control node hostname.

**Configuration mismatch warnings**: Ensure `/etc/crane/config.yaml` is identical on all nodes.

**Jobs not scheduling**: Verify partition configuration and node membership.

**Resource limits**: Check that requested resources don't exceed node definitions.
