# Multi-node Deployment

This guide covers deploying CraneSched binaries and configurations across multiple nodes in a cluster.

## Prerequisites

- CraneSched is built and tested on one node (typically the control node)
- All nodes have network connectivity and can resolve hostnames
- You have root or sudo access to all nodes

## Deployment Methods

### Method 1: Using SCP

Suitable for small clusters or when deploying to specific nodes individually.

#### Deploy to Control Node

```bash
# Copy control node binary
ssh cranectl "mkdir -p /etc/crane"
scp CraneSched-1.1.2-Linux-x86_64-cranectld.rpm cranectl:/tmp
ssh cranectl "rpm -ivh /tmp/CraneSched-1.1.2-Linux-x86_64-cranectld.rpm"
scp /etc/crane/config.yaml cranectl:/etc/crane/
scp /etc/crane/database.yaml cranectl:/etc/crane/
```

#### Deploy to Compute Nodes

```bash
# Copy compute node binary
ssh crane02 "mkdir -p /etc/crane"
scp CraneSched-1.1.2-Linux-x86_64-craned.rpm crane02:/tmp
ssh crane02 "rpm -ivh /tmp/CraneSched-1.1.2-Linux-x86_64-craned.rpm"
scp /etc/crane/config.yaml crane02:/etc/crane/
```

Repeat for each compute node (crane03, crane04, etc.).

### Method 2: Using PDSH

Recommended for larger clusters. PDSH allows parallel execution across multiple nodes.

#### Install PDSH

**Rocky 9:**
```bash
dnf install -y pdsh
```

**CentOS 7:**
```bash
yum install -y pdsh
```

#### Deploy to Control Nodes

```bash
# Copy and install cranectld
pdcp -w cranectl CraneSched-1.1.2-Linux-x86_64-cranectld.rpm /tmp
pdsh -w cranectl "rpm -ivh /tmp/CraneSched-1.1.2-Linux-x86_64-cranectld.rpm"

# Copy configuration files
pdcp -w cranectl /etc/crane/config.yaml /etc/crane/
pdcp -w cranectl /etc/crane/database.yaml /etc/crane/

# Start service
pdsh -w cranectl "systemctl daemon-reload"
pdsh -w cranectl "systemctl enable cranectld"
pdsh -w cranectl "systemctl start cranectld"
```

#### Deploy to Compute Nodes

```bash
# Copy and install craned
pdcp -w crane[01-04] CraneSched-1.1.2-Linux-x86_64-craned.rpm /tmp
pdsh -w crane[01-04] "rpm -ivh /tmp/CraneSched-1.1.2-Linux-x86_64-craned.rpm"

# Copy configuration file
pdcp -w crane[01-04] /etc/crane/config.yaml /etc/crane/

# Start service
pdsh -w crane[01-04] "systemctl daemon-reload"
pdsh -w crane[01-04] "systemctl enable craned"
pdsh -w crane[01-04] "systemctl start craned"
```

## Alternative: Manual Binary Copy

If you built from source instead of RPM packages:

```bash
# Control node
scp /usr/local/bin/cranectld cranectl:/usr/local/bin/
scp /etc/systemd/system/cranectld.service cranectl:/etc/systemd/system/

# Compute nodes
pdcp -w crane[01-04] /usr/local/bin/craned /usr/local/bin/
pdcp -w crane[01-04] /usr/libexec/csupervisor /usr/libexec/
pdcp -w crane[01-04] /etc/systemd/system/craned.service /etc/systemd/system/
```

## Verify Deployment

After deployment, verify that all nodes are online:

```bash
cinfo
```

You should see all compute nodes listed with state `IDLE` or `ALLOC`.

## Updating Deployed Nodes

To update an existing deployment:

```bash
# Stop services
pdsh -w cranectl "systemctl stop cranectld"
pdsh -w crane[01-04] "systemctl stop craned"

# Deploy new version
pdcp -w cranectl CraneSched-new-version-cranectld.rpm /tmp
pdsh -w cranectl "rpm -Uvh /tmp/CraneSched-new-version-cranectld.rpm"

pdcp -w crane[01-04] CraneSched-new-version-craned.rpm /tmp
pdsh -w crane[01-04] "rpm -Uvh /tmp/CraneSched-new-version-craned.rpm"

# Start services
pdsh -w cranectl "systemctl start cranectld"
pdsh -w crane[01-04] "systemctl start craned"
```

## Gres Configuration

> Device resource related configuration

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

  - | Vendor | Device File Path | EnvInjector |
    | :---------- | :---------------------- | :---------- |
    | Nvidia | /dev/nvidia0 ... | nvidia |
    | AMD/Hygon DCU | /dev/dri/renderer128... | hip |
    | Ascend | /dev/davinci0 ... | ascend |
    | Iluvatar | /dev/iluvatar0 ... | nvidia |

  - 
  

## Troubleshooting

**PDSH not found**: Install the `pdsh` package from EPEL repository.

**Permission denied**: Ensure SSH key authentication is set up for root or your deployment user.

**Nodes not appearing in `cinfo`**: Check firewall settings and ensure inter-node communication is allowed on required ports (10010-10013).

**Service fails to start**: Check logs with `journalctl -u cranectld` or `journalctl -u craned`.
