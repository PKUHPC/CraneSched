# Power Saving Module Deployment

Cluster nodes are often underutilized, with many sitting idle and consuming energy unnecessarily. By collecting task and node energy consumption data over time, predicting node activity patterns, and adaptively powering on/off or sleeping/waking nodes, significant energy savings can be achieved.

This guide explains how to enable and configure the power saving module in a CraneSched cluster.

!!! warning "Important"

    After modifying `powerControl.yaml`, you must first power on all nodes in sleep/off state via IPMI commands before starting CraneCtld and the powerControl plugin. Otherwise, those nodes will not have registered with CraneCtld, and the powerControl plugin will be unable to recognize or schedule them. The cplugind process on the control node must obtain registration information from all Craned nodes at startup.

## Environment Preparation

The main application scenarios of the power saving module include:

1. Adaptive power on/off
2. Adaptive sleep/wake followed by adaptive power on/off

Different features depend on different software and hardware prerequisites. Install and deploy according to your actual requirements.

### Hardware Requirements

The powerControl plugin uses IPMI to remotely power off and power on compute nodes, SSH to connect to compute nodes for sleep operations, and Wake-on-LAN to remotely wake compute nodes.

IPMI is typically available only on server-grade hardware equipped with a BMC (Baseboard Management Controller). Use the following commands to verify BMC availability. **All CraneCtld and Craned nodes must support IPMI.**

```bash
ipmitool lan print       # Query BMC network interface information
lsmod | grep ipmi        # Check if IPMI kernel modules are loaded (e.g., ipmi_si)
ls /dev/ipmi*             # Check if IPMI interface devices exist
```

Wake-on-LAN is used to wake a node via its network card. Verify that the network card supports and has Wake-on-LAN enabled. **All Craned nodes must support this.**

```bash
ethtool eth0 | grep -i wake-on
```

### Installing Dependencies

#### InfluxDB

Used to collect task and node energy consumption data. Can be deployed on a standalone node or on the CraneCtld node.

##### Install InfluxDB Server

```bash
# Add repository
sudo tee /etc/yum.repos.d/influxdb.repo > /dev/null <<EOF
[influxdb]
name = InfluxDB Repository - RHEL
baseurl = https://repos.influxdata.com/rhel/9/\$basearch/stable
enabled = 1
gpgcheck = 1
gpgkey = https://repos.influxdata.com/influxdata-archive_compat.key
EOF

# Install
sudo dnf install influxdb2 -y

# Enable auto-start at boot (optional)
systemctl enable --now influxdb
```

##### Install Client

```bash
wget https://dl.influxdata.com/influxdb/releases/influxdb2-client-2.7.5-linux-amd64.tar.gz

tar xvf influxdb2-client-2.7.5-linux-amd64.tar.gz
sudo cp influx /usr/local/bin/
which influx
influx version
```

##### Initialize and Record Configuration

During initialization, set and record the username, password, **organization (Org)**, and **bucket name (Bucket)**.

```bash
# Initialize
influx setup

# Verify installation
influx version
influx ping
```

Retrieve the **Token** for use in subsequent `.yaml` configuration:

```bash
influx auth list
```

#### IPMI

Used for remote power on/off of nodes. Only needs to be deployed on the CraneCtld node.

```bash
sudo dnf install ipmitool -y   # Install
ipmitool -V                     # Verify
```

#### Power Saving Data Model

The power saving module includes a machine learning model for predicting node activity trends over upcoming time periods. Model inference depends on a Python environment and related libraries, and must be deployed on the CraneCtld node.

This part is not yet open source. Please contact the maintenance team.

## Service Configuration

CraneCtld and Craned nodes use different plugins, so their configuration files differ.

### CraneCtld Node

#### plugin.yaml

Edit `/etc/crane/plugin.yaml` and add the following power saving configuration:

```yaml
Plugin:
  Enabled: true
  PlugindSockPath: "cplugind/cplugind.sock"
  PlugindDebugLevel: "trace"
  Plugins:
    - Name: "powerControl"
      Path: "/root/CraneSched-FrontEnd/build/plugin/powerControl.so"
      Config: "/etc/crane/powerControl.yaml"
```

#### powerControl.yaml

Create `/etc/crane/powerControl.yaml` with the following reference content:

??? example "Full powerControl.yaml example"

    ```yaml
    PowerControl:
      # Path to the active node count predictor script
      PredictorScript: "/root/CraneSched-FrontEnd/plugin/powerControl/predictor/predictor.py"
      # Whether to allow sleep mode
      EnableSleep: false
      # Power off nodes after sleeping beyond this threshold (seconds)
      SleepTimeThresholdSeconds: 36000
      # Proportion of idle nodes to reserve (relative to total cluster nodes)
      IdleReserveRatio: 0.2
      # Interval between power control actions (seconds)
      CheckIntervalSeconds: 1800
      # Interval for checking node state transitions (seconds)
      # e.g., after craned01 is powered off, its state becomes powering_off.
      # Every 30 seconds the plugin checks whether the node has fully powered off,
      # then updates the state to powered_off.
      NodeStateCheckIntervalSeconds: 30
      # Power control plugin log file path
      PowerControlLogFile: "/root/powerControlTest/log/powerControl.log"
      # Node state change record file path
      NodeStateChangeFile: "/root/powerControlTest/log/node_state_changes.csv"
      # Cluster state record file path
      ClusterStateFile: "/root/powerControlTest/log/cluster_state.csv"

    Predictor:
      # Disable in production; enable for testing
      Debug: false
      # Active node count predictor deployment URL
      URL: "http://localhost:5000"
      # Predictor log file path
      PredictorLogFile: "/root/powerControlTest/log/predictor.log"
      # Forecast active node count for the next N minutes (model-specific, do not modify)
      ForecastMinutes: 30
      # Retrieve data from the past N minutes (model-specific, do not modify)
      LookbackMinutes: 240
      # Model checkpoint file path (can be placed under /etc/crane/model/)
      CheckpointFile: "/root/powerControlTest/model/checkpoint.pth"
      # Data scaler file path
      ScalersFile: "/root/powerControlTest/model/dataset_scalers.pkl"

    InfluxDB:
      URL: "http://localhost:8086"
      Token: "<TOKEN>"
      Org: "<ORG>"
      # InfluxDB bucket name for node energy consumption data
      Bucket: "<BUCKET>"

    IPMI:
      # BMC username and password (should be the same for all nodes)
      User: "energytest"
      Password: "fhA&%sfiFae7q4"
      # Maximum number of nodes per operation batch
      PowerOffMaxNodesPerBatch: 10
      MaxNodesPerBatch: 10
      # Interval between operation batches (seconds)
      PowerOffBatchIntervalSeconds: 60
      BatchIntervalSeconds: 60
      # Nodes excluded from power control
      ExcludeNodes:
        - "cninfer00"
      # Mapping between Craned node hostnames and their BMC IP addresses
      NodeBMCMapping:
        cninfer05: "192.168.10.55"
        cninfer06: "192.168.10.56"
        cninfer07: "192.168.10.57"
        cninfer08: "192.168.10.58"
        cninfer09: "192.168.10.59"

    # Used for suspending (sleeping) Craned nodes (should be the same for all Craned nodes)
    SSH:
      User: "root"
      Password: "hyq"
    ```

!!! note "Configuration Notes"

    1. After nodes go offline, if there are insufficient idle nodes, submitted jobs will fail directly (Failed) instead of entering a pending state. Therefore, do not set `IdleReserveRatio` too low — a value of at least `0.2` is recommended.
    2. `PowerControlLogFile` and `PredictorLogFile` are debug logs. `NodeStateChangeFile` and `ClusterStateFile` are CSV record files for data analysis. Adjust paths according to your operational requirements.
    3. `SSH` configuration is only needed when sleep mode is enabled (CraneCtld connects to Craned via SSH to execute sleep commands). When `EnableSleep: false` or passwordless SSH is already configured, this section can be omitted.

### Craned Node

#### plugin.yaml

Edit `/etc/crane/plugin.yaml` and add the following power saving configuration:

```yaml
Plugin:
  Enabled: true
  PlugindSockPath: "cplugind/cplugind.sock"
  PlugindDebugLevel: "trace"
  Plugins:
    - Name: "energy"
      Path: "/root/CraneSched-FrontEnd/build/plugin/energy.so"
      Config: "/etc/crane/energy.yaml"
```

#### energy.yaml

Create `/etc/crane/energy.yaml` with the following reference content:

??? example "Full energy.yaml example"

    ```yaml
    Monitor:
      # Sampling interval (model-specific, currently fixed at 60 seconds, do not modify)
      SamplePeriod: 60s
      # Log output file path
      LogPath: "/var/crane/craned/energy.log"
      # GPU type (required for collecting GPU energy consumption data)
      GPUType: "nvidia"
      Enabled:
        # Enable job energy consumption and load monitoring
        Job: true
        # Enable IPMI-based energy consumption monitoring
        Ipmi: true
        # Enable GPU energy consumption and load monitoring
        Gpu: true
        # Enable RAPL-based energy consumption monitoring (for Intel CPUs; IPMI-only is also supported)
        Rapl: true
        # Enable system load monitoring (CPU, memory, etc.)
        System: true

    Database:
      Type: "influxdb"
      Influxdb:
        Url: "http://<INFLUXDB_HOST>:8086"
        Token: "<TOKEN>"
        Org: "<ORG>"
        # InfluxDB bucket name for node energy consumption data
        NodeBucket: "<NODE_BUCKET>"
        # InfluxDB bucket name for job energy consumption data
        JobBucket: "<JOB_BUCKET>"
    ```

## Service Startup

### CraneCtld Node

```bash
systemctl enable --now cranectld
```

#### CraneCtld Plugin Services

For detailed instructions, refer to the [Plugin Guide](./frontend/plugins.md).

!!! tip "Environment Variable Configuration"

    If the predictor fails to start when cplugind launches, add the required environment variable in `cplugind.service`:

    ```ini
    Environment=PYTHONPATH=/root/repo/CraneSched-powersaving-trainning:$PYTHONPATH
    ```

```bash
systemctl enable --now cplugind
```

### Craned Node

!!! warning "Auto-Start Requirement"

    Both `craned` and `cplugind` services on Craned nodes must be enabled for auto-start (`enable`). Otherwise, compute nodes cannot automatically rejoin the cluster or restore plugin functionality after a host reboot.

```bash
systemctl enable --now craned
```

#### Craned Plugin Services

For detailed instructions, refer to the [Plugin Guide](./frontend/plugins.md).

```bash
systemctl enable --now cplugind
```

#### Verify Energy Data Write

On the Craned node, verify InfluxDB read/write access:

??? example "Write and query energy_node data"

    ```bash
    # Write test data to the energy_node bucket in InfluxDB
    curl -i -XPOST "http://<INFLUXDB_HOST>:8086/api/v2/write?org=<ORG>&bucket=energy_node&precision=s" \
      -H "Authorization: Token <TOKEN>" \
      --data-binary "test_node,node=cn01 value=123 $(date +%s)"

    # Query test data from the energy_node bucket
    curl -s -XPOST "http://<INFLUXDB_HOST>:8086/api/v2/query?org=<ORG>" \
      -H "Authorization: Token <TOKEN>" \
      -H "Content-type: application/vnd.flux" \
      -d 'from(bucket:"energy_node")
          |> range(start: -5m)
          |> filter(fn: (r) => r._measurement == "test_node")'
    ```

??? example "Write and query energy_task data"

    ```bash
    # Write test data to the energy_task bucket in InfluxDB
    curl -i -XPOST "http://<INFLUXDB_HOST>:8086/api/v2/write?org=<ORG>&bucket=energy_task&precision=s" \
      -H "Authorization: Token <TOKEN>" \
      --data-binary "test_task,job=testjob value=456 $(date +%s)"

    # Query test data from the energy_task bucket
    curl -s -XPOST "http://<INFLUXDB_HOST>:8086/api/v2/query?org=<ORG>" \
      -H "Authorization: Token <TOKEN>" \
      -H "Content-type: application/vnd.flux" \
      -d 'from(bucket:"energy_task")
          |> range(start: -5m)
          |> filter(fn: (r) => r._measurement == "test_task")'
    ```

## Usage Instructions

### Common CraneSched Commands

Node states support adaptive adjustment and can also be manually controlled by administrators via the command line.

```bash
# Power off
ccontrol update nodeName=cninfer09 state=off
ccontrol update nodeName=cninfer05,cninfer06 state=off

# Power on
ccontrol update nodeName cninfer09 state on reason "power on"
ccontrol update nodeName cninfer05,cninfer06 state on reason "power on"

# Sleep
ccontrol update nodeName=craned1 state sleep
ccontrol update nodeName=craned1,craned2 state sleep

# Wake
ccontrol update nodeName=craned1 state wake
ccontrol update nodeName=craned1,craned2 state wake
```

!!! note

    Power on/off operations may take 2–3 minutes to take effect.

Query node status with the following commands:

```bash
cinfo
ccontrol show node
```

For detailed usage, refer to [cinfo](../command/cinfo.md).

### Common IPMI Commands

When nodes are in an abnormal state and cannot be operated through CraneSched commands, you can directly power on or restart them using IPMI:

```bash
# Power on via IPMI
ipmitool -I lanplus -H 10.129.227.189 -U ADMIN -P 'Admin@123' power on

# Restart via IPMI
ipmitool -I lanplus -H 10.129.227.189 -U ADMIN -P 'Admin@123' power reset
```

!!! info "BMC Configuration"

    The IP, username, and password in the commands above correspond to each node's BMC connection details. All cluster nodes should share the same BMC credentials, which must match the `IPMI` section in `powerControl.yaml`.

    Since each node has a unique BMC IP address, you must manually configure the hostname-to-BMC-IP mapping (`NodeBMCMapping`) in `powerControl.yaml` so that power on/off operations reach the correct Craned node.
