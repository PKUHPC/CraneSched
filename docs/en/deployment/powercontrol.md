# Power Saving Module Deployment
Most cluster nodes are not fully utilized most of the time, and there are often many idle nodes, resulting in waste of energy resources. By collecting task and node energy consumption data over a period of time, estimating node activity patterns, and adaptively powering on/off or sleeping/waking nodes, energy savings can be achieved.  

This guide explains how to enable and configure the power saving module in the Crane cluster.

## Notes
After modifying powerControl.yaml, you need to first power on all sleep/off nodes using IPMI commands. Otherwise, if you start ctld and the powerControl plugin directly, the plugin will not recognize these sleep/off nodes because they haven't registered with ctld, making subsequent scheduling operations impossible. The cplugind on the control node needs to obtain registration information from all craned nodes when starting.

## Environment Preparation
The main application scenarios of the power saving module include:

1. Adaptive power on/off  
2. Adaptive sleep/wake first, then adaptive power on/off  

Different functions depend on different software and hardware conditions, which can be installed and deployed according to actual requirements.

### Hardware Verification
The powerControl plugin uses IPMI to remotely power off and on compute nodes， it uses SSH to connect to compute nodes for sleeping, and Wake-on-LAN to remotely wake compute nodes.

Typically, only server-grade hardware has IPMI, which requires a BMC (Baseboard Management Controller). Therefore, checking whether the machine has a BMC is sufficient. **All ctld and craned nodes need to support this.**

```bash
ipmitool lan print    ## Query BMC network interface information
lsmod | grep ipmi  ## Check if the system supports IPMI related protocols, e.g., ipmi_si
ls /dev/ipmi*    ## Check if IPMI interface devices exist
```

Wake-on-LAN targets waking a specific network card. Check whether the network card supports and has Wake-on-LAN enabled. **All craned nodes need to support this.**
```bash
ethtool eth0 | grep -i wake-on
```

### Installing Dependencies

#### influxDB
Used to collect task and node energy consumption data. Can be deployed on a standalone node or on the ctld node.

##### influxDB安装
```bash
## Add repository
sudo tee /etc/yum.repos.d/influxdb.repo > /dev/null <<EOF
[influxdb]
name = InfluxDB Repository - RHEL
baseurl = https://repos.influxdata.com/rhel/9/\$basearch/stable
enabled = 1
gpgcheck = 1
gpgkey = https://repos.influxdata.com/influxdata-archive_compat.key
EOF

## Install
sudo dnf install influxdb2 -y

## Enable auto-start at boot (optional)
systemctl enable --now influxdb 
```

##### Client Installation
```bash
wget https://dl.influxdata.com/influxdb/releases/influxdb2-client-2.7.5-linux-amd64.tar.gz

tar xvf influxdb2-client-2.7.5-linux-amd64.tar.gz
sudo cp influx /usr/local/bin/
which influx
influx version
```

##### Initialize and Record Configuration Information
Initialize, set and record username, password, organization, bucket and other information.
```bash
## Initialize
influx setup

## Verify installation version
influx version
influx ping
```

Get the token for subsequent configuration in the .yaml file.
```bash
influx auth list
```

#### IPMI
Used for remote power off/on of nodes. Only needs to be deployed on the ctld node.

```bash
sudo dnf install ipmitool -y  ## Install

ipmitool -V  ## Verify
```

#### Power Saving Data Training Model
Deployed on the cranectld node.

Clone the repository: [CraneSched-powersaving-trainning](https://github.com/PKUHPC/CraneSched-powersaving-trainning).  The trainer in the repository includes three components: data acquisition and processing, trainer, and evaluator. Data is read from the database as computing center cluster data, processed by the data processor into training and validation data in the form of labels and features. The training data is fed to the trainer for model training, while the validation data is fed to the evaluator for model performance assessment.   

Install and start the predictor. During deployment, the contents of the model and predictor folders in the repository will be used. The model folder contains the model, and the predictor folder contains scripts for starting the prediction service. These two related paths are directly configured in the PowerControl.yaml mentioned below. The prediction service (predictor) will automatically start when the PowerControl plugin is loaded.  

Starting the predictor requires installing the torch environment and some Python dependency packages. Refer to [README.md](https://github.com/PKUHPC/CraneSched-powersaving-trainning/blob/main/README.md) for details.

## Service Configuration
ctld and craned nodes use different plugins, so their configuration files will differ.

### cranectld Node

#### config.yaml
Edit `/etc/crane/config.yaml` and add the following power saving configuration:
```yaml
Plugin:
  # Toggle the plugin module in CraneSched
  Enabled: true
  # Relative to CraneBaseDir
  PlugindSockPath: "cplugind/cplugind.sock"
  # Debug level of Plugind
  PlugindDebugLevel: "trace"
  # Plugins to be loaded in Plugind
  Plugins:
    - Name: "powerControl"
      Path: "/root/CraneSched-FrontEnd/build/plugin/powerControl.so"
      Config: "/etc/crane/powerControl.yaml"
```

#### powerControl.yaml
Create a new file `/etc/crane/powerControl.yaml` with reference content as follows:

```yaml
PowerControl:
  # Location of the active node count predictor script
  PredictorScript: "/root/CraneSched-FrontEnd/plugin/powerControl/predictor/predictor.py"
  # Whether to allow sleeping
  EnableSleep: false
  # Power off nodes if they have been sleeping beyond this threshold
  SleepTimeThresholdSeconds: 36000
  # Proportion of idle nodes to reserve (relative to total cluster nodes)
  IdleReserveRatio: 0.2
  # Interval for executing power control actions
  CheckIntervalSeconds: 1800
  # Interval for checking node status, mainly used to check if a node has transitioned from a transient state to a stable state
  # e.g., If craned01 is powered off, its status becomes powering_off. The program checks every 30 seconds if the node has completely powered off.
  # If powered off, set craned01's status to powered_off.
  NodeStateCheckIntervalSeconds: 30
  # Log file location for the power control plugin
  PowerControlLogFile: "/root/powerControlTest/log/powerControl.log"
  # File location to record every node state change
  NodeStateChangeFile: "/root/powerControlTest/log/node_state_changes.csv"
  # File location to record cluster state information
  ClusterStateFile: "/root/powerControlTest/log/cluster_state.csv"

Predictor:
  # Debug mode off for production, can be enabled for testing
  Debug: false
  # Deployment address of the active node count predictor
  URL: "http://localhost:5000"
  # Log output location for the predictor
  PredictorLogFile: "/root/powerControlTest/log/predictor.log"
  # Used to specify forecasting active node count for the next 30 minutes (model-specific, no modification needed)
  ForecastMinutes: 30
  # Used to retrieve data from the past 240 minutes (model-specific, no modification needed)
  LookbackMinutes: 240
  # Two model files under the model folder in the model repository. In actual deployment, specify the download location for model storage, can be placed under /etc/crane/model folder
  CheckpointFile: "/root/powerControlTest/model/checkpoint.pth"
  # Location of the data scaler file
  ScalersFile: "/root/powerControlTest/model/dataset_scalers.pkl"

# Used to obtain data collected by the energy collection plugin on the craned side
InfluxDB:
  URL: "http://localhost:8086"
  Token: "T011VN2UYr2PujyWH26oqUhsuk9lei4AQfvae6texcKj6NJyygTVlLXf9916VogAhV4g-9L2ADLRoPpZ_9mMwQ=="
  Org: "pku"
  # Table name in InfluxDB, stores node energy consumption data
  # Bucket is equivalent to a table. This is the corresponding table name in InfluxDB. Org refers to organization, can be configured arbitrarily.
  Bucket: "energy_node"

IPMI:
  # BMC username and password for nodes. Configuration should be the same for all nodes.
  User: "energytest"
  # Maximum number of nodes that can be operated per batch
  Password: "fhA&%sfiFae7q4"
  # Maximum number of nodes that can be operated per batch
  PowerOffMaxNodesPerBatch: 10
  MaxNodesPerBatch: 10
  # Interval between each operation batch
  PowerOffBatchIntervalSeconds: 60
  BatchIntervalSeconds: 60
  # Nodes excluded from power control
  ExcludeNodes:
    - "cninfer00"
  # Mapping between craned nodes and their BMC IP addresses
  NodeBMCMapping:
    cninfer05: "192.168.10.55"
    cninfer06: "192.168.10.56"
    cninfer07: "192.168.10.57"
    cninfer08: "192.168.10.58"
    cninfer09: "192.168.10.59"
# Used for suspending (sleeping) craned nodes. Configuration should be the same for all craned nodes.
SSH:
  User: "root"
  Password: "hyq"
```
**Notes**：  
1. After nodes go offline, if there are insufficient idle nodes, submitted jobs will fail directly and will not enter pending state. Therefore, do not set the IdleReserveRatio value in powerControl.yaml too low. It is recommended to be above 0.2.  
2. PowerControlLogFile and PredictorLogFile are log files used for debugging. NodeStateChangeFile and ClusterStateFile are CSV record files that can be used for data analysis. Adjust actual paths according to operational requirements.  
3. SSH related configurations are used when the sleep function is enabled. ctld connects to craned via SSH to execute sleep commands. If sleep is not needed (i.e., EnableSleep=false) or if passwordless SSH has been configured on the host node, configuration is not required.

### craned Node

#### config.yaml
Edit `/etc/crane/config.yaml` and add the following power saving configuration:
```yaml
Plugin:
  # Toggle the plugin module in CraneSched
  Enabled: true
  # Relative to CraneBaseDir
  PlugindSockPath: "cplugind/cplugind.sock"
  # Debug level of Plugind
  PlugindDebugLevel: "trace"
  # Plugins to be loaded in Plugind
  Plugins:
    - Name: "energy"
      Path: "/root/CraneSched-FrontEnd/build/plugin/energy.so"
      Config: "/etc/crane/energy.yaml"
```

#### energy.yaml
Create a new file `/etc/crane/energy.yaml` with reference content as follows:

```yaml
Monitor:
  # Sampling interval, determined by model requirements. No modification needed, currently fixed at 60s.
  SamplePeriod: 60s
  # Log output file location
  LogPath: "/var/crane/craned/energy.log"
  # If the node has GPUs, configure the GPU type when collecting GPU-related energy consumption data
  GPUType: "nvidia"
  Enabled:
    # Enable energy consumption monitoring and load monitoring for jobs
    Job: true
    # Enable energy consumption monitoring using IPMI
    Ipmi: true
    # Enable energy consumption monitoring and load monitoring for GPUs
    Gpu: true
    # Enable energy consumption monitoring using RAPL (for Intel CPUs). Can also enable only IPMI.
    Rapl: true
    # Enable system load-related monitoring (CPU, memory, etc.)
    System: true

Database:
  Type: "influxdb"
  Influxdb:
    Url: "http://192.168.11.109:8086"
    Token: "T011VN2UYr2PujyWH26oqUhsuk9lei4AQfvae6texcKj6NJyygTVlLXf9916VogAhV4g-9L2ADLRoPpZ_9mMwQ=="
    Org: "pku"
    # Table name in InfluxDB, stores node energy consumption data
    NodeBucket: "energy_node"
    # Table name in InfluxDB, stores job energy consumption data
    JobBucket: "energy_task"
```

## Service Startup

### cranectld Node
```bash
## Start ctld service
systemctl start cranectld
```

#### cranectld Related Plugin Services
For detailed information, refer to **[Plugin Guide](./frontend/plugins.md)**  
If the predictor fails to start when cplugind starts, you need to add environment variables in cplugind.service, for example:
```service
Environment=PYTHONPATH=/root/repo/CraneSched-powersaving-trainning:$PYTHONPATH
```

```bash
## Start plugin service
systemctl enable cplugind
```

### craned Node
```bash
## Start craned service
systemctl start craned
## Auto-start must be enabled, otherwise compute nodes cannot automatically start and rejoin the network after the host node reboots
systemctl enable craned
```
#### craned Related Plugin Services
For detailed information, refer to **[Plugin Guide](./frontend/plugins.md)**

```bash
## Start plugin service
systemctl start cplugind
## Auto-start must be enabled, otherwise compute node plugins cannot automatically start after the host node reboots
systemctl enable cplugind
```

#### Verify Energy Data Write
On the craned node, verify read and write access to InfluxDB:
```bash
## Attempt to write data 123 to the energy_node table in InfluxDB
curl -i -XPOST "http://192.168.234.1:8086/api/v2/write?org=pku&bucket=energy_node&precision=s" \
  -H "Authorization: Token UImiPmWA_TSQCt2j8YbE7c8G6K-o1W_xk8nbN28TvahQuOG7ol8Q6DFiO4Y8mDhPdoFGIrl2xB3Xr8oaivEnpQ==" \
  --data-binary "test_node,node=cn01 value=123 $(date +%s)"
  
## Query data 123 from the energy_node table in InfluxDB
curl -s -XPOST "http://192.168.234.1:8086/api/v2/query?org=pku" \
  -H "Authorization: Token UImiPmWA_TSQCt2j8YbE7c8G6K-o1W_xk8nbN28TvahQuOG7ol8Q6DFiO4Y8mDhPdoFGIrl2xB3Xr8oaivEnpQ==" \
  -H "Content-type: application/vnd.flux" \
  -d 'from(bucket:"energy_node")
      |> range(start: -5m)
      |> filter(fn: (r) => r._measurement == "test_node")'

```

```bash
## Attempt to write data 456 to the energy_task table in InfluxDB
curl -i -XPOST "http://192.168.234.1:8086/api/v2/write?org=pku&bucket=energy_task&precision=s" \
  -H "Authorization: Token UImiPmWA_TSQCt2j8YbE7c8G6K-o1W_xk8nbN28TvahQuOG7ol8Q6DFiO4Y8mDhPdoFGIrl2xB3Xr8oaivEnpQ==" \
  --data-binary "test_task,job=testjob value=456 $(date +%s)"

## Query data 456 from the energy_task table in InfluxDB
curl -s -XPOST "http://192.168.234.1:8086/api/v2/query?org=pku" \
  -H "Authorization: Token UImiPmWA_TSQCt2j8YbE7c8G6K-o1W_xk8nbN28TvahQuOG7ol8Q6DFiO4Y8mDhPdoFGIrl2xB3Xr8oaivEnpQ==" \
  -H "Content-type: application/vnd.flux" \
  -d 'from(bucket:"energy_task")
      |> range(start: -5m)
      |> filter(fn: (r) => r._measurement == "test_task")'
```

## Usage Instructions

### Common Crane Commands
Node status supports adaptive adjustment, and can also be manually controlled by administrators via command line.

```bash
ccontrol update nodeName=cninfer09 state=off   ## Power off node
ccontrol update nodeName=cninfer05,cninfer06 state=off   ## Power off multiple nodes

ccontrol update nodeName cninfer09 state on reason "power on"   ## Power on node
ccontrol update nodeName cninfer05,cninfer06 state on reason "power on"   ## Power on multiple nodes

ccontrol update nodeName=craned1 state sleep  ## Sleep node
ccontrol update nodeName=craned1,craned2 state sleep  ## Sleep multiple nodes

ccontrol update nodeName=craned1 state wake  ## Wake node
ccontrol update nodeName=craned1,craned2 state wake  ## Wake multiple nodes
```
**Note**：When executing on/off commands, wait 2-3 minutes for them to take effect.  

You can query node status with the following commands:
```bash
cinfo
ccontrol show node
```
For detailed information, refer to [cinfo](../command/cinfo.md)

### Common IPMI Commands
When nodes are abnormal and cannot be operated through Crane, you can directly power on or off nodes using IPMI commands.
```bash
ipmitool -I lanplus -H 10.129.227.189 -U ADMIN -P 'Admin@123' power on ## Directly power on via IPMI command

ipmitool -I lanplus -H 10.129.227.189 -U ADMIN -P 'Admin@123' power reset  ## Directly restart node via IPMI command
```
Here, the IP, user, and password correspond to the node's BMC IP, username, and password. These must be configured by operations, with all cluster machines maintaining the same username and password, consistent with the username and password in the IPMI section of powerControl.yaml.  

Since each node's BMC IP address (e.g., 10.129.227.189) is different, you need to manually configure the mapping between each node's hostname and BMC IP in powerControl.yaml. This allows the corresponding craned node to be operated when actively powering on/off.
