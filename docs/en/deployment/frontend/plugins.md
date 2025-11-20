# Plugin Guide

## Overview

The CraneSched plugin system is modular and disabled by default. The `cplugind` daemon must be running on each node to enable plugin functionality. Plugins and `cplugind` versions are strictly coupled and must be updated together.

!!! info
    Plugins are optional. If you do not require any plugin features, you can skip this guide.

### Plugin Architecture

Each CraneSched plugin consists of:

- **Shared library** (`.so` file): Plugin implementation
- **Plugin configuration** (`.yaml` file, optional): Plugin-specific settings

### Configuration Files

Understanding the distinction between three types of configuration files is crucial:

**Global Configuration** (`/etc/crane/config.yaml`)
: CraneSched's main configuration file containing settings for CLIs, backend, and cfored.

**Plugin Global Configuration** (`/etc/crane/plugin.yaml`)
: Plugin system configuration containing cplugind settings and the list of plugins to load. Plugin paths are registered here.

**Individual Plugin Configuration** (e.g., `monitor.yaml`)
: Individual plugin settings. Can be located anywhere readable, with the absolute path specified in the plugin global configuration.

### Available Plugins

CraneSched currently provides the following plugins:

| Plugin | Description |
|--------|-------------|
| **Mail** | Send email notifications on job state changes |
| **Monitor** | Collect job resource usage metrics to InfluxDB (Grafana integration supported) |
| **Energy** | Monitor power consumption for nodes and jobs to InfluxDB |
| **Event** | Record node state changes to InfluxDB |

## Installing cplugind

The `cplugind` daemon is part of the CraneSched-FrontEnd repository. You can install it via packages or build from source.

### Via Package Manager (Recommended)

Install the `cranesched-plugin` package which includes `cplugind` and all plugin shared objects:

**For RPM-based systems** (Rocky Linux, CentOS, Fedora, AlmaLinux):
```bash
sudo dnf install cranesched-plugin-*.rpm
```

**For DEB-based systems** (Debian, Ubuntu):
```bash
sudo apt install ./cranesched-plugin_*.deb
```

This installs:
- `cplugind` daemon to `/usr/bin/`
- Plugin shared objects to `/usr/lib/crane/plugin/`:
  - `dummy.so`, `energy.so`, `event.so`, `mail.so`, `monitor.so`, `powerControl.so`
- `cplugind.service` systemd unit

Enable and start the service:
```bash
systemctl enable --now cplugind
```

### From Source

Refer to the [Frontend Deployment Guide](frontend.md) for instructions on building from source. After building, the plugin files will be located in the `build/plugin/` directory.

Start `cplugind` manually or via systemd:

```bash
systemctl enable cplugind
systemctl start cplugind
```

## Enabling Plugins

Edit the plugin configuration file `/etc/crane/plugin.yaml`:

```yaml
# Toggle the plugin module in CraneSched
Enabled: true

# Socket path relative to CraneBaseDir
PlugindSockPath: "cplugind/cplugind.sock"

# Debug level: trace, debug, or info (use info in production)
PlugindDebugLevel: "info"

# Network listening settings (optional)
PlugindListenAddress: "127.0.0.1"
PlugindListenPort: "10018"

# Plugins to load
Plugins:
  - Name: "monitor"
    Path: "/usr/lib/crane/plugin/monitor.so"  # Package installation
    # Path: "/usr/local/lib/crane/plugin/monitor.so"  # Source installation (default PREFIX)
    Config: "/etc/crane/monitor.yaml"
```

### Configuration Options

- **Enabled**: Controls whether CraneCtld/Craned uses the plugin system
- **PlugindSockPath**: Socket path relative to CraneBaseDir for communication between daemons and cplugind
- **PlugindDebugLevel**: Log level (trace/debug/info; recommended: info for production)
- **PlugindListenAddress**: Network address for cplugind to listen (optional)
- **PlugindListenPort**: Network port for cplugind to listen (optional)
- **Plugins**: List of plugins to load on this node
  - **Name**: Plugin identifier (any string)
  - **Path**: Absolute path to the `.so` file
    - Package installation: `/usr/lib/crane/plugin/<plugin>.so`
    - Source installation: `/usr/local/lib/crane/plugin/<plugin>.so` (or custom PREFIX)
  - **Config**: Absolute path to the plugin configuration file

## Monitor Plugin

The monitor plugin collects job-level resource usage metrics and requires installation on compute nodes.

### Prerequisites

Install InfluxDB (not required on compute nodes, but must be network-accessible):

```bash
docker run -d -p 8086:8086 --name influxdb2 influxdb:2
```

Access the web UI at `http://<Host IP>:8086` and complete the setup wizard. Record the following information:

- **Username**: `your_username`
- **Bucket**: `your_bucket_name`
- **Org**: `your_organization`
- **Token**: `your_token`
- **Measurement**: `ResourceUsage`

### Configuration

1. **Create plugin configuration file** (e.g., `/etc/crane/monitor.yaml`):

```yaml
# Cgroup path pattern (%j is replaced with job's cgroup path)
Cgroup:
  CPU: "/sys/fs/cgroup/cpuacct/%j/cpuacct.usage"
  Memory: "/sys/fs/cgroup/memory/%j/memory.usage_in_bytes"
  ProcList: "/sys/fs/cgroup/memory/%j/cgroup.procs"

# InfluxDB Configuration
Database:
  Username: "your_username"
  Bucket: "your_bucket_name"
  Org: "your_organization"
  Token: "your_token"
  Measurement: "ResourceUsage"
  Url: "http://localhost:8086"

# Sampling interval in milliseconds
Interval: 1000
# Buffer size for batch writes
BufferSize: 32
```

**Note**: For cgroup v2, update paths accordingly.

2. **Register plugin in plugin configuration** (`/etc/crane/plugin.yaml`):

```yaml
Enabled: true
Plugins:
  - Name: "monitor"
    Path: "/usr/lib/crane/plugin/monitor.so"  # Package installation
    # Path: "/usr/local/lib/crane/plugin/monitor.so"  # Source installation (default)
    # Path: "/path/to/build/plugin/monitor.so"  # Development/build directory
    Config: "/etc/crane/monitor.yaml"
```

!!! note "Plugin Path"
    - **Package installation**: Use `/usr/lib/crane/plugin/monitor.so`
    - **Source installation**: Use `/usr/local/lib/crane/plugin/monitor.so` (or custom PREFIX location)
    - **Development**: Use `build/plugin/monitor.so` from the build directory

## Mail Plugin

The mail plugin sends job notifications via email from the CraneSched control node (cranectld).

### System Mail Configuration

1. **Install mail dependencies**:

```bash
dnf install s-nail postfix ca-certificates
```

**Note**: The `mail` command may have different names across distributions. Postfix handles SMTP requests; sendmail can be used as an alternative.

2. **Configure SMTP settings** by creating `/etc/s-nail.rc`:

```bash
set from="your_email@example.com"
set smtp="smtp.example.com"
set smtp-auth-user="your_email@example.com"
set smtp-auth-password="your_app_password"
set smtp-auth=login
```

**Important**: Use an application-specific password, not your account password. Refer to your email provider's documentation.

3. **Test mail functionality**:

```bash
echo "Test Mail Body" | mail -s "Test Mail Subject" recipient@example.com
```

### Plugin Configuration

1. **Create mail plugin configuration** (e.g., `/etc/crane/mail.yaml`):

```yaml
# Sender address for mail notifications
SenderAddr: example@example.com

# Send subject-only emails (no body)
SubjectOnly: false
```

2. **Register plugin** in `/etc/crane/plugin.yaml` (similar to monitor plugin).

### Using Email Notifications

#### In Job Scripts

```bash
#!/bin/bash
#CBATCH --nodes 1
#CBATCH --ntasks-per-node 1
#CBATCH --mem 1G
#CBATCH -J EmailTest
#CBATCH --mail-type ALL
#CBATCH --mail-user user@example.com

hostname
echo "This job triggers email notifications"
sleep 60
```

Submit the job:

```bash
cbatch emailjob.sh
```

#### Command Line

```bash
cbatch --mail-type=ALL --mail-user=user@example.com test.job
```

### Mail Types

| Type | Description |
|------|-------------|
| `BEGIN` | Job starts running (Pending → Running) |
| `FAILED` | Job execution fails |
| `TIMELIMIT` | Job exceeds time limit |
| `END` | Job finishes (Running → Completed/Failed/Cancelled) |
| `ALL` | All of the above events |

### Parameter Priority

- `--mail-type` and `--mail-user` automatically update the internal `--extra-attr` parameter
- In job scripts: Later options override earlier ones
- On command line: `--mail-type/user` always take precedence over `--extra-attr`
- **Recommendation**: Use `--mail-type/user` unless you have specific requirements for `--extra-attr`

## Troubleshooting

- Verify `Enabled: true` in `/etc/crane/plugin.yaml`
- Check that `cplugind` is running: `systemctl status cplugind`
- Ensure `.so` file paths are absolute and readable
- Check `cplugind` logs for errors
- If `/etc/crane/plugin.yaml` doesn't exist, the plugin system will be automatically disabled
