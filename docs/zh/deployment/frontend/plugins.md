# 插件指南

## 概述

鹤思插件系统是模块化的，默认情况下处于禁用状态。必须在每个节点上运行 `cplugind` daemon 才能启用插件功能。插件和 `cplugind` 版本严格耦合，必须一起更新。

!!! info
    插件是可选的。如果您不需要任何插件功能,可以跳过本指南。

### 插件架构

每个鹤思插件包括:

- **共享库**（`.so` 文件）：插件实现
- **插件配置**（`.yaml` 文件，可选）：特定于插件的设置

### 配置文件

理解三种类型的配置文件之间的区别至关重要：

**全局配置** (`/etc/crane/config.yaml`)
: 鹤思的主配置文件，包含 CLI、后端和 cfored 的设置。

**插件全局配置** (`/etc/crane/plugin.yaml`)
: 插件系统配置，包含 cplugind 设置和要加载的插件列表。插件路径在此处注册。

**单个插件配置**（例如 `monitor.yaml`）
: 单个插件设置。可以位于任何可读位置，在插件全局配置中指定绝对路径。

### 可用插件

鹤思目前提供以下插件:

| 插件 | 描述 |
|------|------|
| **Mail** | 在作业状态更改时发送电子邮件通知 |
| **Monitor** | 将作业资源使用指标收集到 InfluxDB（支持 Grafana 集成）|
| **Energy** | 监控节点和作业的功耗到 InfluxDB |
| **Event** | 将节点状态更改记录到 InfluxDB |

## 安装 cplugind

`cplugind` 守护进程是 CraneSched-FrontEnd 仓库的一部分。您可以通过软件包或从源代码构建安装它。

### 通过软件包管理器（推荐）

安装包含 `cplugind` 和所有插件共享对象的 `cranesched-plugin` 软件包：

**对于基于 RPM 的系统**（Rocky Linux、CentOS、Fedora、AlmaLinux）：
```bash
sudo dnf install cranesched-plugin-*.rpm
```

**对于基于 DEB 的系统**（Debian、Ubuntu）：
```bash
sudo apt install ./cranesched-plugin_*.deb
```

这将安装：
- `cplugind` 守护进程到 `/usr/bin/`
- 插件共享对象到 `/usr/lib/crane/plugin/`：
  - `dummy.so`、`mail.so`、`monitor.so`、`powerControl.so`
- `cplugind.service` systemd 单元

启用并启动服务：
```bash
systemctl enable --now cplugind
```

### 从源代码构建

有关从源代码构建的说明，请参阅[前端部署指南](frontend.md)。构建后，插件文件将位于 `build/plugin/` 目录中。

手动启动 `cplugind` 或通过 systemd：

```bash
systemctl enable cplugind
systemctl start cplugind
```

## 启用插件

编辑插件配置文件 `/etc/crane/plugin.yaml`：

```yaml
# 在鹤思中切换插件模块
Enabled: true

# 相对于 CraneBaseDir 的套接字路径
PlugindSockPath: "cplugind/cplugind.sock"

# 调试级别：trace、debug 或 info（生产环境使用 info）
PlugindDebugLevel: "info"

# 网络监听设置（可选）
PlugindListenAddress: "127.0.0.1"
PlugindListenPort: "10018"

# 要加载的插件
Plugins:
  - Name: "monitor"
    Path: "/usr/lib/crane/plugin/monitor.so"  # 软件包安装
    # Path: "/usr/local/lib/crane/plugin/monitor.so"  # 源代码安装（默认 PREFIX）
    Config: "/etc/crane/monitor.yaml"
```

### 配置选项

- **Enabled**：控制 CraneCtld/Craned 是否使用插件系统
- **PlugindSockPath**：相对于 CraneBaseDir 的套接字路径，用于 daemon 与 cplugind 之间的通信
- **PlugindDebugLevel**：日志级别（trace/debug/info；推荐：生产环境使用 info）
- **PlugindListenAddress**：cplugind 监听的网络地址（可选）
- **PlugindListenPort**：cplugind 监听的网络端口（可选）
- **Plugins**：此节点上要加载的插件列表
  - **Name**：插件标识符（任何字符串）
  - **Path**：`.so` 文件的绝对路径
    - 软件包安装：`/usr/lib/crane/plugin/<plugin>.so`
    - 源代码安装：`/usr/local/lib/crane/plugin/<plugin>.so`（或自定义 PREFIX）
  - **Config**：插件配置文件的绝对路径

## Monitor 插件

monitor 插件收集作业级别的资源使用指标，需要安装在计算节点上。

### 先决条件

安装 InfluxDB（计算节点上不需要，但必须可通过网络访问）：

```bash
docker run -d -p 8086:8086 --name influxdb2 influxdb:2
```

在 `http://<主机 IP>:8086` 访问 Web UI 并完成设置向导。记录以下信息：

- **Username**：`your_username`
- **Bucket**：`your_bucket_name`
- **Org**：`your_organization`
- **Token**：`your_token`
- **Measurement**：`ResourceUsage`

### 配置

1. **创建插件配置文件**（例如 `/etc/crane/monitor.yaml`）：

```yaml
# Cgroup 路径模式（%j 将替换为作业的 cgroup 路径）
Cgroup:
  CPU: "/sys/fs/cgroup/cpuacct/%j/cpuacct.usage"
  Memory: "/sys/fs/cgroup/memory/%j/memory.usage_in_bytes"
  ProcList: "/sys/fs/cgroup/memory/%j/cgroup.procs"

# InfluxDB 配置
Database:
  Username: "your_username"
  Bucket: "your_bucket_name"
  Org: "your_organization"
  Token: "your_token"
  Measurement: "ResourceUsage"
  Url: "http://localhost:8086"

# 采样间隔（毫秒）
Interval: 1000
# 批量写入的缓冲区大小
BufferSize: 32
```

**注意**：对于 cgroup v2，请相应更新路径。

2. **在插件配置中注册插件**（`/etc/crane/plugin.yaml`）：

```yaml
Enabled: true
Plugins:
  - Name: "monitor"
    Path: "/usr/lib/crane/plugin/monitor.so"  # 软件包安装
    # Path: "/usr/local/lib/crane/plugin/monitor.so"  # 源代码安装（默认）
    # Path: "/path/to/build/plugin/monitor.so"  # 开发/构建目录
    Config: "/etc/crane/monitor.yaml"
```

!!! note "插件路径"
    - **软件包安装**：使用 `/usr/lib/crane/plugin/monitor.so`
    - **源代码安装**：使用 `/usr/local/lib/crane/plugin/monitor.so`（或自定义 PREFIX 位置）
    - **开发**：使用构建目录中的 `build/plugin/monitor.so`

## Mail 插件

mail 插件从鹤思控制节点（cranectld）通过电子邮件发送作业通知。

### 系统邮件配置

1. **安装邮件依赖项**：

```bash
dnf install s-nail postfix ca-certificates
```

**注意**：`mail` 命令在不同发行版中可能有不同的名称。Postfix 处理 SMTP 请求；sendmail 可用作替代方案。

2. **通过创建 `/etc/s-nail.rc` 配置 SMTP 设置**：

```bash
set from="your_email@example.com"
set smtp="smtp.example.com"
set smtp-auth-user="your_email@example.com"
set smtp-auth-password="your_app_password"
set smtp-auth=login
```

**重要**：使用应用程序特定的密码，而不是您的账户密码。请参阅您的电子邮件提供商的文档。

3. **测试邮件功能**：

```bash
echo "Test Mail Body" | mail -s "Test Mail Subject" recipient@example.com
```

### 插件配置

1. **创建邮件插件配置**（例如 `/etc/crane/mail.yaml`）：

```yaml
# 邮件通知的发件人地址
SenderAddr: example@example.com

# 仅发送主题的电子邮件（无正文）
SubjectOnly: false
```

2. **在 `/etc/crane/plugin.yaml` 中注册插件**（类似于 monitor 插件）。

### 使用电子邮件通知

#### 在作业脚本中

```bash
#!/bin/bash
#CBATCH --nodes 1
#CBATCH --ntasks-per-node 1
#CBATCH --mem 1G
#CBATCH -J EmailTest
#CBATCH --mail-type ALL
#CBATCH --mail-user user@example.com

hostname
echo "此作业触发电子邮件通知"
sleep 60
```

提交作业：

```bash
cbatch emailjob.sh
```

#### 命令行

```bash
cbatch --mail-type=ALL --mail-user=user@example.com test.job
```

### 邮件类型

| 类型 | 描述 |
|------|------|
| `BEGIN` | 作业开始运行（Pending → Running）|
| `FAILED` | 作业执行失败 |
| `TIMELIMIT` | 作业超出时间限制 |
| `END` | 作业完成（Running → Completed/Failed/Cancelled）|
| `ALL` | 上述所有事件 |

### 参数优先级

- `--mail-type` 和 `--mail-user` 自动更新内部 `--extra-attr` 参数
- 在作业脚本中：后面的选项会覆盖前面的选项
- 在命令行上：`--mail-type/user` 始终优先于 `--extra-attr`
- **建议**：除非您对 `--extra-attr` 有特定要求，否则使用 `--mail-type/user`

## 故障排除

- 验证 `/etc/crane/plugin.yaml` 中的 `Enabled: true`
- 检查 `cplugind` 是否正在运行：`systemctl status cplugind`
- 确保 `.so` 文件路径是绝对路径且可读
- 检查 `cplugind` 日志以查找错误
- 如果 `/etc/crane/plugin.yaml` 不存在，插件系统将自动禁用
