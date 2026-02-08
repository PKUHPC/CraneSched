# ccon - 容器作业管理

ccon 是鹤思系统的容器作业管理工具，用于创建、管理和监控容器化作业。ccon 的设计理念借鉴了 Docker CLI，使用户能够以熟悉的方式操作容器。

!!! info "前置条件"
    使用 ccon 前，需确保集群已启用容器功能。参见 [容器功能部署](../deployment/container.md)。

## 快速开始

运行一个简单的容器作业：

```bash
# 在 CPU 分区运行 alpine 容器
ccon -p CPU run alpine:latest -- echo "Hello from container"
```

## 命令概览

| 命令 | 说明 |
|:-----|:-----|
| `run` | 创建并运行新容器 |
| `ps` | 列出容器（作业步） |
| `pods` | 列出容器 Pod（作业） |
| `stop` | 停止运行中的容器 |
| `wait` | 等待当前作业的所有容器步骤完成 |
| `logs` | 查看容器日志 |
| `attach` | 连接到运行中的容器 |
| `exec` | 在运行中的容器内执行命令 |
| `inspect` | 显示容器步骤详情 |
| `inspectp` | 显示 Pod 详情 |
| `login` | 登录容器镜像仓库 |
| `logout` | 登出容器镜像仓库 |

## 全局选项

**-C, --config=&lt;path&gt;**

:   配置文件路径。默认值：`/etc/crane/config.yaml`。

**--debug-level=&lt;level&gt;**

:   设置调试输出级别。可用级别：`trace`、`debug`、`info`。默认值：`info`。

**--json**

:   以 JSON 格式输出结果。

**-h, --help**

:   显示帮助信息。

**-v, --version**

:   显示版本信息。

## run 命令

创建并运行新的容器作业。

```bash
ccon [Crane 选项] run [Run 选项] IMAGE [COMMAND] [ARG...]
```

!!! tip "Crane 选项位置"
    Crane 选项（如 `-p`、`-N`、`--mem`）必须放在 `ccon` 和 `run` 之间，而非 `run` 之后。

### Crane 选项（资源调度）

以下选项用于控制作业的资源分配和调度行为：

**-N, --nodes=&lt;num&gt;**

:   运行所需的节点数量。默认值：1。

**-c, --cpus-per-task=&lt;ncpus&gt;**

:   每个任务所需的 CPU 核心数。默认值：1。

**--ntasks-per-node=&lt;ntasks&gt;**

:   每个节点要调用的任务数量。默认值：1。

**--mem=&lt;size&gt;**

:   最大实际内存量。支持单位：GB（G, g）、MB（M, m）、KB（K, k）和 Bytes（B）。默认单位：MB。

**--gres=&lt;list&gt;**

:   每个任务所需的通用资源。格式：`gpu:a100:1` 或 `gpu:1`。

**-p, --partition=&lt;partition&gt;**

:   请求的分区。

**-A, --account=&lt;account&gt;**

:   提交作业的账户。

**-q, --qos=&lt;qos&gt;**

:   作业使用的服务质量（QoS）。

**-t, --time=&lt;time&gt;**

:   时间限制，格式：`[day-]hours:minutes:seconds`。

**-w, --nodelist=&lt;nodes&gt;**

:   要分配给作业的节点（逗号分隔列表）。

**-x, --exclude=&lt;nodes&gt;**

:   从分配中排除的节点（逗号分隔列表）。

**-r, --reservation=&lt;name&gt;**

:   使用预留资源。

**--exclusive**

:   请求独占节点资源。

**-H, --hold**

:   以挂起状态提交作业。

**--extra-attr=&lt;json&gt;**

:   作业的额外属性（JSON 格式）。

**--mail-type=&lt;type&gt;**

:   邮件通知类型。支持：`NONE`、`BEGIN`、`END`、`FAIL`、`TIMELIMIT`、`ALL`。

**--mail-user=&lt;email&gt;**

:   通知接收者的邮件地址。

**--comment=&lt;string&gt;**

:   作业注释。

### Run 选项（容器配置）

以下选项用于配置容器运行参数：

**--name=&lt;name&gt;**

:   为容器指定名称。

**-e, --env=&lt;KEY=VALUE&gt;**

:   设置环境变量。可多次使用。

**-v, --volume=&lt;host:container&gt;**

:   绑定挂载卷。格式：`宿主机路径:容器路径`。可多次使用。

**-p, --ports=&lt;host:container&gt;**

:   发布容器端口到宿主机。格式：`宿主机端口:容器端口`。可多次使用。

**-d, --detach**

:   后台运行容器并输出容器 ID。

**-i, --interactive**

:   保持标准输入对容器进程可用。

**-t, --tty**

:   为容器分配伪终端。

**--entrypoint=&lt;cmd&gt;**

:   覆盖镜像默认入口点。

**-u, --user=&lt;uid[:gid]&gt;**

:   以指定 UID 运行容器。当 `--userns=false` 时，仅允许当前用户及其可访问的组。

**--userns**

:   启用用户命名空间。默认：`true`（容器内用户映射为 root）。

**--network=&lt;mode&gt;**

:   容器网络模式。支持 `host`（使用宿主机网络）和 `default`（默认 Pod 网络）。

**-w, --workdir=&lt;dir&gt;**

:   容器内的工作目录。

**--pull-policy=&lt;policy&gt;**

:   镜像拉取策略。支持：`Always`、`IfNotPresent`、`Never`。

**--dns=&lt;ip-address&gt;**

:   为容器设置使用的 DNS 服务器（仅支持 IPv4）。指定的 DNS server 会被添加到系统默认 DNS 之前（优先级更高）。
### 示例

基本容器作业：

```bash
ccon -p CPU run alpine:latest -- echo "Hello"
```

交互式容器：

```bash
ccon -p CPU run -it ubuntu:22.04 -- /bin/bash
```

带资源限制的 GPU 容器：

```bash
ccon -p GPU --gres gpu:1 --mem 8G run pytorch/pytorch:latest -- python train.py
```

挂载数据目录：

```bash
ccon -p CPU run -v /data/input:/input -v /data/output:/output alpine:latest -- cp /input/file /output/
```

多节点容器作业：

```bash
ccon -p CPU -N 4 run mpi-image:latest -- mpirun -np 4 ./my_program
```

后台运行：

```bash
ccon -p CPU run -d nginx:latest
# 输出: 123.1（作业ID.步骤ID）
```

## ps 命令

列出容器步骤（Container Step）。

```bash
ccon ps [选项]
```

**-a, --all**

:   显示所有容器（默认仅显示运行中的）。

**-q, --quiet**

:   仅显示容器 ID。

### 示例

```bash
# 列出运行中的容器
ccon ps

# 列出所有容器（包括已结束的）
ccon ps -a

# 仅输出容器 ID
ccon ps -q
```

## pods 命令

列出容器 Pod（容器作业）。

```bash
ccon pods [选项]
```

**-a, --all**

:   显示所有 Pod（默认仅显示运行中的）。

**-q, --quiet**

:   仅显示 Pod ID。

### 示例

```bash
# 列出运行中的 Pod
ccon pods

# 列出所有 Pod
ccon pods -a
```

## stop 命令

停止运行中的容器。

```bash
ccon stop [选项] CONTAINER
```

**-t, --timeout=&lt;seconds&gt;**

:   等待容器停止的超时时间（秒），超时后强制终止。默认值：10。

### 示例

```bash
# 停止容器（等待 10 秒）
ccon stop 123.1

# 立即停止容器
ccon stop -t 0 123.1
```

## wait 命令

等待当前作业的所有容器步骤完成。通常在 cbatch 脚本中使用。

```bash
ccon wait [选项]
```

**-t, --interval=&lt;seconds&gt;**

:   轮询间隔时间（秒），最小 10 秒。

### 示例

```bash
# 在 cbatch 脚本中等待所有容器完成
ccon wait
```

## logs 命令

查看容器日志。

```bash
ccon logs [选项] CONTAINER
```

**-f, --follow**

:   持续跟踪日志输出。

**--tail=&lt;lines&gt;**

:   显示日志末尾的行数。

**-t, --timestamps**

:   显示时间戳。

**--since=&lt;time&gt;**

:   显示指定时间之后的日志。格式：`2025-01-15T10:30:00Z` 或相对时间如 `42m`。

**--until=&lt;time&gt;**

:   显示指定时间之前的日志。格式同上。

**-n, --target-node=&lt;node&gt;**

:   从指定节点获取日志。

### 示例

```bash
# 查看容器日志
ccon logs 123.1

# 跟踪日志输出
ccon logs -f 123.1

# 查看最后 100 行日志
ccon logs --tail 100 123.1
```

## attach 命令

连接到运行中的容器的标准输入、输出和错误流。

```bash
ccon attach [选项] CONTAINER
```

**--stdin**

:   连接标准输入。默认：`true`。

**--stdout**

:   连接标准输出。默认：`true`。

**--stderr**

:   连接标准错误。默认：`false`。

**--tty**

:   分配伪终端。默认：`true`。

**--transport=&lt;protocol&gt;**

:   传输协议。支持：`spdy`、`ws`。默认：`spdy`。

**-n, --target-node=&lt;node&gt;**

:   连接到指定节点上的容器。

### 示例

```bash
# 连接到容器
ccon attach 123.1
```

## exec 命令

在运行中的容器内执行命令。

```bash
ccon exec [选项] CONTAINER COMMAND [ARG...]
```

**-i, --interactive**

:   保持标准输入打开。

**-t, --tty**

:   分配伪终端。

**--transport=&lt;protocol&gt;**

:   传输协议。支持：`spdy`、`ws`。默认：`spdy`。

**-n, --target-node=&lt;node&gt;**

:   在指定节点上执行命令。

### 示例

```bash
# 在容器内执行命令
ccon exec 123.1 ls -la

# 交互式 shell
ccon exec -it 123.1 /bin/bash
```

## inspect 命令

显示容器步骤的详细信息。

```bash
ccon inspect CONTAINER
```

### 示例

```bash
ccon inspect 123.1
```

## inspectp 命令

显示 Pod 的详细信息。

```bash
ccon inspectp POD
```

### 示例

```bash
ccon inspectp 123
```

## login 命令

登录容器镜像仓库。

```bash
ccon login [选项] SERVER
```

**-u, --username=&lt;user&gt;**

:   用户名。

**-p, --password=&lt;pass&gt;**

:   密码。

**--password-stdin**

:   从标准输入读取密码。

### 示例

```bash
# 交互式登录
ccon login registry.example.com

# 使用用户名密码登录
ccon login -u myuser -p mypass registry.example.com

# 从标准输入读取密码
echo $TOKEN | ccon login -u myuser --password-stdin registry.example.com
```

## logout 命令

登出容器镜像仓库。

```bash
ccon logout SERVER
```

### 示例

```bash
ccon logout registry.example.com
```

## 容器 ID 格式

ccon 使用 `JOBID.STEPID` 格式标识容器：

- **JOBID**：作业编号，由调度系统分配
- **STEPID**：作业步编号，从 0 开始递增

例如：
- `123.0`：作业 123 的第一个容器步骤
- `123.1`：作业 123 的第二个容器步骤

Pod（容器作业）使用纯 JOBID 标识，如 `123`。

## 与 cbatch 配合使用

!!! info "Pod 相关选项"
    完整的 Pod 相关选项，请参考 [cbatch 命令手册](cbatch.md) 和 [容器快速上手](../reference/container/quickstart.md)。

使用 `cbatch --pod` 创建支持容器的作业，然后在脚本中使用 `ccon run` 启动容器：

```bash
#!/bin/bash
#CBATCH --pod
#CBATCH -N 2
#CBATCH -c 4
#CBATCH --mem 8G
#CBATCH -p GPU

# 在作业内启动多个容器
ccon -N 1 run -d alpine:latest -- echo "Hello from container"
ccon -N 1 run -d pytorch/pytorch:latest -- python train.py

# 等待容器作业步完成后自动退出
ccon wait
```

提交作业：

```bash
cbatch train_job.sh
```

## 另请参阅

- [容器功能概览](../reference/container/index.md) - 容器功能介绍
- [核心概念](../reference/container/concepts.md) - Pod、容器步骤等概念说明
- [快速上手](../reference/container/quickstart.md) - 快速体验容器功能
- [使用示例](../reference/container/examples.md) - 典型使用场景
- [故障排除](../reference/container/troubleshooting.md) - 常见问题与解决方案
- [cbatch](cbatch.md) - 批处理作业提交
- [cqueue](cqueue.md) - 查看作业队列
