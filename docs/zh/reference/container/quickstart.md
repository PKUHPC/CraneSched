# 快速上手

本页帮助你快速开始使用鹤思容器功能。阅读本文后，你将能够提交容器作业、查看容器状态、追加容器作业步并进行运行期交互。

!!! note "前置条件"
    - 集群管理员已启用容器功能（CRI 已配置）
    - 你拥有有效的用户账户和分区访问权限
    - 容器镜像可从节点访问（本地或远程仓库）

---

## 第一个容器作业

使用 `ccon run` 提交一个简单的容器作业：

```bash
ccon -p CPU run ubuntu:22.04 echo "Hello from container"
```

参数说明：

| 参数 | 说明 |
| --- | --- |
| `-p CPU` | 指定分区（Crane Flag，放在 `ccon` 和 `run` 之间） |
| `ubuntu:22.04` | 容器镜像 |
| `echo "Hello from container"` | 容器启动命令 |

命令执行后，作业将进入队列等待调度。调度成功后，容器在分配的节点上运行，输出结果后退出。

---

## 查看容器状态

### 查看容器作业步

```bash
# 查看运行中的容器作业步
ccon ps

# 查看所有容器作业步（包括已完成）
ccon ps -a
```

输出示例：

```
CONTAINER ID   IMAGE          COMMAND                  CREATED        STATUS     NODE
123.1          ubuntu:22.04   echo "Hello from co..."  2 minutes ago  Running    node01
124.1          python:3.11    python train.py          5 minutes ago  Completed  node02
```

### 查看容器作业（Pod）

```bash
# 查看运行中的容器作业
ccon pods

# 查看所有容器作业
ccon pods -a
```

输出示例：

```
POD ID   NAME       CREATED         STATUS     NODES
123      myjob      2 minutes ago   Running    node01
124      training   5 minutes ago   Completed  node02
```

---

## 交互式容器

使用 `-it` 参数启动交互式容器：

```bash
ccon -p CPU run -it ubuntu:22.04 /bin/bash
```

参数说明：

| 参数 | 说明 |
| --- | --- |
| `-i` | 保持标准输入打开 |
| `-t` | 分配伪终端 |

进入容器后，你可以像使用普通终端一样执行命令。退出容器后，作业自动结束。

---

## 后台运行容器

使用 `-d` 参数在后台运行容器：

```bash
ccon -p CPU run -d --name myserver nginx:latest
```

命令返回容器 ID（格式为 `JOBID.STEPID`），容器在后台持续运行。

---

## 使用 cbatch --pod

`cbatch --pod` 允许你使用批处理脚本创建容器作业，适合需要编排多个容器任务的场景。

### 创建脚本

创建文件 `job.sh`：

```bash
#!/bin/bash
#CBATCH --job-name=container-job
#CBATCH -p CPU
#CBATCH -N 1
#CBATCH --pod

echo "Job started on $(hostname)"

# 运行第一个容器任务
ccon run python:3.11 python -c "print('Step 1: Data preprocessing')"

# 运行第二个容器任务
ccon run python:3.11 python -c "print('Step 2: Model training')"

echo "Job completed"
```

### 提交作业

```bash
cbatch job.sh
```

脚本中的 `#CBATCH --pod` 指令表示这是一个容器作业。作业开始时，系统自动创建 Pod，脚本作为 Primary Step 运行。脚本内的 `ccon run` 命令追加容器作业步，复用作业分配的资源。

---

## 追加容器作业步

在容器作业运行期间，可以追加新的容器作业步。

### 在 cbatch 脚本内

脚本内直接使用 `ccon run`：

```bash
#!/bin/bash
#CBATCH --pod
#CBATCH -p CPU

# 追加容器作业步
ccon run ubuntu:22.04 echo "This is a container step"

# 后台追加容器作业步
ccon run -d redis:latest

# 等待所有容器作业步完成
ccon wait
```

### 在交互式会话中

首先使用 `calloc` 分配资源并创建容器作业：

```bash
# 分配资源（此功能需要配合 cbatch --pod 使用）
cbatch --pod -N 1 -p CPU --wrap "sleep infinity" &

# 假设作业 ID 为 125，可以在另一个终端追加容器作业步
export CRANE_JOB_ID=125
ccon run python:3.11 python script.py
```

---

## 运行期交互

### 查看容器日志

```bash
# 查看容器日志
ccon logs 123.1

# 实时跟踪日志
ccon logs -f 123.1

# 查看最后 100 行
ccon logs --tail 100 123.1
```

### 连接到运行中的容器

```bash
# 连接到容器的标准输入/输出
ccon attach 123.1
```

使用 `Ctrl+C` 断开连接（不会终止容器）。

### 在容器内执行命令

```bash
# 执行单个命令
ccon exec 123.1 ls -la /app

# 启动交互式 shell
ccon exec -it 123.1 /bin/bash
```

---

## 停止容器

```bash
# 停止容器作业步
ccon stop 123.1

# 指定超时时间（秒）
ccon stop -t 30 123.1
```

停止容器作业步会向容器发送 SIGTERM 信号，超时后发送 SIGKILL。

---

## 常用资源参数

`ccon run` 支持标准的 Crane 资源参数：

```bash
# 指定 CPU 和内存
ccon -c 4 --mem 8G -p CPU run python:3.11 python train.py

# 使用 GPU
ccon --gres gpu:1 -p GPU run pytorch/pytorch:latest python train.py

# 指定时间限制
ccon -t 1:00:00 -p CPU run ubuntu:22.04 ./long_task.sh

# 指定节点数
ccon -N 2 -p CPU run mpi-image mpirun ./my_mpi_app
```

!!! tip "Crane Flags 位置"
    Crane 资源参数（如 `-p`、`-c`、`--mem`、`--gres`）必须放在 `ccon` 和 `run` 之间，而容器参数（如 `-d`、`-it`、`-v`）放在 `run` 之后。
    
    ```bash
    ccon [Crane Flags] run [Container Flags] IMAGE [COMMAND]
    ```

---

## 挂载目录

使用 `-v` 参数挂载主机目录到容器：

```bash
# 挂载数据目录
ccon -p CPU run -v /data/input:/input -v /data/output:/output python:3.11 python process.py
```

格式为 `主机路径:容器路径`。

---

## 环境变量

使用 `-e` 参数设置环境变量：

```bash
ccon -p CPU run -e MODEL_PATH=/models -e BATCH_SIZE=32 python:3.11 python train.py
```

---

## 下一步

- [使用示例](examples.md)：更多典型场景示例
- [核心概念](concepts.md)：深入理解容器作业模型
- [运行期操作与排错](troubleshooting.md)：问题诊断与解决
