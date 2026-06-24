# 使用示例

本页提供鹤思容器功能的典型使用场景示例。多数示例使用 `alpine`、`ubuntu`、`python:3.11`、`nginx`、`nicolaka/netshoot` 等公共镜像，可直接复制运行；少数演示真实工作流的示例是模板，需要替换为你自己的脚本，已在注释中标注。

---

## 基础用法

```bash
# 一次性命令：作业完成后容器立即退出
ccon -p CPU run alpine:latest echo "Hello, World!"

# 携带资源限制
#   -c 4         分配 4 个 CPU 核
#   --mem 8G     分配 8 GiB 内存
#   -t 2:00:00   最长运行 2 小时（hh:mm:ss）
ccon -p CPU -c 4 --mem 8G -t 2:00:00 run python:3.11 \
    python -c "import math; print(math.pi)"

# 交互式容器
#   -i  保持 stdin 打开    -t  分配伪终端
#   Ctrl-P + Ctrl-Q 可脱离连接且不停止容器
ccon -p CPU run -it ubuntu:22.04 /bin/bash

# 后台运行：命令返回容器 ID 后立即脱离终端
#   -d  detached 模式
ccon -p CPU run -d python:3.11 python -m http.server 8000
```

---

## GPU 作业

```bash
# 单 GPU 自检：检测 CUDA 是否可用
#   --gres gpu:1   申请 1 块 GPU（任意型号）
ccon -p GPU --gres gpu:1 run pytorch/pytorch:latest \
    python -c "import torch; print(torch.cuda.is_available(), torch.cuda.device_count())"

# 指定 GPU 型号（如 A100），并用 nvidia-smi 查看可见设备
#   --gres gpu:a100:2   申请 2 块 A100
ccon -p GPU --gres gpu:a100:2 run pytorch/pytorch:latest nvidia-smi

# 多 GPU 训练模板（需自备 train.py）
ccon -p GPU --gres gpu:4 run pytorch/pytorch:latest \
    torchrun --nproc_per_node=4 train.py
```

---

## 多节点作业

`-N` 控制并行节点数，容器会在每个分配到的节点上同时启动一份。

```bash
# 在 2 个节点上各打印一次主机名
ccon -p CPU -N 2 run alpine:latest hostname

# 多节点 GPU 训练模板（4 节点 × 2 GPU，需自备 train.py）
ccon -p GPU -N 4 --gres gpu:2 run pytorch/pytorch:latest \
    torchrun --nnodes=4 --nproc_per_node=2 train.py
```

多节点作业的日志查询和命令执行需通过 `-n` 指定目标节点：

```bash
ccon logs -n node01 123.1
ccon exec -n node01 123.1 hostname
```

---

## 数据挂载与环境变量

```bash
# -v HOST:CONTAINER   将宿主机目录挂载到容器内
# -e KEY=VALUE        注入环境变量
ccon -p CPU run \
    -v /tmp:/data \
    -e GREETING=hello \
    alpine:latest sh -c 'echo "$GREETING from $(hostname)"; ls /data | head'
```

---

## cbatch 脚本编排

将多个容器步骤串联到一个批处理脚本中，所有步骤复用同一份资源分配。

创建 `pipeline.sh`：

```bash
#!/bin/bash
#CBATCH --job-name=pipeline-demo
#CBATCH -p CPU
#CBATCH -t 0:30:00
#CBATCH --pod                       # 声明为容器作业（Pod）

# 步骤 1：模拟数据准备
ccon run alpine:latest sh -c 'echo "prepare $(date)"'

# 步骤 2：模拟计算
ccon run python:3.11 python -c "print(sum(range(1000)))"

# 步骤 3：模拟评估
ccon run alpine:latest sh -c 'echo "evaluate done"'
```

提交：

```bash
cbatch pipeline.sh
```

---

## 混合作业步

容器与非容器作业步可以在同一个作业内混用：

```bash
#!/bin/bash
#CBATCH -p CPU
#CBATCH -N 2
#CBATCH --pod

# 容器作业步
ccon run alpine:latest sh -c 'echo prepare on $(hostname)'

# 非容器作业步（直接在分配节点上执行）
crun -N 2 hostname

# 容器作业步
ccon run alpine:latest sh -c 'echo postprocess done'
```

---

## 私有镜像仓库

```bash
# 登录：交互式输入用户名 / 密码，凭证保存在用户配置中
ccon login registry.example.com

# 使用私有镜像运行
ccon -p CPU run registry.example.com/team/image:v1.0 echo "private ok"

# 登出：清除已保存的凭证
ccon logout registry.example.com
```

---

## 调试

```bash
# 查看日志（123.1 = JOB.STEP）
ccon logs 123.1
ccon logs -f 123.1          # -f 持续跟踪输出
ccon logs --tail 100 123.1  # 仅显示最后 100 行

# 进入容器
#   Ctrl-P + Ctrl-Q 可脱离连接且不停止容器
ccon exec -it 123.1 /bin/bash

# 查看详情
ccon inspect 123.1          # 容器作业步
ccon inspectp 123           # 容器作业（Pod）
```

---

## 用户命名空间

默认启用用户命名空间，容器内的 root（uid 0）映射到宿主机上的普通用户，避免容器内特权越界。

```bash
# 默认行为：容器内显示为 root
ccon -p CPU run ubuntu:22.04 id
# uid=0(root) gid=0(root)

# 关闭后，进程以宿主机用户身份运行
ccon -p CPU run --userns=false ubuntu:22.04 id
# uid=1000(user) gid=1000(user)
```

---

## 网络配置

`nicolaka/netshoot` 自带 `curl`、`dig`、`tcpdump`、`ip` 等网络调试工具，非常适合验证容器网络。

```bash
# 默认网络：容器有独立网络命名空间
ccon -p CPU run nicolaka/netshoot ip addr show

# --network host：直接共享宿主机网络栈，无端口隔离
ccon -p CPU run --network host nicolaka/netshoot \
    sh -c 'hostname; ss -ltn | head'

# 端口映射：把容器内的 80 端口暴露到宿主机的 8080 端口
#   -p HOST:CONTAINER
ccon -p CPU run -p 8080:80 nginx:latest
```

cbatch 脚本中等价的指令：

```bash
#CBATCH --pod
#CBATCH --pod-host-network       # 等价于 ccon run --network host
#CBATCH --pod-port=8080:80       # 等价于 ccon run -p 8080:80
```

---

## 相关文档

- [快速上手](quickstart.md)
- [核心概念](concepts.md)
- [运行期操作与排错](troubleshooting.md)
