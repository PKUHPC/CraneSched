# 使用示例

本页提供鹤思容器功能的典型使用场景示例。

---

## 基础用法

```bash
# 运行简单命令
ccon -p CPU run ubuntu:22.04 echo "Hello, World!"

# 带资源限制
ccon -p CPU -c 4 --mem 8G -t 2:00:00 run python:3.11 python compute.py

# 交互式容器
ccon -p CPU run -it ubuntu:22.04 /bin/bash

# 后台运行
ccon -p CPU run -d python:3.11 python server.py
```

---

## GPU 作业

```bash
# 单 GPU
ccon -p GPU --gres gpu:1 run pytorch/pytorch:latest python train.py

# 多 GPU（单节点）
ccon -p GPU --gres gpu:4 run pytorch/pytorch:latest \
    torchrun --nproc_per_node=4 train.py

# 指定 GPU 类型
ccon -p GPU --gres gpu:a100:2 run pytorch/pytorch:latest python train.py
```

---

## 多节点作业

使用 `-N` 参数时，容器会同时在多个节点上启动：

```bash
# 2 节点并行
ccon -p CPU -N 2 run python:3.11 python worker.py

# 多节点 GPU 训练（4 节点 × 2 GPU）
ccon -p GPU -N 4 --gres gpu:2 run pytorch/pytorch:latest \
    torchrun --nnodes=4 --nproc_per_node=2 train.py
```

多节点容器交互需指定目标节点：

```bash
ccon logs -n node01 123.1
ccon exec -n node01 123.1 hostname
```

---

## 数据挂载与环境变量

```bash
ccon -p GPU --gres gpu:1 run \
    -v /shared/data:/data:ro \
    -v /home/user/output:/output \
    -e BATCH_SIZE=64 \
    -e NUM_WORKERS=4 \
    pytorch/pytorch:latest python train.py
```

---

## cbatch 脚本编排

创建 `pipeline.sh`：

```bash
#!/bin/bash
#CBATCH --job-name=ml-pipeline
#CBATCH -p GPU
#CBATCH --gres=gpu:1
#CBATCH -t 4:00:00
#CBATCH --pod

# 数据预处理
ccon run python:3.11 python preprocess.py

# 模型训练
ccon run pytorch/pytorch:latest python train.py

# 模型评估
ccon run pytorch/pytorch:latest python evaluate.py
```

提交：

```bash
cbatch pipeline.sh
```

---

## 混合作业步

容器与非容器作业步可以混用：

```bash
#!/bin/bash
#CBATCH -p CPU
#CBATCH -N 2
#CBATCH --pod

# 容器作业步
ccon run python:3.11 python prepare.py

# 非容器作业步
crun -N 2 ./mpi_compute

# 容器作业步
ccon run python:3.11 python postprocess.py
```

---

## 私有镜像仓库

```bash
# 登录
ccon login registry.example.com

# 使用私有镜像
ccon -p CPU run registry.example.com/team/image:v1.0 ./run.sh

# 登出
ccon logout registry.example.com
```

---

## 调试

```bash
# 查看日志
ccon logs 123.1
ccon logs -f 123.1          # 实时跟踪

# 进入容器
ccon exec -it 123.1 /bin/bash

# 查看详情
ccon inspect 123.1          # 容器作业步
ccon inspectp 123           # 容器作业
```

---

## 用户命名空间

```bash
# 默认启用（容器内为 root）
ccon -p CPU run ubuntu:22.04 id
# uid=0(root) gid=0(root)

# 禁用用户命名空间
ccon -p CPU run --userns=false ubuntu:22.04 id
# uid=1000(user) gid=1000(user)
```

---

## 网络配置

```bash
# 使用主机网络
ccon -p CPU run --network host python:3.11 python server.py

# 端口映射
ccon -p CPU run -p 8888:8888 jupyter/minimal-notebook
```

cbatch 脚本中：

```bash
#CBATCH --pod
#CBATCH --pod-host-network
#CBATCH --pod-port=8888:8888
```

---

## 相关文档

- [快速上手](quickstart.md)
- [核心概念](concepts.md)
- [运行期操作与排错](troubleshooting.md)
