PMIx 使用指南
PMIx（Process Management Interface for Exascale）是面向超算的进程管理接口标准，为 MPI 等并行程序提供节点间进程发现、信息交换（modex）和集合屏障（fence）等核心服务。
CraneSched 内置了完整的 PMIx 服务端，由每个计算节点上的 Supervisor 进程承载。用户在使用 crun --mpi=pmix 时，无需借助 mpirun——CraneSched 本身就充当 Process Manager，直接在分配到的节点上按照 --ntasks-per-node 数量启动 MPI 进程，并通过内置 PMIx 服务端为每个进程分配 rank 和 namespace。

---
一、系统要求
1.1 运行时依赖
暂时无法在飞书文档外展示此内容

---
1.2 安装 OpenPMIx
=== "Rocky Linux 9 / RHEL 9（包管理器）"
```bash
sudo dnf install -y epel-release
sudo dnf install -y pmix pmix-devel
```
=== "Ubuntu 22.04 / Debian 12（包管理器）"
```bash
sudo apt-get install -y libpmix-dev libpmix2
```
=== "从源码编译（生产环境推荐）"
```bash
# 以安装到 /opt/pmix 为例
wget https://github.com/openpmix/openpmix/releases/download/v4.2.9/pmix-4.2.9.tar.bz2
tar xf pmix-4.2.9.tar.bz2 && cd pmix-4.2.9
./configure --prefix=/opt/pmix
make -j$(nproc) && sudo make install
```

---
1.3 安装启用了 PMIx 支持的 MPI 实现
Open MPI（推荐）
# 以 Open MPI 5.0.6 + 系统 PMIx 为例
wget https://download.open-mpi.org/release/open-mpi/v5.0/openmpi-5.0.6.tar.bz2
tar xf openmpi-5.0.6.tar.bz2 && cd openmpi-5.0.6

# 若 PMIx 安装在 /opt/pmix，通过 --with-pmix 指定
./configure --prefix=/opt/openmpi \
            --with-pmix=/opt/pmix

make -j$(nproc) && sudo make install
验证 PMIx 支持是否开启：
/opt/openmpi/bin/ompi_info | grep pmix
# 输出中应包含 MCA psec: pmix 或类似内容
!!! tip 若使用系统包管理器安装的 OpenPMIx（/usr），可以省略 --with-pmix，Open MPI 的配置脚本会自动检测到。
MPICH 4.x
MPICH 4.0 及以上版本通过 PRRTE（PMIx Reference Runtime Environment）支持 PMIx：
wget https://www.mpich.org/static/downloads/4.2.3/mpich-4.2.3.tar.gz
tar xf mpich-4.2.3.tar.gz && cd mpich-4.2.3
./configure --prefix=/opt/mpich \
            --with-pmix=/opt/pmix \
            --with-device=ch4:ucx   # 可选：使用 UCX 传输层
make -j$(nproc) && sudo make install

---
1.4 确认 CraneSched 已启用 PMIx 支持
PMIx 支持需在编译 CraneSched 时通过 CMake 选项开启。请联系集群管理员确认。
管理员启用方式：
cmake -DWITH_PMIX=/opt/pmix [其他选项] ..
若同时安装了 UCX，系统会通过 pkg-config 自动检测并启用高性能直连模式：
# 检查是否同时支持 UCX
pkg-config --libs ucx

---
二、快速上手
以下示例在 2 个节点、每节点 4 个 MPI 进程（共 8 个 rank）上运行 MPI 程序：
crun --nodes=2 --ntasks-per-node=4 --mpi=pmix ./my_mpi_program
CraneSched 会：
1. 向调度器申请 2 个节点的资源
2. 在每个节点上直接启动 4 个 ./my_mpi_program 进程
3. 通过内置 PMIx 服务端为这 8 个进程分配 rank（0–7）及 namespace
4. MPI 程序调用 MPI_Init() 时，通过 PMIx 协议完成握手，自动获取自己的 rank
整个过程不需要 mpirun。

---
三、基本用法
3.1 crun 直接模式（作业模式）
在登录节点上，从零申请资源并启动 MPI 程序：
crun [资源参数] --mpi=pmix <MPI程序路径> [程序参数]
常用示例：
# 2 节点，每节点 8 进程，共 16 rank
crun --nodes=2 \
     --ntasks-per-node=8 \
     --cpus-per-task=2 \
     --mem=16G \
     --time=01:00:00 \
     --partition=CPU \
     --mpi=pmix \
     ./my_simulation input.cfg

# 不指定节点数，只指定总任务数（由调度器决定节点分布）
crun --ntasks=32 \
     --cpus-per-task=1 \
     --mpi=pmix \
     ./my_simulation
3.2 cbatch 批处理作业
在批处理脚本内，通过 crun 启动 MPI 程序，资源参数写在 #CBATCH 指令中：
#!/bin/bash
#CBATCH --job-name=mpi_job
#CBATCH --nodes=4
#CBATCH --ntasks-per-node=16
#CBATCH --cpus-per-task=2
#CBATCH --mem-per-cpu=2G
#CBATCH --time=04:00:00
#CBATCH --partition=CPU
#CBATCH --output=mpi_%j.out
#CBATCH --error=mpi_%j.err
# 加载 MPI 环境（根据实际安装路径调整）

export PATH=/opt/openmpi/bin:$PATH
export LD_LIBRARY_PATH=/opt/openmpi/lib:$LD_LIBRARY_PATH
echo "作业 ID: $CRANE_JOB_ID"
echo "节点列表: $CRANE_JOB_NODELIST"
echo "节点数: $CRANE_JOB_NUM_NODES"

# 通过 crun 启动 PMIx 作业步# --nodes 和 --ntasks-per-node 可继承自 #CBATCH，或重新指定
crun --nodes=4 --ntasks-per-node=16 --mpi=pmix ./my_simulation
提交：
cbatch mpi_job.sh
3.3 作业步模式（在 calloc 会话中）
先用 calloc 申请资源，再用 crun 在已分配资源内创建多个独立的 PMIx 作业步：
# 1. 分配资源，进入交互式 shell
calloc --nodes=4 --ntasks-per-node=8 --time=02:00:00

# 2. 在分配的资源内依次运行两个 MPI 步骤#    （每次 crun 启动新的 PMIx 服务端，rank 从 0 开始独立编号）
crun --ntasks-per-node=8 --mpi=pmix ./preprocess data/
crun --ntasks-per-node=8 --mpi=pmix ./main_solver

---
四、支持的 MPI 实现
4.1 Open MPI
Open MPI 4.x / 5.x 原生支持 PMIx，直接运行编译好的二进制即可：
crun --nodes=4 --ntasks-per-node=8 --mpi=pmix ./my_mpi_program

---
五、高级配置
通过在作业脚本中设置环境变量，可以调整 CraneSched PMIx 服务端的行为。
5.1 集合通信算法（Fence 类型）
PMIx fence（屏障同步）支持两种集合算法：
暂时无法在飞书文档外展示此内容
# 使用环形算法（适合小规模作业）
export CRANE_PMIX_FENCE=ring
crun --nodes=4 --ntasks-per-node=4 --mpi=pmix ./my_program

# 使用树形算法（适合大规模作业，默认即为 tree，可不设置）
export CRANE_PMIX_FENCE=tree
crun --nodes=64 --ntasks-per-node=16 --mpi=pmix ./my_program
在 #CBATCH 脚本中：
#!/bin/bash
#CBATCH --nodes=8
#CBATCH --ntasks-per-node=16
export CRANE_PMIX_FENCE=ring

crun --nodes=8 --ntasks-per-node=16 --mpi=pmix ./my_program
5.2 UCX 直连模式
当集群配备 InfiniBand、RoCE 等高速网络并安装了 UCX 时，可启用 UCX 直连模式，让节点间 PMIx 元数据交换（modex、fence）绕过 gRPC，直接通过 UCX 传输，降低延迟：
export CRANE_PMIX_DIRECT_CONN_UCX=true
crun --nodes=16 --ntasks-per-node=8 --mpi=pmix ./my_program
!!! note 该选项需要 CraneSched 在编译时检测到 UCX（pkg-config --libs ucx 成功）。若 CraneSched 未编译 UCX 支持，任务启动失败。
5.3 同时配置两者
#!/bin/bash#CBATCH --nodes=32
#CBATCH --ntasks-per-node=24
#CBATCH --cpus-per-task=2
#CBATCH --time=08:00:00
export CRANE_PMIX_FENCE=tree
export CRANE_PMIX_DIRECT_CONN_UCX=true

crun --nodes=32 --ntasks-per-node=24 --mpi=pmix ./cfd_solver run.cfg

---
七、完整示例
示例 1：单节点 MPI Hello World
crun --nodes=1 --ntasks-per-node=4 --mpi=pmix ./hello_mpi
输出示例：
Hello from rank 0 of 4 on node cn01
Hello from rank 1 of 4 on node cn01
Hello from rank 2 of 4 on node cn01
Hello from rank 3 of 4 on node cn01
示例 2：多节点 CFD 模拟（批处理）
#!/bin/bash
#CBATCH --job-name=cfd_sim
#CBATCH --nodes=8
#CBATCH --ntasks-per-node=16
#CBATCH --cpus-per-task=2
#CBATCH --mem-per-cpu=2G
#CBATCH --time=06:00:00
#CBATCH --partition=CPU
#CBATCH --output=cfd_%j.out

export PATH=/opt/openmpi/bin:$PATH
echo "开始时间: $(date)"
echo "节点列表: $CRANE_JOB_NODELIST"

crun --nodes=8 --ntasks-per-node=16 --mpi=pmix ./cfd_solver -c config.json

echo "结束时间: $(date)"
示例 3：多阶段流水线作业
#!/bin/bash

#CBATCH --job-name=pipeline
#CBATCH --nodes=4
#CBATCH --ntasks-per-node=8
#CBATCH --time=04:00:00

export PATH=/opt/openmpi/bin:$PATH
export CRANE_PMIX_FENCE=ring

# 阶段 1：数据预处理（16 进程）
crun --nodes=2 --ntasks-per-node=8 --mpi=pmix ./preprocess data/input

# 阶段 2：主计算（32 进程）
crun --nodes=4 --ntasks-per-node=8 --mpi=pmix ./solver data/preprocessed

# 阶段 3：后处理（8 进程）
crun --nodes=1 --ntasks-per-node=8 --mpi=pmix ./postprocess data/results
示例 4：高性能网络环境（InfiniBand + UCX）
#!/bin/bash

#CBATCH --job-name=hpc_large
#CBATCH --nodes=64
#CBATCH --ntasks-per-node=16
#CBATCH --cpus-per-task=2
#CBATCH --time=12:00:00
#CBATCH --partition=IB

export CRANE_PMIX_FENCE=tree
export CRANE_PMIX_DIRECT_CONN_UCX=true# UCX 传输层同时用于 PMIx 元数据交换和 MPI 点对点通信

crun --nodes=64 --ntasks-per-node=16 --mpi=pmix \
     ./large_scale_sim -config run.cfg

echo "Simulation completed at $(date)"

---
八、常见问题与故障排查
Q1：提交时提示 MPI type pmix is not supported
原因：当前集群的 CraneSched 编译时未启用 PMIx 支持（即未使用 -DWITH_PMIX）。
解决方法：联系集群管理员确认并重新编译 CraneSched：
# 管理员重新编译（示例）
cmake -DWITH_PMIX=/opt/pmix -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)

---
Q2：MPI 程序在 MPI_Init() 处挂起
可能原因：节点间网络不通，导致 PMIx fence 超时；或 Supervisor 进程异常退出。
排查步骤：
1. 检查 Supervisor 日志：
tail -f /var/crane/supervisor/supervisor.log | grep -iE 'pmix|error'
1. 确认计算节点间 craned 监听端口（默认 10010）可互通。
2. 尝试减少节点数或切换 fence 算法：
export CRANE_PMIX_FENCE=ring

---
Q4：每个作业步的 rank 都从 0 开始，是否正常？
是正常行为。每次 crun --mpi=pmix 都创建一个独立的作业步，拥有独立的 PMIx namespace（如 crane.123.0、crane.123.1）。在同一作业步内，rank 范围是 0 到 总任务数 - 1。

---
九、注意事项
1. MPI 程序的编译要求：MPI 程序需要链接支持 PMIx 的 MPI 库（如带 --with-pmix 编译的 Open MPI）。传统的 PMI1/PMI2 程序在 PMIx 模式下可能无法正常初始化。
2. PMIx namespace 格式：每个作业步的 namespace 形如 crane.<job_id>.<step_id>，例如作业 ID=123、步骤 ID=0 时为 crane.123.0。