# PMIx 使用指南

PMIx（Process Management Interface for Exascale）是面向超算的进程管理接口标准，为 MPI 等并行程序提供节点间进程发现、信息交换（modex）和集合屏障（fence）等核心服务。
CraneSched 内置了完整的 PMIx 服务端，由每个计算节点上的 Supervisor 进程承载。用户在使用 crun --mpi=pmix 时，无需借助 mpirun——CraneSched 本身就充当 Process Manager，直接在分配到的节点上启动 MPI 进程，并通过内置 PMIx 服务端为每个进程分配 rank 和 namespace。


## 一、系统要求
### 1.1 运行时依赖

|组件| 最低版本 | 说明|
|-----|------|-----|
OpenPMIx | 4.x | PMIx 运行时库（必须）|
Open MPI | 4.1+ | 支持 PMIx 的 MPI 实现（推荐 5.x）|
UCX（可选）| 1.12+	| 启用高性能直连模式（需要 InfiniBand/RoCE 等） |

### 1.2 安装 OpenPMIx
从源码编译（生产环境推荐），建议参考OpenPmix官方文档安装
```bash
sudo dnf install libevent-devel
sudo dnf install hwloc-devel
# 以安装到 /opt/pmix 为例
wget https://github.com/openpmix/openpmix/releases/download/v4.2.9/pmix-4.2.9.tar.bz2
tar xf pmix-4.2.9.tar.bz2 && cd pmix-4.2.9
./configure --prefix=/opt/pmix
make -j$(nproc) && sudo make install
```

### 1.3 安装启用了 PMIx 支持的 MPI 实现
**Open MPI（推荐）**

以 Open MPI 5.0.6 + 系统 PMIx 为例。建议参考OpenMpi官方文档安装
```bash
sudo dnf install gcc-gfortran
wget https://download.open-mpi.org/release/open-mpi/v5.0/openmpi-5.0.6.tar.bz2
tar xf openmpi-5.0.6.tar.bz2 && cd openmpi-5.0.6

# 若 PMIx 安装在 /use/pmix，通过 --with-pmix 指定
./configure --prefix=/opt/openmpi \
            --with-pmix=/opt/pmix

make -j$(nproc) && sudo make install
# 验证 PMIx 支持是否开启：
/opt/openmpi/bin/ompi_info | grep pmix
# 输出中应包含 MCA psec: pmix 或类似内容
```
**若使用系统包管理器安装的 OpenPMIx（/opt），可以省略 --with-pmix，Open MPI 的配置脚本会自动检测到。**


### 1.4 确认 CraneSched 已启用 PMIx 支持
PMIx 支持需在编译 CraneSched 时通过 CMake 选项开启。请联系集群管理员确认。
```bash
# 管理员启用方式：
cmake -DWITH_PMIX=/opt/pmix [其他选项] ..
# 若同时安装了 UCX，系统会通过 pkg-config 自动检测并启用高性能直连模式：
# 检查是否同时支持 UCX
pkg-config --libs ucx
```
---
## 二、快速上手
以下示例在 2 个节点、每节点 4 个 MPI 进程（共 8 个 rank）上运行 MPI 程序：
```bash
crun --nodes=2 --ntasks-per-node=4 --mpi=pmix ./my_mpi_program
```
CraneSched 会：
1. 向调度器申请 2 个节点的资源
2. 在每个节点上直接启动 4 个 ./my_mpi_program 进程
3. 通过内置 PMIx 服务端为这 8 个进程分配 rank（0–7）及 namespace
4. MPI 程序调用 MPI_Init() 时，通过 PMIx 协议完成握手，自动获取自己的 rank
整个过程不需要 mpirun。


## 三、基本用法

### 3.1 crun 直接模式（作业模式）

在登录节点上，从零申请资源并启动 MPI 程序：
crun [资源参数] --mpi=pmix <MPI程序路径> [程序参数]

常用示例：

2 节点，每节点 8 进程，共 16 rank
```bash
crun --nodes=2 \
     --ntasks-per-node=8 \
     --mpi=pmix \
     ./my_simulation input.cfg
```

不指定节点数，只指定总任务数（由调度器决定节点分布）
```bash
crun --ntasks=32 \
     --mpi=pmix \
     ./my_simulation
```

### 3.2 cbatch 批处理作业

在批处理脚本内，通过 crun 启动 MPI 程序，资源参数写

在 #CBATCH 指令中：
```sh
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
```

提交：
```bash
cbatch mpi_job.sh
```

### 3.3 作业步模式（在 calloc 会话中）

先用 calloc 申请资源，再用 crun 在已分配资源内创建多个独立的 PMIx 作业步：

1. 分配资源，进入交互式 shell
```bash
calloc --nodes=4 --ntasks-per-node=8 --time=02:00:00
```
2. 在分配的资源内依次运行两个 MPI 步骤#    （每次 crun 启动新的 PMIx 服务端，rank 从 0 开始独立编号）
```bash
crun --ntasks-per-node=8 --mpi=pmix ./preprocess data/
crun --ntasks-per-node=8 --mpi=pmix ./main_solver
```

## 四、高级配置
通过在作业脚本中设置环境变量，可以调整 CraneSched PMIx 服务端的行为。

### 4.1 集合通信算法（Fence 类型）
PMIx fence（屏障同步）支持两种集合算法：

| 值 | 算法	| 适用场景|
|---|----|-----|
tree | 树形聚合 | 节点数较多时效率更高 |
ring | 环形传递 | 节点数较少时延迟更低 |

**使用环形算法（适合小规模作业）**
```bash
export CRANE_PMIX_FENCE=ring
crun --nodes=4 --ntasks-per-node=4 --mpi=pmix ./my_program
```

**使用树形算法（适合大规模作业，默认即为 tree，可不设置）**
```bash
export CRANE_PMIX_FENCE=tree
crun --nodes=64 --ntasks-per-node=16 --mpi=pmix ./my_program
```

在 #CBATCH 脚本中：
```sh
#!/bin/bash
#CBATCH --nodes=8
#CBATCH --ntasks-per-node=16
export CRANE_PMIX_FENCE=ring

crun --nodes=8 --ntasks-per-node=16 --mpi=pmix ./my_program
```

### 4.2 UCX 直连模式
当集群配备 InfiniBand、RoCE 等高速网络并安装了 UCX 时，可启用 UCX 直连模式，让节点间 PMIx 元数据交换（modex、fence）绕过 gRPC，直接通过 UCX 传输，降低延迟：
```bash
export CRANE_PMIX_DIRECT_CONN_UCX=true
crun --nodes=16 --ntasks-per-node=8 --mpi=pmix ./my_program
```
**该选项需要 CraneSched 在编译时检测到 UCX（pkg-config --libs ucx 成功）。若 CraneSched 未编译 UCX 支持，任务启动失败。**

## 五、完整示例
示例 1：单节点 MPI Hello World
```bash
crun --nodes=1 --ntasks-per-node=4 --mpi=pmix ./hello_mpi
```
输出示例：
Hello from rank 0 of 4 on node cn01
Hello from rank 1 of 4 on node cn01
Hello from rank 2 of 4 on node cn01
Hello from rank 3 of 4 on node cn01

示例 2：多节点 CFD 模拟（批处理）
```sh
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
```
示例 3：多阶段流水线作业
```sh
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
```

示例 4：高性能网络环境（InfiniBand + UCX）
```sh
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
```

## 六、常见问题与故障排查

**Q1：提交时提示 `PMIx support is not compiled in.`**

原因：当前集群的 CraneSched 编译时未启用 PMIx 支持（即未使用 `-DWITH_PMIX`）。

解决方法：联系集群管理员确认并重新编译 CraneSched：

管理员重新编译（示例）
```bash
cmake -DWITH_PMIX=/opt/pmix -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)
```