# PMIx 使用指南

PMIx（Process Management Interface for Exascale）是面向超算的进程管理接口标准，为 MPI 等并行程序提供节点间进程发现、信息交换（modex）和集合屏障（fence）等核心服务。
CraneSched 内置了完整的 PMIx 服务端，由每个计算节点上的 Supervisor 进程承载。用户在使用 crun --mpi=pmix 时，无需借助 mpirun——CraneSched 本身就充当 Process Manager，直接在分配到的节点上启动 MPI 进程，并通过内置 PMIx 服务端为每个进程分配 rank 和 namespace。


## 一、系统要求
### 1.1 运行时依赖

|组件| 最低版本 | 说明|
|-----|------|-----|
OpenPMIx | 3.x | PMIx 运行时库（必须）|
Open MPI | 4.1+ | 支持 PMIx 的 MPI 实现（推荐 4.1.6）|
UCX（可选）| 1.12+	| 启用高性能直连模式（需要 InfiniBand/RoCE 等） |

### 1.2 安装 OpenPMIx

CraneSched 默认开启 PMIx，构建需要 PMIx 3 或更新版本的开发包；运行节点需要匹配的运行库。
优先使用发行版软件包：Rocky Linux 9/10 安装 `pmix-devel`，Ubuntu 安装 `libpmix-dev`，
由包管理器准备 libevent、hwloc 等依赖。Rocky Linux 8 的 PMIx 2.2.5 不满足要求，需要维护的较新版本。
系统包不足时，按 [OpenPMIx 官方安装说明](https://docs.openpmix.org/en/latest/installing-pmix/index.html)
构建并交付依赖，确保运行节点也能安装对应版本；仅在构建机安装不能满足运行要求。

自定义安装通过 `PMIx_ROOT` 或 `CMAKE_PREFIX_PATH` 提供前缀。使用自定义共享库目录时可设置
`CMAKE_INSTALL_RPATH_USE_LINK_PATH=ON`，安装后检查实际加载路径。不能把同名 SONAME 当成版本兼容证明。

### 1.3 安装启用了 PMIx 支持的 MPI 实现

构建 CraneSched 的 PMIx 支持本身不需要 Open MPI。运行 MPI 程序时，还需要支持 PMIx 的 MPI 实现，
可使用发行版 Open MPI，或按 [Open MPI 安装说明](https://docs.open-mpi.org/en/main/installing-open-mpi/index.html)
安装匹配版本。核对 MPI 客户端与 PMIx 服务端互操作，以及 libevent/hwloc 等支持库的一致性；
实际执行 MPI 作业，而不只检查版本命令。`crun --mpi=pmix` 直接启动 MPI 进程，不需要再套一层 `mpirun`。

### 1.4 配置 CraneSched PMIx 支持

功能开关为 `CRANE_ENABLE_PMIX`，默认 ON；启用时缺少兼容依赖会在 CMake 配置阶段失败。
系统安装自动发现，自定义前缀示例如下：

```bash
cmake -S . -B build -DCRANE_ENABLE_PMIX=ON -DPMIx_ROOT=/opt/pmix
cmake --build build --parallel 4
```

不需要 PMIx 时显式设置 `-DCRANE_ENABLE_PMIX=OFF`。路径提示不承担启停功能。
UCX 传输默认关闭；安装 UCX 开发包后，通过 `-DCRANE_ENABLE_UCX=ON` 显式启用，要求 PMIx 同时开启
且 pkg-config 能找到 UCX 1.12 或更新版本。仅安装 UCX 不会自动改变构建功能。

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
### 节点身份、连接地址与 TLS

PMIx 使用[节点配置](config.md)中已解析的 `name`（规范 CranedId）、`NodeHostname` 和 `NodeAddr`。
本机身份由 Craned 传入 Supervisor；端口交换、peer 查找和 tree/ring 成员匹配均使用规范 ID，
不再把系统 `gethostname()` 的返回值直接当作节点 ID。PMIx 对客户端公布的主机名和节点映射使用 `NodeHostname`。

gRPC 连接目的地为 `NodeAddr`，启用 TLS 时按 `NodeHostname` 校验证书，与控制端连接 Craned 的规则一致。
未配置 `NodeHostname` 时使用 `name`；未配置 `NodeAddr` 时使用 `NodeHostname`，由现有配置解析统一补齐。
`NodeAddr` 不作为节点别名。PMIx 不拼接 `TLS.DomainSuffix`，也不要求逻辑 ID 能被 DNS 解析。
证书需覆盖实际配置的 `NodeHostname`；非 TLS 路径仍使用相同的连接地址映射。

通过在作业脚本中设置环境变量，可以调整 CraneSched PMIx 服务端的行为。
请在 `crun` 命令前导出这些变量，并确保其 `--export` 策略包含它们。完整输入列表、注入任务的变量和节点列表作用域见[作业环境变量](../../reference/job_env_vars.md)。

### 4.1 集合通信算法（Fence 类型）
PMIx fence（屏障同步）支持两种集合算法：

| 值 | 算法	| 适用场景|
|---|----|-----|
tree | 树形聚合 | 节点数较多时效率更高 |
ring | 环形传递 | 节点数较少时延迟更低 |

`CRANE_PMIX_FENCE` 未设置或不是 `tree`、`ring` 时，fence 回调在收集非空数据时选择 `ring`，其他情况选择 `tree`，并非始终默认 `tree`。算法性能还取决于具体工作负载与网络。

**使用环形算法（适合小规模作业）**
```bash
export CRANE_PMIX_FENCE=ring
crun --nodes=4 --ntasks-per-node=4 --mpi=pmix ./my_program
```

**显式选择树形算法：**
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
**该选项需要 CraneSched 设置 `CRANE_ENABLE_UCX=ON` 并找到兼容 UCX。若 CraneSched 未编译 UCX 支持，任务启动失败。**
仅精确值 `true` 启用该模式，其他值使用 gRPC。

### 4.3 超时与客户端临时目录

- `CRANE_PMIX_TIMEOUT`：PMIx 操作及集合通信清理的超时秒数，默认 `5`。应使用正整数；空值或转换失败时保留默认值。
- `PMIXP_PMIXLIB_TMPDIR`：可选的客户端文件基础目录，须对作业用户可访问。CraneSched 在其后添加 `pmix.crane.cli/spmix_appdir_<uid>.<job>.<step>`；未设置或为空时使用 PMIx 默认目录。

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
export CRANE_PMIX_DIRECT_CONN_UCX=true
# 此开关选择 PMIx 元数据传输；MPI 点对点通信由 MPI 自身的传输配置决定

crun --nodes=64 --ntasks-per-node=16 --mpi=pmix \
     ./large_scale_sim -config run.cfg

echo "Simulation completed at $(date)"
```

## 六、常见问题与故障排查

**Q1：提交时提示 `PMIx support is not compiled in.`**

原因：当前集群的 CraneSched 编译时设置了 `CRANE_ENABLE_PMIX=OFF`，或使用了未启用 PMIx 的旧产物。

解决方法：联系集群管理员，按[配置 CraneSched PMIx 支持](#14-配置-cranesched-pmix-支持)重新构建。
