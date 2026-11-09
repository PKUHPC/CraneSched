# PMI-2 直接启动

`crun --mpi=pmi2` 使用每个 step 的 Supervisor 提供 PMI-2 服务，直接启动 MPI rank。
PMIx 仍通过 `--mpi=pmix` 选择。协议取决于 MPI 的实际构建配置，不能仅根据厂商或版本名推断。

## 构建与安装

`CRANE_ENABLE_PMI2` 默认 ON，与 `CRANE_ENABLE_PMIX` 独立。关闭时提交 PMI-2 作业或 step 会在准入阶段报错。
控制端、计算端和 `crun` 应部署支持该协议的同一产品版本；新增内部 RPC 不代表旧版本支持 PMI-2。

`craned` 软件包包含 CraneSched 自有的 PMI-2 客户端。构建会生成只导出标准 PMI-2 接口的 `libpmi2.so`，安装在
`${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}/crane/pmi2/`。
例如 `/usr` 前缀和 `lib64` 目录对应 `/usr/lib64/crane/pmi2/libpmi2.so`。
头文件在 `${prefix}/include/crane/pmi2/pmi2.h`。该库使用独立的私有目录，无需替换系统 PMI 库，也不依赖 Slurm 守护进程。

## 使用

先用所选 MPI 实现的 `mpicc` 编译，并确保每个计算节点都具有匹配的 MPI 运行库。
随后直接提交应用：

```bash
crun -N 2 -n 4 --ntasks-per-node=2 --mpi=pmi2 ./mpi_application
```

Intel MPI 通过 `I_MPI_PMI_LIBRARY` 指向产品安装的 **PMI-2** 客户端，例如：

```bash
export I_MPI_PMI_LIBRARY=/usr/lib64/crane/pmi2/libpmi2.so
crun -N 2 -n 4 --ntasks-per-node=2 --mpi=pmi2 ./intel_mpi_application
```

`--mpi=pmi2` 选择的是 CraneSched 的 PMI-2 服务端协议，不会识别 MPI 厂商，也不会自动设置
`I_MPI_PMI2` 或 `OMPI_MCA_ess`。Intel MPI 2017/2018 还需要显式选择 PMI-2 路径；WM1 测试镜像通过
Environment Modules 提供 `intel-pmi2` profile，设置 `I_MPI_PMI2=1` 和匹配的客户端库。Open MPI 4
的 PMI-2 构建通过 `openmpi-pmi2` profile 设置 `OMPI_MCA_ess=pmi`。每次更换 MPI 实现前执行
`module purge`，测试后卸载当前 module；不要把两个实现的库路径混合到同一 shell。

必须对实际安装的 Intel MPI 版本、客户端加载路径和网络 transport 做端到端验证。
MPICH 需要以 PMI-2 构建。Open MPI 4 的 PMI 直接启动组件需要在构建时包含 PMI-2，
必要时通过 `OMPI_MCA_ess=pmi` 选择；仅含 PMIx 的发行版构建应继续使用 `--mpi=pmix`。
MPI SDK 的库搜索路径由应用部署负责，不要混用不同实现的 `libmpi`。

可以在同一批作业内依次执行带不同 `--mpi` 选择的 `crun` step；每个 step 的 KVS、rank、fd 和端点独立。
不要再把应用包在 `mpirun`、Hydra 或 PRRTE 中。本功能不提供这些启动器的调度器集成。

## 支持范围

支持静态 world 的 Init/Finalize、Job ID、KVS Put/Get/Fence、进程映射、节点属性和必要的 job 属性。
应用通过继承的 `PMI_FD` 连接本地服务；rank 到节点的映射来自实际 step 分配。
Fence 在有任务的节点间使用树形 TCP/gRPC 交换，沿用集群的地址和 TLS 配置。
`CRANE_PMIX_FENCE`、`CRANE_PMIX_DIRECT_CONN_UCX` 只控制 PMIx，不改变 PMI-2。

PMI-1、动态 Spawn/Connect、跨作业 KVS 和名称服务不在支持范围内，未实现的请求返回明确错误。
协议初始化、未完成请求和 fence 的默认超时为 60 秒。断连、超时及 Abort 触发当前 step 的失败传播和回收。
PMI-2 控制通道使用 TCP，不规定 MPI 数据通道；TCP 功能通过不等于 RDMA 或跨物理节点验收。
