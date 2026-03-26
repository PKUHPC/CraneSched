# 鹤思 vs Slurm 功能对比

鹤思（CraneSched）是国内首个支持超智算领域的信创开源算力调度系统，在调度功能上全面对标国际主流系统 Slurm，并在性能、容器支持、国产化适配等多个维度实现超越。

---

## 性能对比 { #性能对比 }

鹤思在调度性能上显著优于 Slurm 和 OpenPBS，实测数据如下：

### 平均每分钟作业调度数

| 调度系统 | 平均每分钟调度数 | 相对鹤思倍率 |
|---------|:------------:|:---------:|
| **鹤思** | **105,538** | **1x** |
| OpenPBS | 11,136 | 9.4x 慢 |
| Slurm | 4,259 | 24.7x 慢 |

### 平均每分钟峰值调度数

| 调度系统 | 峰值每分钟调度数 | 相对鹤思倍率 |
|---------|:-------------:|:---------:|
| **鹤思** | **122,427** | **1x** |
| OpenPBS | 20,541 | 6x 慢 |
| Slurm | 4,551 | 26.9x 慢 |

### 关键性能指标

| 指标 | 鹤思 |
|------|------|
| 调度性能 | 较 Slurm 提升 **5-20 倍** |
| 集群规模 | 支持节点 **10 万+** |
| 作业吞吐 | 实时调度 **1 万+/秒**，小时级任务吞吐量超 **3800 万** |
| 并发能力 | 作业并行 **200 万+** |
| 交互体验 | 响应 **毫秒级** 低延迟 |

---

## 调度功能对比

| 功能特性 | 鹤思 | Slurm | 说明 |
|---------|:---:|:-----:|------|
| **基础调度** | | | |
| 回填调度（Backfill） | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 利用空闲时间窗口运行短作业，提高资源利用率 |
| 公平共享调度（Fair Share） | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 基于历史使用量的公平调度策略 |
| 优先级调度 | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 多因子优先级计算 |
| FIFO 调度 | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 先进先出基础调度 |
| **资源管理** | | | |
| 资源抢占（Preemption） | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 高优先级作业抢占低优先级作业资源 |
| 资源预留（Reservation） | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 为特定用户或作业预留资源时间窗口 |
| TRES 资源细粒度追踪 | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 可追踪的资源类型（CPU、内存、GPU 等） |
| QOS 服务质量管理 | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 差异化服务等级控制 |
| 资源逃逸保护 | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 防止作业超出分配资源 |
| **作业管理** | | | |
| 作业依赖（Job Dependencies） | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 作业间依赖关系控制 |
| 作业数组（Job Array） | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 批量提交参数化作业 |
| 作业步（Job Steps） | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 作业内多步骤管理 |
| 交互式作业 | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 实时交互计算 |
| **节能与效率** | | | |
| 节能调度（Power Saving） | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 低负载时自动关闭空闲节点 |
| AI 作业时间预测（ORA） | :material-check-circle:{ .success } | :material-close-circle:{ .danger } | 基于大语言模型的作业运行时间预测，准确率提升 41% |
| 智能公平共享（TSMF） | :material-check-circle:{ .success } | :material-close-circle:{ .danger } | 自研两阶段多因子算法，利用率提升至 97.3% |
| 自动化节能（EcoSched） | :material-check-circle:{ .success } | :material-close-circle:{ .danger } | 自动化电源控制，低负载能耗降低 78.64% |
| **账户与权限** | | | |
| 层级账户管理 | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 树状层级用户/账户管理 |
| RBAC 权限控制 | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 基于角色的访问控制 |
| **高可用** | | | |
| 自动故障恢复 | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 控制节点故障后自动恢复 |
| 分布式容错 | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 无单点故障设计 |

---

## 容器支持对比

鹤思和 Slurm 在容器支持方面采用了不同的技术路线：

| 对比维度 | 鹤思 | Slurm |
|---------|------|-------|
| **技术路线** | 基于 K8s 底层 CRI RPC 接口（事实上的云原生标准） | 基于底层 OCI 模型兼容，使用 CLI 接口 |
| **容器运行时** | containerd / CRI-O（通过 CRI 接口） | Singularity / Enroot（通过 CLI） |
| **镜像管理** | 自动拉取，无需用户手动处理 | 用户需自行下载和处理镜像格式 |
| **专用 CLI** | ccon 命令，借鉴 Docker CLI 设计，易于上手 | 使用 Slurm 原生命令（sbatch/srun），与 Docker 使用方式不同 |
| **网络隔离** | 支持 CNI，多租户网络隔离（Calico Underlay） | 几乎不支持 |
| **文件系统隔离** | 完整的 user/network/mount namespace 隔离 | 有限 |
| **Fake Root** | 基于 User Namespace，支持容器内 Root 体验 | 依赖 Singularity 的 fakeroot |
| **RDMA 支持** | 支持 SR-IOV 共享 RNIC 和直接透传 | 有限 |
| **运维工具** | crictl/nerdctl/ctr 等成熟工具 | 依赖社区工具 |

### 鹤思容器的独特优势

- **易用性高**：不用手动拉镜像；有独立的 CLI（ccon），用过 Docker 但没接触过 Slurm 的人容易上手
- **网络隔离彻底**：支持 CNI，管理员可以实现各种容器组网策略，包括多租户网络隔离
- **RDMA 网络支持**：支持中小规模 RoCE 网络（SR-IOV）和大规模 AI 训练集群（Spine-Leaf 架构）
- **Pod/Job 概念映射**：将 K8s 的 Pod/Container 概念映射到 Job/Step，实现命令式编排

---

## 命令兼容性 { #命令兼容性 }

鹤思自研了 Slurm & LSF Wrapper，完全兼容 Slurm 和 LSF 命令行语法：

| Slurm 命令 | 鹤思原生命令 | 功能 |
|-----------|----------|------|
| `sbatch` | `cbatch` | 提交批处理作业 |
| `squeue` | `cqueue` | 查看作业队列 |
| `srun` | `crun` | 运行交互式作业 |
| `salloc` | `calloc` | 分配交互式资源 |
| `sinfo` | `cinfo` | 查看集群信息 |
| `sacct` | `cacct` | 查看作业历史 |
| `sacctmgr` | `cacctmgr` | 账户管理 |
| `scancel` | `ccancel` | 取消作业 |
| `scontrol` | `ccontrol` | 系统管理控制 |

**零迁移成本**：通过 Slurm Wrapper，用户无需修改任何脚本或操作习惯，即可从 Slurm 无感知切换至鹤思。北京大学未名教学二号集群已成功实现从 Slurm 到鹤思的无感知迁移，支持数百种用户软件。

---

## 异构硬件支持 { #异构硬件支持 }

鹤思全面适配国内外主流硬件平台：

### 架构支持

| 架构 | 支持情况 |
|------|---------|
| X86 | :material-check-circle:{ .success } |
| ARM | :material-check-circle:{ .success } |
| RISC-V | :material-check-circle:{ .success } |

### CPU 适配

| 类别 | 支持品牌 |
|------|---------|
| 国外 | Intel、AMD |
| 国内 | 飞腾、曙光、华为鲲鹏 |

### 加速卡适配

| 类别 | 支持品牌 |
|------|---------|
| 国外 | Nvidia GPU、AMD GPU |
| 国内 | 华为昇腾、海光、寒武纪 MLU、天数智芯、昆仑芯、沐曦、摩尔线程 |

### 操作系统适配

| 类别 | 支持系统 |
|------|---------|
| 国外 | CentOS、Ubuntu、Rocky Linux |
| 国内 | OpenEuler、银河麒麟 |

鹤思已获得浪潮、飞腾、海光、昆仑芯等多家厂商的产品兼容性互认证书。

---

## 总结

| 维度 | 鹤思优势 |
|------|---------|
| **性能** | 较 Slurm 提升 5-20 倍 |
| **功能** | 全面覆盖 Slurm 功能，额外支持 AI 预测、智能节能等 |
| **容器** | 原生 CRI/CNI 支持，多租户网络隔离，RDMA 支持 |
| **兼容** | 完全兼容 Slurm/LSF 命令，零迁移成本 |
| **国产化** | 全面适配国产 CPU、GPU/NPU、操作系统 |
| **融合** | 超算智算一体化，存·算·用彻底融合 |
