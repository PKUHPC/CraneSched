---
hide:
  - navigation
---

# 鹤思

<p class="lead">新一代超智融合算力调度系统 — 国内首个超智融合信创开源算力调度系统</p>

<p>鹤思（CraneSched）是一款面向高性能计算（HPC）和人工智能（AI）工作负载的分布式作业调度系统。它在统一平台上整合超算与智算算力资源，为教育、工业、气象等各行各业提供高效、稳定、可靠的调度服务。</p>

<p>本项目由北京大学、北京大学长沙计算与数字经济研究院和长沙尖山塔图科技有限公司联合自主研发。</p>

[快速开始](deployment/index.md){ .md-button .md-button--primary } [体验演示](https://hpc.pku.edu.cn/demo/cranesched){ .md-button } [GitHub](https://github.com/PKUHPC/CraneSched){ .md-button }

---

## 为什么选择鹤思?

<div class="grid cards" markdown>

- :material-speedometer: **性能指标国际领先**

    ---

    调度性能较 Slurm 提升 5-20 倍，每秒实时调度超 1 万个任务，同时运行 200 万以上任务，任务分发延迟低于 10 毫秒。支持超 10 万节点算力规模。

    [:octicons-arrow-right-24: 查看性能对比](features/slurm-comparison.md#性能对比)

- :material-merge: **超算智算深度融合**

    ---

    一套集群同时具备超算与智算能力，满足 HTC+HPC+AI 全部计算业务场景。算力、存储、数据资源池化，实现高效共享，告别资源孤岛。

    [:octicons-arrow-right-24: 了解融合方案](features/convergence-solution.md)

- :material-chip: **统一异构算力纳管**

    ---

    支持 X86、ARM、RISC-V 等架构，适配 Intel、AMD、飞腾、鲲鹏等 CPU，以及 Nvidia、AMD GPU 和华为昇腾、寒武纪、昆仑芯等加速卡。

    [:octicons-arrow-right-24: 查看兼容清单](features/slurm-comparison.md#异构硬件支持)

- :material-brain: **智能算法提效降耗**

    ---

    自研 ORA 作业时间预测算法（发表于 CCF-B 会议 ICS），预测准确率提升 41%；自研 TSMF 算法显著改善资源利用率；自研节能算法实测低负载时集群能耗降低 78.64%。

    [:octicons-arrow-right-24: 了解调度算法](features/convergence-solution.md#自研调度算法)

- :material-swap-horizontal: **完全兼容 Slurm/LSF 命令**

    ---

    自研 Slurm & LSF Wrapper，用户无需修改脚本或操作习惯，实现零迁移成本无感知切换。

    [:octicons-arrow-right-24: 查看兼容性](features/slurm-comparison.md#命令兼容性)

- :material-monitor-dashboard: **鹤思 + SCOW 一体化方案**

    ---

    与自研算力平台 SCOW 深度集成，提供从资源管理、任务调度到监控计费的全生命周期闭环管理，打造"超·智·量·云"一体化算力服务。

    [:octicons-arrow-right-24: 了解一体化方案](features/scow-integration.md)

</div>

---

## 解决方案

<div class="grid cards" markdown>

- :material-database-sync: **存·算·用融合超智算方案**

    ---

    传统方案中超算与智算独立建设，算力、存储、数据难以共享，形成资源孤岛。鹤思超智一体化融合方案实现：

    - **算力融合**：一套集群同时具备超算与智算能力
    - **存储融合**：超智统一存储，数据资源池化共享
    - **使用融合**：统一平台简化运维，统一用户认证与资源管理

    [:octicons-arrow-right-24: 详细了解](features/convergence-solution.md)

- :material-application-brackets: **鹤思 + SCOW 算力一体化方案**

    ---

    鹤思与自研算力平台系统 SCOW（Super Computing On Web）深度集成，形成完整的算力中心解决方案：

    - **运营管理**：计费收费、用户管理、账户管理、身份认证、权限管理
    - **资源使用**：在线作业提交、Shell 平台、可视化桌面、跨集群文件传输
    - **资源管理**：资源虚拟化、资源授权、资源配置

    [:octicons-arrow-right-24: 详细了解](features/scow-integration.md)

</div>

---

## 功能完备性

鹤思在调度功能上全面对标 Slurm，并在多项关键能力上实现超越。

| 功能特性 | 鹤思 | Slurm | LSF | 说明 |
|---------|:---:|:-----:|:---:|------|
| 回填调度（Backfill） | :material-check-circle:{ .success } | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 利用空闲时间窗口运行短作业，提高资源利用率 |
| 公平共享调度（Fair Share） | :material-check-circle:{ .success } | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 基于历史使用量的公平调度策略 |
| 资源抢占（Preemption） | :material-check-circle:{ .success } | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 高优先级作业抢占低优先级作业资源 |
| 资源预留（Reservation） | :material-check-circle:{ .success } | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 为特定用户或作业预留资源时间窗口 |
| 节能调度（Power Saving） | :material-check-circle:{ .success } | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 低负载时自动关闭空闲节点 |
| TRES 资源细粒度追踪 | :material-check-circle:{ .success } | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 可追踪的资源类型（CPU、内存、GPU 等） |
| 作业依赖（Job Dependencies） | :material-check-circle:{ .success } | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 作业间依赖关系控制 |
| 作业数组（Job Array） | :material-check-circle:{ .success } | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 批量提交参数化作业 |
| QOS 服务质量管理 | :material-check-circle:{ .success } | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 差异化服务等级控制 |
| 容器原生编排（CRI/CNI） | :material-check-circle:{ .success } | :material-check-circle:{ .success } | :material-check-circle:{ .success } | 基于 CRI/CNI 标准的原生容器编排 |
| 多租户容器网络隔离 | :material-check-circle:{ .success } | :material-close-circle:{ .danger } | :material-close-circle:{ .danger } | 支持 CNI 多租户网络隔离（Calico Underlay） |
| 容器 RDMA 网络支持 | :material-check-circle:{ .success } | :material-close-circle:{ .danger } | :material-close-circle:{ .danger } | 支持 SR-IOV 共享 RNIC 和直接透传 |
| 国产化信创全面适配 | :material-check-circle:{ .success } | :material-close-circle:{ .danger } | :material-close-circle:{ .danger } | 全面适配国产 CPU、GPU/NPU、操作系统 |
| 超智算一体化融合调度 | :material-check-circle:{ .success } | :material-close-circle:{ .danger } | :material-close-circle:{ .danger } | 一套集群同时具备超算与智算能力 |
| AI 作业时间预测 | :material-check-circle:{ .success } | :material-close-circle:{ .danger } | :material-close-circle:{ .danger } | 基于大语言模型的作业运行时间预测，准确率提升 41% |

[:octicons-arrow-right-24: 查看完整功能对比](features/slurm-comparison.md){ .md-button }

---

## 应用场景

鹤思系统适用于多种计算场景：

<div class="grid cards" markdown>

- :material-weather-cloudy: **传统超算**

    ---

    空气动力学模拟、大气模拟、高能物理研究等。支持 WRF、OpenFOAM、CMAQ、CESM、ABAQUS、GROMACS 等主流应用。

- :material-robot: **智能计算**

    ---

    支持 DeepSeek、Qwen、Llama、CPMBee、ChatGLM 等大模型的高效训练和推理。支持 Docker、Singularity 等多种容器运行环境。

- :material-integrated-circuit-chip: **芯片设计**

    ---

    支持 EDA 芯片设计等对调度系统吞吐量要求极高的应用场景，深度适配 Cadence、Synopsys 等主流 EDA 工具。

- :material-flask: **科学研究**

    ---

    大数据分析、生物医药设计、电池材料研究、医学大模型等科研场景。

</div>

---

## 应用案例

鹤思已在全国 **8 个省市**、**10+ 个算力中心** 部署使用。

<div class="grid cards" markdown>

- :material-school: **北京大学未名教学二号集群**

    ---

    2024 年 6 月上线，支撑师生实时在线教学科研，支撑 300+ 课时在线教学，兼容数百种用户软件。从 Slurm 无感知切换至鹤思，系统稳定运行至今。

- :material-brain: **北京大学未名卓越一号集群**

    ---

    2024 年 11 月上线，全国产华为昇腾、鲲鹏架构，国内高校中率先使用国产超智算一体集群方案。运行 DeepSeek、Qwen、Llama 等大模型训推任务。

</div>

**更多部署单位**：中科院软件所、天津大学、北京联合大学、南京航空航天大学、贵州财经大学、中国海洋大学等。

**荣誉**：入选 2024 年工信部"典型应用案例"及"重点推荐应用案例"、"2024 年教育信息技术应用创新优秀案例集"。

---

## 技术特点

<div class="grid cards" markdown>

- :material-speedometer: **高性能**

    ---

    每秒超过 10 万次调度决策，快速的作业资源匹配。

- :material-arrow-expand: **可扩展性**

    ---

    经过验证的设计，支持百万核心集群和大规模部署。

- :material-account-cog: **易用性**

    ---

    为用户和管理员提供简洁一致的命令行界面（cbatch、cqueue、crun、calloc、cinfo 等）。

- :material-shield-lock: **安全性**

    ---

    内置基于角色的访问控制（RBAC）和加密通信，源代码开源开放，系统完全自主可控，符合信创安全标准。

- :material-heart-pulse: **弹性**

    ---

    自动作业恢复，无单点故障，快速状态恢复。分布式容错设计，稳定可靠。

- :material-source-repository: **开源**

    ---

    基于 AGPLv3 开源，社区驱动，具有可插拔架构的可扩展性。

</div>

---

## 命令行参考

- 用户命令：[cbatch](command/cbatch.md)、[cqueue](command/cqueue.md)、[crun](command/crun.md)、[calloc](command/calloc.md)、[cinfo](command/cinfo.md)
- 管理员命令：[cacct](command/cacct.md)、[cacctmgr](command/cacctmgr.md)、[ceff](command/ceff.md)、[ccontrol](command/ccontrol.md)、[ccancel](command/ccancel.md)
- 容器命令：[ccon](command/ccon.md)
- 退出代码：[参考](reference/exit_code.md)

---

## 链接

- 演示集群：<https://hpc.pku.edu.cn/demo/cranesched>
- 后端仓库：<https://github.com/PKUHPC/CraneSched>
- 前端仓库：<https://github.com/PKUHPC/CraneSched-FrontEnd>
- SCOW 算力平台：<https://github.com/PKUHPC/OPENSCOW>

---

## 许可证

鹤思采用 AGPLv3 和商业许可证双重许可。有关商业许可，请查看 `LICENSE` 文件或联系 mayinping@pku.edu.cn。

---
