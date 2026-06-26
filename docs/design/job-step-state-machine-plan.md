# Job/Step 状态机文档执行计划

本文记录 Job 与 Step 状态机文档的拆分方案、执行顺序和协作边界。目标是为
Feishu 内部文档产出稳定的写作契约，避免 Job、Step、Array、Recovery 和
Requeue 的责任边界在不同文档中重复或冲突。

## 文档集

最终文档拆成三份：

| 本地草稿 | Feishu 文档定位 | 责任 |
|---|---|---|
| `docs/design/job-step-state-machine-overview.md` | Job/Step 状态机总览与写作契约 | 定义统一术语、事件模型、跨文档边界、图形规范和入口导航 |
| `docs/design/job-state-machine.md` | Job 状态机 | 描述 `JobInCtld` 的状态、调度、资源、持久化、array 聚合、requeue 和 recovery |
| `docs/design/step-state-machine.md` | Step 状态机 | 描述 daemon/common/primary step 的状态、Craned 状态上报、cleanup、terminal 汇总和与 Job 的交接 |

这三份文档不进入 `mkdocs.yaml`，除非后续明确要发布到官方文档站。当前目标是
Feishu 内部架构说明。

## 执行顺序

### M0: 总览骨架和写作契约

产物：

- `job-step-state-machine-plan.md`
- `job-step-state-machine-overview.md`
- `job-state-machine.md` 占位骨架
- `step-state-machine.md` 占位骨架

M0 只固定写法和边界，不展开完整 Job/Step 状态机正文。

### M1: Job 状态机草稿

输入：

- `job-step-state-machine-overview.md`
- `job-state-machine.md`
- `src/CraneCtld/JobScheduler.cpp`
- `src/CraneCtld/CtldPublicDefs.{h,cpp}`
- `src/CraneCtld/Array.{h,cpp}`
- `src/CraneCtld/Database/EmbeddedDbClient.{h,cpp}`

重点：

- `JobInCtld` 的状态主体和持久化字段。
- submit/recovery 到 pending 的入口。
- pending 到 running/configuring 的调度路径。
- Step terminal event 如何被 Job final path 消费。
- 普通结束、cancel、deadline、manual requeue、automatic requeue。
- array parent/child 的聚合和恢复。
- runtime path 与 recovery path 的差异。
- pending/running map、array meta、EmbeddedDB/MongoDB 写入顺序和失败收敛。

### M2: Step 状态机草稿

输入：

- `job-step-state-machine-overview.md`
- `step-state-machine.md`
- `src/CraneCtld/CtldPublicDefs.{h,cpp}`
- `src/CraneCtld/JobScheduler.cpp`
- `src/Craned/Core/JobManager.cpp`
- `src/Craned/Core/CtldClient.cpp`

重点：

- `DaemonStepInCtld`、`CommonStepInCtld`、primary step 的状态主体。
- Craned 本地事件如何进入 `StepStatusChange`。
- Configuring/Running/Completing/terminal 的守卫条件和副作用。
- daemon cleanup 与 primary/common cleanup 的分工。
- Step 只生产 job final event，不直接执行 job archive/requeue。
- synthetic/internal event 与真实 Craned 上报分开描述。

### M3: 交叉一致性复查

检查项：

- Job 文档是否引用 Step terminal event，但没有重写 Step 内部状态机。
- Step 文档是否描述 terminal 汇总，但没有重写 Job final/requeue/archive。
- Recovery 是否显式区分 runtime path。
- Array child 和 parent 的责任是否只在 Job 文档主讲。
- Mermaid 图是否可以单独复制到 Feishu。
- Job/Step 两篇是否没有重复定义同一条 transition。
- 每条 transition 是否覆盖入站 RPC/event、锁和 map ownership、DB 更新顺序、
  出站 action、执行模式和可观测结果。

### M4: 总览文档补全

在 Job/Step 两篇稳定后，回填总览文档：

- 补最终导航图。
- 补 Job/Step handshake 图。
- 补状态词表中的跨文档引用。
- 补已确认的未决问题和已知边界。
- 确认 Overview 没有重新解释完整 Job 或 Step 状态机。

### M5: 图形导出和 Feishu 集成

产物：

- Mermaid 源码保留在 Markdown 中。
- 如 Feishu Mermaid 渲染不稳定，导出 SVG/PNG 附件。
- 每个图必须有稳定标题和源文档位置，便于后续代码变更后重画。

## Agent / Session 切分建议

建议开独立 agent 或 session 时按写入范围拆分，避免同一文件冲突：

| 角色 | 写入范围 | 任务 |
|---|---|---|
| Overview owner | `job-step-state-machine-overview.md` | 维护术语、边界、导航和最终汇总 |
| Job owner | `job-state-machine.md` | 写 Job 状态机正文和 Job 图 |
| Step owner | `step-state-machine.md` | 写 Step 状态机正文和 Step 图 |
| Reviewer | 只读，或输出 review 文件 | 查找边界冲突、遗漏路径和图文不一致 |
| Diagram owner | 只写图片导出目录 | Mermaid 导出 SVG/PNG，并检查 Feishu 可读性 |

如果多个 agent 并行工作，必须先阅读 `job-step-state-machine-overview.md`，
并遵守其中的状态主体、事件模型和单条 transition 模板。

## 跨文档引用规则

- `JobStatus` 是共享枚举词表，不代表 Job 和 Step 共用同一个状态机。
- Job 文档可以引用 Step 文档的 terminal event，但不展开 daemon/common step 的内部
  guard。
- Step 文档可以引用 Job 文档的 final path，但不展开资源释放、DB archive、
  requeue、dependency event 和 array parent finalize。
- Recovery 是独立视角。文档必须说明某个 transition 是 runtime path、recovery
  path，还是两者都适用。
- Array parent 聚合由 Job 文档主讲；Step 文档只说明 primary/daemon terminal
  如何产生可供 Job 聚合的输入。

## 完成标准

每个正式文档完成时至少满足：

- 每个 transition 都说明状态主体、存储状态、事件来源、处理入口、
  入站 RPC/event、锁和 map ownership、副作用、出站 RPC/action、
  执行模式、持久化写入、DB 更新顺序、幂等性、
  过期/乱序事件处理、失败收敛和可观测结果。
- 每个图都能独立解释一个层次，不用单张巨图覆盖所有路径。
- 每篇正式文档最多一张主状态图，副作用用 sequence 图或表格展开。
- runtime 与 recovery 不混写。
- 涉及代码入口时使用当前分支的文件路径和函数名。
- 对不确定或依赖后续验证的行为标注为未决问题，而不是写成已确认语义。
