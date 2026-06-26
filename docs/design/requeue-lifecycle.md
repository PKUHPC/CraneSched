# Requeue Lifecycle and State Machine Design

本文记录 `dev/requeue` 分支中 requeue 的当前生命周期。目标是帮助排查
`ccontrol requeue`、失败自动重排、ctld 重启恢复、以及 craned cleanup 之间的
状态流转问题。

## Scope

本文只描述当前实现，不定义新的行为。重点覆盖：

- 用户请求如何进入 ctld。
- requeue intent 如何持久化。
- job/step 状态机何时触发 requeue。
- `requeue_requested`、`requeue_if_failed`、`requeue_count` 如何流转。
- requeue 与 daemon/primary step cleanup 的关系。
- 当前已知边界和风险。

## User Entry Points

### Manual Requeue

手动 requeue 通过 `RequeueJob` RPC 进入 ctld：

- `protos/Crane.proto` 定义 `RequeueJobRequest` 和 `RequeueJobReply`。
- `RequeueJobRequest` 包含 `operator_uid` 和 `job_ids`。
- `RequeueJobReply` 将结果拆成 `requeued_jobs` 和 `not_requeued_jobs`。
- `CraneCtldServiceImpl::RequeueJob` 做 ctld RPC 入口检查后调用
  `JobScheduler::RequeueJob`。

当前 `JobScheduler::RequeueJob` 的行为：

```text
ccontrol requeue <job_ids>
  -> RequeueJobRequest(operator_uid, job_ids)
  -> CtldGrpcServer::RequeueJob
  -> JobScheduler::RequeueJob
      -> 只在 running job map 中查 job
      -> 校验 operator uid 是否有权限
      -> 拒绝 Interactive job
      -> 设置 job.requeue_requested=true
      -> Configuring job 同时设置 cancel_requested=true
      -> 先写 EmbeddedDB runtime attr
      -> Running job 终止 primary step
      -> 后续由 step/job final path 完成真正 requeue
```

也就是说，手动 requeue 的 RPC 返回成功只表示 requeue intent 已经记录并开始终止
当前运行，不表示 job 已经重新入 pending queue。

### Automatic Requeue on Failure

自动失败 requeue 不经过 `RequeueJob` RPC。它由 job final path 根据
`JobInCtld::ShouldRequeue()` 决定：

```text
step reports terminal status
  -> common/daemon step 状态机汇总 step final status
  -> job final path 更新 job status/end_time/exit_code
  -> JobInCtld::ShouldRequeue()
  -> should_requeue=true 时进入 requeue path
```

当前判断规则：

- 非 Batch job 不 requeue。
- `requeue_requested=true` 时 requeue。
- `job_to_ctld.no_requeue=true` 时不 requeue。
- `exit_code` 为 `EC_CRANED_DOWN` 或 `EC_RPC_ERR` 时自动 requeue。
- `requeue_if_failed=true` 且 job status 不是 `Completed`/`Cancelled` 时 requeue。

## Array Requeue Semantics

当前 requeue 只支持单个已 materialized array child，不支持直接 requeue 整个
array parent。

这里的“单个任务”指已经生成出独立 `job_id` / `job_db_id` 的 array child。该 child
已经是一个真实的 `JobInCtld` 运行实体，可以复用普通 Batch job 的 requeue final
path：

```text
array child final path
  -> archive current child run to MongoDB
  -> reset the same child job record
  -> keep array task identity
  -> put the child back to pending queue
  -> wait for scheduler to run this child again
```

当前不支持的场景：

- `requeue <array_parent_id>` 展开并重排整个 array。
- 复活已经完成并归档的 array child。
- 直接 requeue 尚未 materialized 的 array task。
- 使用 array range selector 做批量 requeue。

这与现有 array 生命周期冲突较小，原因是 materialized child 本来就不是虚拟任务，而是
一个独立 job record。requeue 只发生在这个 child 自己的 attempt 上，不需要重新设计
parent 的 materialization 顺序，也不需要让 parent 重新生成已经存在的 task。

需要避免的关键点是：requeued child 不能被当成真正 terminal 的 child 通知
`ArrayManager::OnChildTerminal()`。它虽然退出了 running map，但在再次完成之前仍是
array 中未终结的 materialized task。如果此时减少 parent 的 running/incomplete 计数，
parent 可能提前 finalize。

自动失败 requeue 也遵循 task-local 规则：失败的是哪个 array child，就只 reset/requeue
这个 child。array parent 的 aggregate state 只用于展示、依赖、归档和最终汇总，不用于
把某个 child failure 扩大成 whole-array requeue。

## Finalization Path

requeue 的实际执行发生在 job final processing 中，而不是在 step 状态机中直接入队。

当 job 进入 final path 后，scheduler 会先收集 craned 上需要释放的 job，然后计算
`should_requeue`。如果需要 requeue：

```text
job final path
  -> FreeResourceFromNode / FreeResourceFromResv
  -> FreeQosRunningResource
  -> FreeLicense
  -> 不触发 dependency events
  -> 将 job 放入 context.requeue_jobs
  -> 从 running job map 移除
  -> PersistAndRequeueJobs_
      -> Insert current run into MongoDB
      -> ResetJobStepIdCounter in EmbeddedDB
      -> JobInCtld::ResetForRequeue()
      -> UpdateRuntimeAttrOfJob in EmbeddedDB
      -> 达到 MaxRequeueCount 时 set held
      -> 放回 pending job map
```

与普通完成路径相比，requeue path 的关键区别是：

- 只释放 running QoS 资源，保留 submit count。
- 释放 license。
- 不触发 dependency events。
- 当前 run 会先归档，再 reset 成一个新的 pending run。

## Job and Step State Machines

### Daemon Step

`DaemonStepInCtld::StepStatusChange` 负责 daemon step 的状态汇总。

主要流转：

```text
Configuring
  -> Running
      条件：所有 daemon nodes configured 且 ctld prolog complete
      动作：创建 primary step，并向 craned 分发 primary step allocation

Configuring
  -> Failed/Cancelled
      动作：记录 error status，触发 daemon cleanup，不创建 primary step

Running/Completing
  -> Completing
      条件：收到 daemon node completing
      动作：所有 daemon nodes completing 后向 craned 发 FreeSteps

Completing
  -> terminal status
      条件：所有 daemon nodes cleanup finished
      动作：release daemon step；若 primary step 未创建，daemon final status 成为 job final status
```

Daemon step 的 cleanup 是 job 环境清理的关键点。craned 侧在释放 daemon step 时保存
job cgroup 和 epilog env，等待 supervisor 退出后清理 job environment，然后才可能
向 ctld 回传 daemon terminal status。

正常 daemon+primary/common step 状态机假设 daemon step 是最后的 job-level cleanup
门闩：primary/common step terminal 后，primary 分支触发 daemon `FreeSteps`；daemon
terminal 后释放 daemon step；此时 `AllStepsFinished()` 才能让 job 进入 final path。
这里的“正常”指没有外部因素绕过状态机直接 kill daemon supervisor，也没有 ctld/craned
recovery 期间的视图不一致。这个 cleanup 假设不只属于 Batch；Interactive/Container
等只要走 daemon+primary/common step 模型，也应遵守同样的 job-level cleanup 顺序。

### Primary/Common Step

`CommonStepInCtld::StepStatusChange` 处理 primary step 和其他 common steps。

主要流转：

```text
Configuring
  -> Running
      primary step running 会推动 job 进入 Running

Configuring
  -> Failed/Cancelled
      进入 completing flow，并取消其他相关 step

Running/Completing
  -> Completing
      向 craned 发 FreeSteps 清理 step

Completing
  -> terminal status
      primary terminal status 最终驱动 job final status
```

requeue 不在 common step 状态机内部直接执行。step 状态机只把 job 推到 final path，
由 final path 统一决定是否 requeue。

## Runtime Fields

### `requeue_requested`

`requeue_requested` 是手动 requeue intent：

- `JobScheduler::RequeueJob` 将其设置为 `true`。
- 设置后立即写入 EmbeddedDB runtime attr。
- 对 Configuring job，还会同时设置 `cancel_requested=true`，等待配置状态机收敛。
- `ResetForRequeue()` 会将其重置为 `false`。

该字段使 ctld 重启恢复时仍能知道某个 running/configuring job 已被请求 requeue。

### `requeue_if_failed`

`requeue_if_failed` 表示失败时是否允许自动 requeue。它参与
`JobInCtld::ShouldRequeue()` 的最后一条判断：

```text
requeue_if_failed && status != Completed && status != Cancelled
```

当前需要注意：proto、job、step、JobInfo 中都有该字段，但后端从
`JobToCtld.requeue_if_failed` 复制到 `JobInCtld::requeue_if_failed` 的路径需要重点复核。
如果这条赋值链路缺失，`cbatch --requeue` 只会体现在 proto/展示层，自动失败 requeue
可能不会按预期触发。

### `no_requeue`

`job_to_ctld.no_requeue()` 会阻止 system failure requeue。当前 `ShouldRequeue()` 在
system failure 分支之前检查该字段。

需要复核前端是否真正设置 `no_requeue=true`。如果前端只把
`requeue_if_failed=false` 当作 `--no-requeue`，则 `EC_CRANED_DOWN` / `EC_RPC_ERR`
仍会触发自动 requeue。

### `requeue_count`

`requeue_count` 标识同一个 `job_id` 的第几次运行：

- `ResetForRequeue()` 每次自增。
- runtime attr 会同步写入 EmbeddedDB。
- step 创建时继承 job 当前 `requeue_count`。
- MongoDB 以 `(job_id, requeue_count)` 作为 job/step 归档关联键。
- 达到 `MaxRequeueCount` 后，job 仍会完成本次 reset，但会以 held pending job 形式保留。

## Craned Cleanup Relationship

ctld requeue 需要依赖 craned 完成当前 run 的 step cleanup：

```text
manual requeue or failed job
  -> ctld terminates primary/common step
  -> craned supervisor reports Completing
  -> ctld sends FreeSteps
  -> craned cleans step/supervisor/cgroup
  -> craned reports terminal status
  -> ctld marks step/job final
  -> ctld sends FreeJobs for finished job allocation
```

craned 侧当前关键行为：

- `JobManager::FreeSteps` 将 step 放入 cleanup queue。
- daemon step cleanup 会保存 job cgroup、resource、epilog env。
- daemon supervisor 通过 `ShutdownSupervisor()` 退出。
- silent cleanup 的非 daemon step 会通过 `TerminateStep()` 退出。
- `JobManager::FreeJobs` 只释放已经完全 freeable 的 job record：
  `step_map.empty()` 且 job cgroup 已清理。

因此，daemon step 比 primary/common step 更早或更晚清理完成都可能发生。任何“job 本地
结束”的判断都不能只依赖 daemon cleanup 完成这一时刻，而应该等待所有本地 step 和 job
cgroup 都完成清理。

这不改变正常状态机中 daemon 最后收尾的语义，也不把该语义限定为 Batch。它只约束异常
cleanup/recovery 实现：不能用“daemon cleanup callback 已执行”作为唯一完成条件，而应
显式检查本地 step map、job cgroup 和 cleanup retry 状态都已清空。

## Restart Recovery

ctld restart recovery 中，scheduler 会从 EmbeddedDB/MongoDB 恢复 job。

当前和 requeue 相关的行为：

- 如果 recovered job 的 runtime attr 中 `requeue_requested=true`，恢复流程会尝试
  reset step id counter、`ResetForRequeue()`，再放回 pending queue。
- recovered ended job 如果 `ShouldRequeue()` 为 true，也会走恢复 requeue 路径。
- 恢复路径与正常 final path 一样受 `MaxRequeueCount` 限制。

需要特别关注 craned 与 ctld 视图不一致的场景。若 ctld 重启后不再跟踪某个 job，但
craned 本地仍有该 job 的 active supervisor，craned recovery/cleanup 必须确保这些
orphaned local steps 能被终止并释放，否则 AutoTest 的 `wait_supervisor` 会看到残留
`csupervisor`。

## Known Boundaries and Risks

- `JobScheduler::RequeueJob` 只查 running job map；pending、completed、历史 job 不能
  手动 requeue。
- 当前只支持单个 materialized array child requeue，不支持 whole-array requeue。
- `RequeueJobRequest` 仍使用 `repeated uint32 job_ids`，没有使用 `JobIdSelector`，因此
  后端无法表达 `array_parent_task` 或 array range selector。
- 传入 array parent id 时，当前不应尝试展开整个 array；它应按“不支持/不在 running
  map 中”的边界处理。
- array child requeue 后，在再次完成前仍应视为未终结的 materialized task，不能让
  parent 仅因为 child 离开 running map 就提前 finalize。
- 手动 RPC 只显式拒绝 Interactive job；但 `ShouldRequeue()` 只允许 Batch job。若其他
  类型 job 被手动标记成功，final path 可能不会真正 requeue。
- `requeue_if_failed` 的后端赋值链路需要复核，否则自动失败 requeue 可能不会生效。
- `no_requeue` 的前端设置链路需要复核，否则 system failure 仍可能自动 requeue。
- requeue path 不触发 dependency events；依赖语义需要使用方明确。
- requeue 先归档 MongoDB，再 reset/update EmbeddedDB。任一步 DB 操作失败时，当前实现会
  drop/purge ended job，存在一致性和可恢复性风险。
- 达到 `MaxRequeueCount` 后 job 会被 held，而不是直接进入 final completed/failed 历史。
- daemon step cleanup 的快慢不能代表整个 job cleanup 完成，需要等待所有 step cleanup
  和 job cgroup cleanup 都结束。

## Key Code References

- RPC definitions: `protos/Crane.proto`
  - `RequeueJobRequest`
  - `RequeueJobReply`
  - `CraneCtld::RequeueJob`
- Runtime fields: `protos/PublicDefs.proto`
  - `requeue_if_failed`
  - `no_requeue`
  - `requeue_count`
  - `requeue_requested`
- Manual requeue entry: `src/CraneCtld/RpcService/CtldGrpcServer.cpp`
  - `CraneCtldServiceImpl::RequeueJob`
- Manual scheduler logic: `src/CraneCtld/JobScheduler.cpp`
  - `JobScheduler::RequeueJob`
- Finalization and requeue persistence: `src/CraneCtld/JobScheduler.cpp`
  - final job processing around `ShouldRequeue()`
  - `JobScheduler::PersistAndRequeueJobs_`
- Requeue decision and reset: `src/CraneCtld/CtldPublicDefs.cpp`
  - `JobInCtld::ShouldRequeue`
  - `JobInCtld::ResetForRequeue`
- Array lifecycle: `src/CraneCtld/Array.cpp`
  - `ArrayManager::MaterializeChildForAllocation`
  - `ArrayManager::OnChildTerminal`
  - `ArrayManager::ResolveJobIdSelector`
- Step state machines: `src/CraneCtld/CtldPublicDefs.cpp`
  - `DaemonStepInCtld::StepStatusChange`
  - `CommonStepInCtld::StepStatusChange`
- Craned cleanup: `src/Craned/Core/JobManager.cpp`
  - `JobManager::FreeJobs`
  - `JobManager::FreeSteps`
  - daemon cleanup handling in `EvCheckSupervisorRunning_`
