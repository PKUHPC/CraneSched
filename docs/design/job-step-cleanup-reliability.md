# Job/Step Cleanup Reliability Design

本文记录 `TerminateSteps`、`FreeSteps`、Craned 重连 reconcile 和
`StepStatusChange` 之间的可靠性边界。目标是同时解决两类现场问题：

- step 已经进入 `Completing`，但 Ctld 下发 `FreeSteps` 失败，导致 job 长时间停在
  `Running`。
- step 或 job 仍显示 `Running`，但实际 supervisor、Cfored、Craned 或 RPC 状态已经
  断开，普通 cancel 不能保证收敛。

本文是内部设计草案，不改变现有行为。后续代码修改应先对齐这里的语义。

## 背景

CraneSched 的 step 结束不是单个 RPC 完成的，而是两段式：

1. `TerminateSteps` 或进程自然退出让 user process 停止。
2. Craned 上报 `Completing`，Ctld 再下发 `FreeSteps`，Craned 完成本地 cleanup 后补发
   terminal status。

这两段中任何一个 RPC 都可能丢失、超时、遇到 stale stub，或者在 Ctld/Craned
重连期间乱序到达。因此 cleanup 路径必须按 at-least-once delivery 设计：

- Ctld 可以重复发 cleanup 请求。
- Craned 必须能幂等处理重复请求。
- 重连 reconcile 必须能重新推动卡住的中间状态。
- cancel 不能被当成 completion barrier。

## 现有问题

### Completing 卡住

现场形态：

```text
cacct:
  job:     Running
  daemon:  Running
  primary: Completing

craned:
  primary supervisor exited
  primary Completing was sent to Ctld

ctld:
  primary step completing, terminating other steps
  Failed to FreeSteps for [job.primary] steps on Node <node>, stub invalid
```

根因：

- Supervisor 已经正常退出，Craned 和 Ctld 都知道 primary 到达 `Completing`。
- Ctld 依赖 `FreeSteps(primary)` 推动 Craned 本地 cleanup，并等待 Craned 补发
  `Completed`。
- `FreeSteps` 下发失败后当前只记录日志，没有 retry，也没有在重连时覆盖
  `Ctld=Completing, Craned=Completing` 的状态。
- 因为 primary 没有 terminal，Ctld 不会触发 daemon `FreeSteps`，job 保持
  `Running`。

关键代码入口：

- `CommonStepInCtld::StepStatusChange`: primary/common step 全节点到
  `Completing` 后生成 `craned_step_free_map`。
- `JobScheduler::CleanJobStatusChangeQueueCb_`: 对 `craned_step_free_map` 发
  `FreeSteps`。
- `CranedServiceImpl::FreeSteps`: Craned 收到后进入 `JobManager::FreeSteps`。
- `JobManager::FreeStepAllocation_`: cleanup 完成后补发 terminal status。
- `CtldClient` 配置同步: 当前 `craned_status == ctld_status` 会直接跳过，导致双方都
  是 `Completing` 时不会重放 `FreeSteps`。

### Running 卡住

现场形态会有多种：

```text
job or step: Running
实际状态: supervisor 已退出、Cfored 断开、Craned 短暂断线、TerminateSteps RPC 失败、
          或 Ctld/Craned 对 step 状态认知不一致
```

风险点：

- `ccancel` 只表示 cancel 请求被接收，不表示 step/job 已完成 cleanup。
- `TerminateSteps` 失败后如果没有可靠后续事件，Ctld 仍可能保留 `Running`。
- Craned 本地只接受 `Running/Starting` 的 terminate；如果状态已经进入
  `Completing`，terminate 不会重新推动 cleanup。
- 重连 reconcile 对 `Ctld=Running, Craned=Running` 的一致状态不会做额外检查；如果
  一致状态背后缺少活着的 supervisor 或 pending status，仍可能卡住。
- 重连 reconcile 对 `Ctld=Running, Craned=Starting` 的处理过激：当前会把 Craned
  本地 `Starting` step 标记为 invalid，并走 silent cleanup。Ctld 仍认为该 step
  `Running`，但 Craned 已经删除本地 step 且不向 Ctld 上报 terminal，job 会继续卡在
  `Running`。

因此 Running 卡住和 Completing 卡住不是同一个状态点，但需要同一套可靠性原则：

- `TerminateSteps` 负责停止仍在运行的 step。
- `StepStatusChange(Completing)` 负责把停止事实传回 Ctld。
- `FreeSteps` 负责本地 cleanup 和 terminal 补发。
- 任意一段丢失时，reconcile 必须能重新推动下一段，而不是只比较状态字符串。

#### Ctld Running / Craned Starting 卡住

现场形态：

```text
ctld:
  primary step current status Starting, got new status Starting
  primary step is ready to run
  job status becomes Running

craned reconnect/configure:
  daemon:  Ctld Running, Craned Running
  primary: Ctld Running, Craned Starting
  primary is starting but not running, mark as invalid
  Terminating invalid steps (not tracked by Ctld): [job.primary]

supervisor:
  Receive TerminateRunningStep Request
  Terminating step at Starting state, completing without launching tasks
  All tasks finished, exiting
```

根因：

- Ctld 已经完成 primary configure，并把 step/job 推到 `Running`，随后应由
  `ExecuteSteps` 驱动 Craned 从 `Starting` 进入实际执行。
- Craned 在 reconnect/configure 时仍看到本地 step 是 `Starting`。这不是
  “Ctld 不再跟踪的 invalid step”，而是 Ctld 比 Craned 领先一个执行阶段。
- 当前 reconcile 把 `craned_status == Starting` 的 mismatch 直接放入
  `invalid_steps`，并通过 `free_steps_silently` 清理本地 step。
- silent cleanup 会 suppress status forwarding；supervisor 在 Starting 状态被正常终止
  时也可能不发送 `Completing`。最终 Ctld 继续等待这个 primary step，job 停在
  `Running`。

正确收敛方向：

- `Ctld=Running, Craned=Starting` 不应默认视为 invalid。
- 当前不支持 `Starting` 状态恢复。`Ctld=Running, Craned=Starting` 说明 Ctld 已认为
  step 进入运行阶段，但 Craned 还没有可证明的本地执行事实，不能在 reconnect 中补发
  或重放 `ExecuteSteps`。
- Craned 可以清理本地 `Starting` step，但不能 silent cleanup；必须把该 step 作为
  `lost_steps` 上报给 Ctld。
- Ctld 收到 `lost_steps` 后应按统一 lost-step 路径合成
  `Completing + Failed`，并推动关联 step/job 收敛。这样用户能看到明确失败，而不是
  长期 `Running`。

## 目标语义

### `TerminateSteps`

`TerminateSteps` 的职责是让 Craned 对指定 step 发起终止动作。

语义要求：

- 对 `Running/Starting` step，调用 supervisor `TerminateStep`。
- 对已经 `Completing` 或 terminal 的 step，不能阻塞收敛；应返回成功或转交
  `FreeSteps` 路径，而不是无限重排 terminate 请求。
- 对不存在的 step，不能盲目生成新的失败状态。应结合 Ctld 是否仍认为该 step active
  来决定是 lost-step 处理还是重复请求。
- RPC 失败不能证明 step 已终止，也不能证明 step 未终止。

### `FreeSteps`

`FreeSteps` 的职责是释放 Craned 本地 step 资源，并在 cleanup 完成后补发 terminal
status。

建议把 `FreeSteps` 定义为幂等请求：

```text
FreeSteps(job.step) 多次调用的效果等价于一次调用。
```

状态语义：

| Craned 本地状态 | `FreeSteps` 行为 |
|---|---|
| step 在 `step_map` 中 | 启动 cleanup；普通 step 从 `step_map` 转移到 cleanup tracking；daemon step 保存 job cleanup context |
| step 已在 cleanup tracking 中 | 认为 cleanup 已在进行；不重复 terminate，不重复释放资源 |
| step 已 cleanup 完成 | 返回 success；如果有 terminal cache，可重发同一个 terminal；没有 cache 时不要生成新的失败状态 |
| step 从未见过 | 记录 warning；优先交给 recovery/lost-step 路径处理，不在普通 `FreeSteps` 中合成 Failed |

重要边界：

- `FreeSteps` 不应该负责判断用户进程是否应被 cancel。
- `FreeSteps` 不应该在重复请求时把成功完成的 step 改写成 Failed。
- 是否保存 cleanup completed cache 是增强项，不是最小修复的前置条件。

## 什么时候发送 `FreeSteps`

### Runtime 正常路径

1. Common/primary step 全执行节点上报 `Completing` 后，Ctld 发
   `FreeSteps(common_or_primary)`。
2. Primary step terminal 后，Ctld 发 `FreeSteps(daemon)`。
3. Daemon step 全节点上报 `Completing` 后，Ctld 发 `FreeSteps(daemon)`。

### Runtime 异常路径

1. Ctld 判定 Craned down 或 step lost 时，可以合成 `Completing + terminal`，并终止
   其他仍在线节点上的对应 step。
2. Ctld 对 `FreeSteps` RPC 失败不应立即合成 terminal。失败只说明 RPC 结果未知。
3. 如果后续 Craned 重新连接，reconcile 应重新触发本地 `FreeSteps`。

### Reconnect / Configure 路径

重连时，Craned 会收到 Ctld 的 job/step 状态快照，并和本地 step 状态对比。这里需要
明确两类动作：

- 状态不一致时，按当前逻辑同步更高优先级状态或 cleanup。
- 状态一致但仍处在中间状态时，也要检查是否需要推进。

建议规则：

```text
if ctld_status == Completing:
    enqueue local FreeSteps(job.step)
    continue
```

这个规则应放在 `craned_status == ctld_status` 的早返回之前。否则
`Ctld=Completing, Craned=Completing` 会被当成稳定状态跳过。

对于 Running 状态，不能简单地对所有 `Ctld=Running, Craned=Running` 都 terminate。
应增加本地活性检查：

- supervisor pid 是否存在；
- supervisor RPC stub 是否可用；
- 是否存在 pending unexpected supervisor exit；
- 是否已有 pending status change；
- interactive step 的 Cfored 连接是否已经明确断开。

如果本地 Running 失去必要活性，Craned 应生成 `Completing + terminal` 或触发
`TerminateSteps`/`FreeSteps`，而不是继续向 Ctld 声称 Running。

对于 `Ctld=Running, Craned=Starting`，也不能走 silent invalid cleanup。当前决策是：
不支持 `Starting` 恢复，直接按 lost/failed 收敛：

- Craned 本地释放该 `Starting` step 的残留资源。
- Craned 在 `EvConfigurationDone` / `CranedRegister` 中携带 `lost_steps`。
- Ctld 在注册处理里对 lost step 合成 `Completing + Failed`，再由现有状态机继续
  `TerminateSteps` / `FreeSteps`。
- 后续如果要支持 `Starting` 恢复，必须先给 `ExecuteSteps` 增加明确的 generation、
  幂等和重复执行保护；在当前语义下不要尝试重放 execute。

## 不建议的方案

### Ctld 在 `FreeSteps` 失败时直接完成 step

不建议。原因是 `FreeSteps` 失败有两种不同含义：

- RPC 没到 Craned，资源还未释放。
- RPC 到了 Craned，但 reply 丢失或 Ctld stub 状态过期。

Ctld 在这个点合成 terminal 会让 Ctld 与 Craned 本地资源状态分裂。

### 把 `ccancel` 当 completion barrier

不建议。`ccancel` 只说明 cancel 请求进入系统，不说明：

- supervisor 已终止；
- Craned 已上报 `Completing`；
- Ctld 已发出并完成 `FreeSteps`；
- daemon cleanup 和 job cgroup cleanup 已完成；
- EmbeddedDB/MongoDB 状态已经完成归档。

测试和运维脚本必须 poll job/step terminal 和节点资源释放。

### 普通 `FreeSteps` 对不存在 step 合成 Failed

不建议作为默认行为。重复 `FreeSteps`、迟到 `FreeSteps` 和真正 lost step 都可能表现为
“本地 step 不存在”。普通 cleanup RPC 无法区分这些情况。合成 Failed 应放在明确的
lost-step/recovery 路径中。

## 最小修复方案

### M1: Completing 卡住修复

改动目标：

1. Craned reconnect reconcile 中，`ctld_status == Completing` 时总是加入
   `completing_steps`，即使 Craned 本地也是 `Completing`。
2. `FreeSteps` 对不存在 step 的默认处理改为幂等成功，不再合成
   `Failed/EC_TERMINATED`。
3. Ctld `FreeSteps` RPC 失败时保留 `Completing`，日志说明等待 Craned
   reconnect/reconcile，不在失败点合成 terminal。

预期效果：

- 解决 `FreeSteps` 没到 Craned 导致 primary 永远 `Completing` 的问题。
- 不引入 completed terminal cache。
- 不改变正常成功路径。

仍未覆盖：

- Craned 已 cleanup 完成，但 terminal status 到 Ctld 的 RPC 丢失。没有 terminal cache
  时，Craned 无法精确重发原 terminal。

### M2: Running 卡住修复

改动目标：

1. Craned reconnect reconcile 增加 Running 活性检查。
2. `Ctld=Running, Craned=Starting` 不再进入 invalid silent cleanup；当前不支持
   `Starting` 恢复，应本地 cleanup 后通过 `lost_steps` 上报 Ctld，由 Ctld 合成
   `Completing + Failed`。
3. `TerminateSteps` 对 `Completing` step 不再无限等待 Running/Starting，可转为
   `FreeSteps` 或直接返回幂等成功。
4. 对 supervisor 已退出但没有 final status 的情况，保留并强化现有
   unexpected-supervisor-exit 兜底，确保最终发出 `Completing + terminal`。
5. 对 Ctld 侧 `TerminateSteps` RPC 失败，明确后续收敛策略：等待 reconnect/lost-step，
   或进入有限重试，而不是静默停在 Running。

预期效果：

- cancel 后如果进程已经不存在，不会因为 step 还显示 Running 而长期卡住。
- Craned 重连时不会只因为状态字符串一致就放过没有本地活性的 Running step。

### M3: 可选增强

1. Ctld 为 `FreeSteps` 和 `TerminateSteps` 增加有限 retry 队列。
2. Craned 保存 cleanup completed terminal cache，支持重复 `FreeSteps` 时重发同一个
   terminal status。
3. 给 cleanup request 增加 request id 或 generation，方便日志和去重。
4. 增加指标和告警：
   - step 在 `Completing` 超过阈值；
   - job 在 `Running` 但 primary `Completing` 超过阈值；
   - `FreeSteps` RPC 失败次数；
   - `TerminateSteps` RPC 失败次数；
   - reconnect reconcile 重放 cleanup 次数。

## 推荐实现细节

### `FreeSteps` 幂等化

建议调整 `JobManager::EvCleanFreeStepsQueueCb_`：

- `step_map` 找不到 step 且 cleanup tracking 存在：保持等待或立即 resolve waiter。
- `step_map` 找不到 step 且 cleanup tracking 不存在：记录 warning 并 resolve success；
  不调用 `SendCompletingAndTerminal_(Failed, ...)`。
- daemon step 已经有 `daemon_job_cleanup`：认为 cleanup 已经开始，避免重复移动 job
  cgroup ownership。
- 普通 step release 后进入 cleanup tracking，由 timer 检查 supervisor 是否退出并最终
  `FreeStepAllocation_`。

### Reconnect Completing 重放

建议调整 `CtldClient` 配置同步：

```cpp
if (ctld_status == StepStatus::Completing) {
  completing_steps[job_id].insert(step_id);
  continue;
}

if (craned_status == ctld_status) {
  continue;
}
```

这样双方都是 `Completing` 时也会重放 `FreeSteps`。由于 `FreeSteps` 已经幂等，重复
触发是安全的。

### Running 活性检查

建议新增 `JobManager` 查询接口，供 reconnect reconcile 使用：

```text
CheckLocalStepLiveness(job_id, step_id) -> Alive / CompletingPending / DeadNoTerminal / Missing
```

可能的判定：

- `kill(supv_pid, 0) == 0`: supervisor alive。
- step 在 `m_completing_step_retry_map_`: cleanup pending。
- step 有 pending terminal status: terminal can be resent after cleanup。
- unexpected supervisor exit map 中有记录: 等待 grace 后生成 terminal。
- step 不存在: missing，由 lost-step 或幂等 cleanup 规则处理。

对于 `Ctld=Running, Craned=Running` 但本地判定 `DeadNoTerminal` 的 step，Craned 应
主动 `SendCompletingAndTerminal_`，让 Ctld 状态机继续走。

### Ctld Running / Craned Starting

当前不支持从 `Starting` 恢复到 `Running`。建议调整 `CtldClient` 配置同步：

```cpp
if (ctld_status == StepStatus::Running &&
    craned_status == StepStatus::Starting) {
  lost_steps[job_id].insert(step_id);
  invalid_steps[job_id].insert(step_id);  // local cleanup only
  continue;
}
```

这里的 `invalid_steps` 只能表示 Craned 本地要释放残留资源，不能再使用
`free_steps_silently` 把 Ctld 可见状态吞掉。实现上可以拆成两个集合：

- `local_cleanup_steps`: Craned 本地删除或释放，不向 Ctld 发送普通 terminal。
- `lost_steps`: 随 `CranedRegister` 上报 Ctld，由 Ctld 合成失败终态。

Ctld 侧已有 `lost_steps` 处理路径：注册请求中带上 lost step 后，Ctld 对该 step 发送
synthetic `Completing + Failed`，失败原因使用 “Craned re-registered but step
lost.”，并继续触发后续 cleanup。这样 `Starting` 不再被静默删除，也不会长期保持
`Running`。

## 测试建议

### Completing 卡住

1. 注入 Ctld -> Craned `FreeSteps(primary)` 一次性失败。
2. 确认 job 暂时停在 `Running` / primary `Completing`。
3. 模拟 Craned reconnect/configure。
4. 确认 reconnect 后 Craned 重放 `FreeSteps(primary)`，primary terminal，随后 daemon
   cleanup，job terminal。

### 重复 FreeSteps

1. 对同一个 non-daemon step 连续发送两次 `FreeSteps`。
2. 第二次不应生成 Failed terminal。
3. job 最终 terminal 与第一次 cleanup 结果一致。

### Running 卡住

1. 模拟 supervisor 退出但 final status 没成功进入 Ctld。
2. reconnect 后 `Ctld=Running, Craned=Running` 但本地 supervisor dead。
3. 确认 Craned 生成 `Completing + terminal` 或触发 cleanup，job 不再长期 Running。

### Ctld Running / Craned Starting

1. 模拟 Ctld 已将 primary step 推到 `Running`，但 Craned reconnect/configure 时本地
   primary 仍是 `Starting`。
2. 确认 reconcile 不把该 step 当 invalid silent cleanup。
3. 确认 Craned 上报 `lost_steps`，Ctld 合成 `Completing + Failed`，job 不再长期
   `Running`。

### Cancel 不是 barrier

1. 发 `ccancel` 后立即查询，允许看到中间态。
2. poll 到 job/step terminal 和节点资源释放后再 reset DB counters/history。

## 未决问题

- 是否需要 completed terminal cache，以及 cache 的生命周期是 job 释放前、固定 TTL，
  还是持久化到本地 recovery 文件。
- Ctld 是否应该实现有限 retry，还是完全依赖 reconnect/reconcile。
- `FreeSteps` 对真正 “Ctld active 但 Craned 从未见过” 的 step 应该返回 success、
  synthetic failed，还是上报 lost-step 让 Ctld 统一处理。
- Running 活性检查是否只在 reconnect 时执行，还是 runtime 周期性执行。
- daemon step 的重复 `FreeSteps` 如何避免二次移动或二次释放 job cgroup ownership。

## 结论

短期应先修 Completing 卡住：

- `FreeSteps` 幂等化；
- reconnect 时重放所有 Ctld 视角的 `Completing` step；
- Ctld 不在 `FreeSteps` 失败点直接合成 terminal。

随后处理 Running 卡住：

- reconnect 增加本地活性检查；
- `Ctld=Running, Craned=Starting` 当前按 lost/failed 处理，不做 execute 恢复；
- `TerminateSteps` 与 `FreeSteps` 的中间态职责分清；
- supervisor dead/no-terminal 的路径必须最终生成 `Completing + terminal`。

最终目标是让 stop/cleanup/status 三段链路都具备可重放、幂等和可观测的收敛语义。
