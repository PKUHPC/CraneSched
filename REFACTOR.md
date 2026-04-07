# Task 生命周期 Hook 重构方案

## 说明

本文已经基于当前代码实现做过核对，主要参考：

- `src/Craned/Supervisor/TaskManager.cpp`
- `src/Craned/Supervisor/TaskManager.h`
- `src/Craned/Supervisor/CforedClient.cpp`

下面的结论不是抽象推测，而是按当前代码路径整理后的重构建议。

## 背景

当前 supervisor 中，任务级生命周期 hook 被拆成了两条彼此独立的流水线：

1. 启动侧：`ProcInstance::Spawn()` 中执行 task prolog。
2. 停止与收尾侧：`TaskManager::EvCleanTaskStopQueueCb_()` 中执行 task epilog，并计算任务最终状态。

这已经带来了可观测的不一致：

- 有些任务会执行 task epilog，但其实从未执行过 task prolog。
- 有些执行模型有 epilog 路径，但根本没有 prolog 路径。
- 有些早期失败路径直接结束任务，绕过了统一的 stop/finalize 队列。
- task prolog 与 task epilog 的覆盖范围和触发时机并不对齐。

结果就是：任务生命周期 hook 并没有挂在一个清晰、统一的状态机上，而是分散在不同执行模型和不同失败路径里。

## 当前代码行为

### Task prolog 的实际执行位置

当前 task 级 prolog 只在 `ProcInstance::Spawn()` 的子进程路径中执行，位置在握手成功之后、`execv()` 之前。包括：

- 配置项中的 `TaskPrologs`
- step 上的 `task_prolog`

这意味着：

- batch 任务会跑 task prolog
- crun 任务会跑 task prolog
- container 任务没有 task prolog 路径
- pod 任务没有 task prolog 路径

换句话说，prolog 现在被绑定在“进程型任务的 child exec 路径”上，而不是“任务生命周期本身”上。

### Task epilog 的实际执行位置

当前 task 级 epilog 集中在 `TaskManager::EvCleanTaskStopQueueCb_()` 中执行，包括：

- step 上的 `task_epilog`
- 配置项中的 `TaskEpilogs`

任务进入这条路径的入口目前包括：

- 普通进程任务的 `SIGCHLD`
- crun 任务在 `CforedClient::TaskEnd()` 中调用 `TaskStopAndDoStatusChange()`
- container 任务通过 CRI container event 进入 `EvCleanCriEventQueueCb_()` 后，再进入 `TaskStopAndDoStatusChange()`
- pod 任务在 `PodInstance::Kill()` 中手动调用 `TaskStopAndDoStatusChange(0)` 作为兜底收尾

需要特别说明的是：

- 代码里虽然给 `EvCleanCriEventQueueCb_()` 留了 pod 分支
- 但当前没有找到 pod 事件真正入队的生产者
- `PodInstance::Spawn()` 里也明确写了 TODO，说明 pod 正常退出的监控还缺失

因此，epilog 路径“比 prolog 更接近统一”，但 pod 的自然退出仍然没有真正并入同一套收尾机制。

## 已确认的问题

### 1. task prolog 只存在于 `ProcInstance`

目前只有 `ProcInstance::Spawn()` 会跑 task prolog。`ContainerInstance` 和 `PodInstance` 都没有对应的 task-level prolog 执行点，但它们仍然可能进入公共 epilog 路径。

当前效果是：

- `ProcInstance`：可能执行 prolog，也可能执行 epilog
- `ContainerInstance`：不会执行 prolog，但会执行 epilog
- `PodInstance`：不会执行 prolog，但在现有可达收尾路径中可能执行 epilog

这是当前最明确的“prolog 语义不统一”问题。

### 2. 多个失败路径直接调用 `TaskFinish_()`，绕过 stop 队列

当前有多条路径会直接调用 `TaskFinish_()`，而不是先经过 `TaskStopAndDoStatusChange()` 再进入 `EvCleanTaskStopQueueCb_()`。代表性场景包括：

- `LaunchExecution_()` 中的 task `Prepare()` 失败
- `LaunchExecution_()` 中的 task `Spawn()` 失败
- `EvGrpcExecuteTaskCb_()` 中的密码条目查询失败
- `EvGrpcExecuteTaskCb_()` 中的 step `Prepare()` 失败
- `EvGrpcExecuteTaskCb_()` 中的 step user cgroup 分配失败
- `EvTaskTimerCb_()` 中的 calloc 超时
- `EvCleanTerminateTaskQueueCb_()` 中 interactive 任务在“没有运行中的进程”时被直接结束

这些路径的直接后果是：

- task epilog 会被跳过
- 最终状态判定与真正收尾逻辑被拆散在多个调用点中

这正是当前 epilog 缺失问题的根源之一。

### 3. 有些失败会执行 epilog，但其实从未执行 prolog

在 `ProcInstance::Spawn()` 中，父子握手失败会设置 `err_before_exec`，随后父进程杀掉子进程。之后该任务仍会通过 `SIGCHLD` 被回收，并进入 `TaskStopAndDoStatusChange()`，最终走到 epilog 路径。

但 task prolog 的执行发生在更后面：只有父子握手成功、子进程继续往下走到 `execv()` 前，才会执行 prolog。

因此这类失败的现状是：

- 没有 prolog
- 但有 epilog

这未必一定错误，但它说明当前代码并没有显式定义 hook 语义，而只是依赖“碰巧走到了哪条函数路径”。

### 4. pod 路径仍然明显分叉，而且比文档表面看起来更严重

daemon pod 的执行路径由 `ExecutePodAsync()` / `EvExecutePodCb_()` 单独处理，这条路径部分复制了普通任务执行逻辑，但没有真正共享同一套生命周期收尾控制面。

这条路径当前至少有几个问题：

- 没有 task-prolog 阶段
- pod 正常退出监控仍缺失，`PodInstance::Spawn()` 中已经明确留下 TODO
- `EvExecutePodCb_()` 中的某些失败分支没有干净地汇聚到统一 finalize 路径

尤其要注意：

- `EvExecutePodCb_()` 中密码查询失败时，会直接 `TaskFinish_()`
- `EvExecutePodCb_()` 中 step `Prepare()` 失败时，当前只是 `break`，甚至没有进入 `TaskFinish_()` 或 stop 队列

这说明 pod 路径不仅分叉，而且仍然存在“失败后没有统一收尾”的缺口。

## 根因分析

根本问题不是“某几个 epilog 被漏掉了”，而是当前设计没有把下面这些概念明确区分开：

- 启动前准备
- 用户负载已经开始尝试启动
- 任务已经停止
- 任务最终状态已经确定
- 任务清理与 hook 执行

现在代码主要依赖两个低层函数表达这些语义：

- `TaskStopAndDoStatusChange()`
- `TaskFinish_()`

但这两个函数的职责边界并不清晰，结果就是：

- 有些调用方把“stopped”当作统一收尾入口
- 有些调用方直接跳到“finished”
- prolog 绑在某一种执行模型上
- epilog 绑在另一条相对统一但仍不完整的收尾路径上

## 重构目标

这次重构应该建立一套对所有执行模型都成立的任务生命周期状态机。

目标：

- 统一 proc、crun、container、pod 的任务收尾入口
- 让 hook 语义显式化，而不是隐含在调用路径里
- 保证 task epilog 最多执行一次
- 明确定义 task prolog 什么时候必须执行、什么时候不该执行
- 去掉外部路径对 `TaskFinish_()` 的直接依赖
- 把 OOM / timeout / cancellation / launch failure 的终态判定集中到一个地方

第一阶段非目标：

- 不在这一步重做整个 supervisor 状态流转
- 不在这一步把 CRI 事件处理整体迁出 supervisor
- 不修改外部 hook 配置格式

## 建议方案

### 1. 引入统一的 finalize 请求入口

增加一个统一的、基于队列的 finalize 请求，例如：

```cpp
struct TaskFinalizeRequest {
  task_id_t task_id;
  std::optional<TaskExitInfo> exit_info_override;
  std::optional<FinalStatusOverride> final_status_override;
  bool should_run_epilog;
};
```

所有任务结束路径，包括早期失败路径，都不再直接调用 `TaskFinish_()`，而是统一投递这个请求。

这样一来，`EvCleanTaskStopQueueCb_()` 可以逐步演化为真正意义上的 `EvFinalizeTaskCb_()`。

### 2. 把 `TaskFinish_()` 收缩成纯 sink

建议把 `TaskFinish_()` 重命名为类似 `CommitTaskFinalStatus_()` 的名字，并把它的职责严格限制为：

- 从 `m_task_map_` 中移除任务
- 调用 `task->Cleanup()`
- 更新 `final_termination_status`
- 在全部任务结束后触发 step 完成与 supervisor 退出

它不应该再被启动路径或错误处理路径当成通用 API 直接调用。

### 3. 显式记录任务生命周期阶段与 hook 状态

建议为每个 task 增加显式生命周期状态，例如：

```cpp
enum class TaskLifecyclePhase {
  Created,
  Prepared,
  LaunchAttempted,
  Stopped,
  Finalized,
};
```

再增加显式 hook 状态：

```cpp
struct TaskHookState {
  bool prolog_ran{false};
  bool epilog_ran{false};
};
```

然后明确规定：

- task prolog 只在“即将尝试启动用户负载”时执行
- task epilog 在任务进入统一 finalize 流程后执行
- task epilog 最多只能执行一次

这样可以消除当前“一个 stopped 的 task 到底有没有跑过 prolog，没人记录”的模糊状态。

### 4. 把 hook 策略从 `ProcInstance` 解耦出来

当前把 task-level hook 策略放在 `ProcInstance` 的 child 路径里，抽象边界是错的。

更合理的方向是：

- 各种 task type 继续保留各自的负载启动细节
- task-level hook 的时机和策略统一由 `TaskManager` 编排

可选落地方式：

1. 第一阶段先保留进程任务在 child context 中执行 prolog，但显式记录它是否已经执行。
2. 在 `ITaskInstance` 上增加类似 `RunTaskPrologIfNeeded()` 的接口。
3. 让 container / pod 也实现同一份 task-level hook 契约。

对当前代码来说，第一步最稳妥的过渡方案是第 2 种：先把“是否需要执行 prolog”的决策提到统一接口层，再逐步补齐不同 task type 的实现。

### 5. 用更丰富的终态覆盖结构替代 `err_before_exec`

当前 `err_before_exec` 其实已经在扮演一种“有限的终态覆盖”角色，但表达能力太弱。

建议替换成更明确的结构：

```cpp
struct FinalStatusOverride {
  crane::grpc::TaskStatus status;
  uint32_t exit_code;
  std::optional<std::string> reason;
};
```

这样早期失败路径就可以进入同一个 finalize 队列，同时又能明确指定最终状态，而不是在多个函数里各自拼接状态与 exit code。

### 6. 让 pod 路径真正并入普通任务生命周期

`ExecutePodAsync()` / `EvExecutePodCb_()` 不应该继续复制一套局部生命周期逻辑。

建议方向：

- pod 特有的资源准备仍然保留在 pod 相关代码中
- 但任务创建、hook 执行、终态判定、cleanup 入口都汇聚到同一个 finalize 管线
- pod bootstrap 失败也必须投递 finalize 请求，而不是 `break` 或直接 `TaskFinish_()`
- 明确补齐 pod 正常退出监控，不再依赖 `PodInstance::Kill()` 中的手动兜底收尾

## 建议语义

### Task prolog

task prolog 只应在任务真正进入“尝试启动用户负载”这一阶段时执行。

这意味着，下面这些场景不应该强行执行 prolog：

- 密码查询失败，任务尚未进入真正启动阶段
- step 级 cgroup 分配失败，用户负载尚未开始启动
- task 对象创建或准备阶段失败，尚未尝试启动用户负载

### Task epilog

task epilog 应该在任务已经进入受管生命周期、并且进入统一 finalize 流程后执行，而不是取决于“当前代码恰好是从哪条函数路径结束的”。

这应当至少覆盖：

- 正常退出
- 信号退出
- timeout
- cancellation
- OOM
- 任务对象已经进入执行管线之后发生的启动失败
- pod/container/proc/crun 这些执行模型的统一收尾

如果以后想收紧 epilog 语义，也应该通过显式生命周期阶段来表达，而不是继续通过“有没有调用 `TaskFinish_()`”来间接推断。

## 迁移计划

### Phase 1：机械收敛

- 将 `TaskFinish_()` 改名为更明确的 sink 风格命名
- 引入统一的 finalize 请求结构
- 把所有直接调用 `TaskFinish_()` 的外部路径改成投递 finalize 请求

### Phase 2：集中 epilog

- 让 task epilog 只在统一 finalize handler 中执行
- 通过每任务 hook 状态保证只执行一次
- 清理剩余的零散 epilog 入口

### Phase 3：集中 prolog 契约

- 增加显式的 prolog 执行状态
- 定义所有 task type 通用的 pre-launch hook 接口
- 如果 task-level hook 的语义本来就是“任务级”，那就为 container / pod 补齐等价支持

### Phase 4：收敛 pod 执行面

- 让 `EvExecutePodCb_()` 与普通任务共享同一套生命周期控制平面
- 补齐 pod 正常退出监控
- 确保 pod bootstrap 失败也统一进入 finalize 队列

## 验证计划

重构后至少应补充以下测试或定向集成验证：

- batch 任务正常成功：prolog 执行，epilog 执行
- batch 任务在 `exec` 前启动失败：prolog 不执行，epilog 语义明确
- batch 任务在 fork 后握手失败：prolog 不执行，epilog 语义明确
- batch 任务 timeout：epilog 执行
- crun 任务正常退出：prolog 执行，epilog 执行
- container 任务正常退出：prolog 语义明确，epilog 执行
- pod daemon bootstrap 失败：进入统一 finalize 路径
- pod 正常退出：不再依赖手动 kill 兜底，也能进入统一 finalize 路径
- 任意路径都不会重复执行 epilog
- 任意路径都不会绕过统一 finalize 聚合点

## 总结

当前问题不只是“某些失败路径漏掉了 epilog”。

更本质的问题是：task 生命周期 hook 被拆散在了下面几类路径里：

- 执行模型专属的启动代码
- 一条相对统一但仍不完整的 stop/finalize 队列
- 多个直接调用 `TaskFinish_()` 的捷径路径

这次重构的重点，应该是把生命周期阶段显式化，并让所有任务结束路径都先汇聚到同一个 finalize 管线。只有这样，才能同时修复：

- epilog 在早期失败时被跳过的问题
- prolog 只对部分执行模型生效的问题
- pod/container/proc/crun 之间生命周期语义不一致的问题
