# Pod Bootstrap Regression Fix Plan

## Background

Recent standalone `ccon run` jobs fail with:

```text
Pod config file /tmp/<job>.out/.<node>.bin does not exist
```

The failure first appears after commit `bf9c016` ("feat: X11 multi connection and multi task support for step").

This document records the full path analysis, the chosen fix, and the validation plan.

## Step And Task Classification

Supervisor behavior is not uniform across all jobs. The key dimensions are:

- `StepType`
  - `DAEMON`
  - `PRIMARY`
  - `COMMON`
- `TaskType`
  - `Batch`
  - `Interactive`
    - `Crun`
    - `Calloc`
  - `Container`

The effective execution modes in the current code are:

- `DAEMON` + `pod_meta`
  - This is the container bootstrap step.
  - The pod is created during supervisor init in `Supervisor.cpp`, before `SupervisorFinishInit(Running)`.
  - This path uses `TaskManager::ExecuteTaskAsync()` directly from supervisor init.
- `DAEMON` without `pod_meta`
  - No task execution is triggered during init.
  - The daemon step mainly exists for job lifecycle / supervision purposes.
- `PRIMARY` / `COMMON` + `container_meta`
  - This is a container execution step.
  - It starts later through the `ExecuteTask` RPC from Craned.
  - It must load pod bootstrap artifacts (`.<node>.bin` and `.<node>.lock`) produced by the daemon pod step.
- `PRIMARY` / `COMMON` + batch or interactive payload
  - This is a process execution step.
  - It also starts later through the `ExecuteTask` RPC from Craned.
- `Calloc`
  - Still has a logical task identity, but does not spawn a real process.

## End-To-End Path

### 1. Ctld builds step payloads

- `DaemonStepInCtld::GetStepToD()` builds the daemon step payload.
- `CommonStepInCtld::GetStepToD()` builds primary/common payloads.

Important difference:

- Common steps explicitly populate `task_res_map`.
- Daemon steps do not populate `task_res_map`.

This is currently visible in:

- `src/CraneCtld/CtldPublicDefs.cpp`

### 2. Craned spawns a supervisor per step

- Craned creates a `StepInstance` for each `StepToD`.
- It forks `csupervisor` and sends the whole `StepToD` over `InitSupervisorRequest`.

Relevant path:

- `src/Craned/Core/JobManager.cpp`
- `src/Craned/Core/StepInstance.cpp`

### 3. Supervisor classifies the step

- `DAEMON` + `pod_meta`
  - `Supervisor.cpp` calls `g_task_mgr->ExecuteTaskAsync()` during init.
  - If pod setup succeeds, `SupervisorFinishInit(Running)` is sent.
- non-daemon steps
  - `SupervisorFinishInit(Starting)` is sent during init.
  - Actual task launch happens later through the `ExecuteTask` RPC.

Relevant path:

- `src/Craned/Supervisor/Supervisor.cpp`
- `src/Craned/Supervisor/SupervisorServer.cpp`

### 4. TaskManager launches concrete task instances

Inside `EvGrpcExecuteTaskCb_()`:

- it creates `PodInstance`, `ContainerInstance`, or `ProcInstance` by iterating over `m_step_.task_ids`
- it prepares the step
- it allocates the step cgroup
- it launches each task

For pod daemon steps, `PodInstance::Spawn()` persists:

- `.<node>.bin`
- `.<node>.lock`

For container steps, `ContainerInstance::Prepare()` later loads those files.

Relevant path:

- `src/Craned/Supervisor/TaskManager.cpp`

## Root Cause

### Direct root cause

After `bf9c016`, supervisor-side `StepInstance` started deriving `task_ids`
from `step.task_res_map()`.

That is correct for common steps, because ctld fills `task_res_map` for them.

It is incorrect for pod daemon steps, because ctld still sends daemon steps
without `task_res_map`, while the pod bootstrap path still needs one synthetic
supervisor task `0` to drive `PodInstance`.

Therefore:

- daemon step `task_ids` becomes empty
- `EvGrpcExecuteTaskCb_()` skips the `for (auto task_id : m_step_.task_ids)` loop
- no `PodInstance` is created
- no `PodInstance::Prepare()` / `Spawn()` runs
- no `.<node>.bin` is written
- container step later fails in `LoadPodSandboxInfo_()`

### Secondary regression

`EvGrpcExecuteTaskCb_()` currently pushes the step to `Running` before tasks are actually launched.

That is acceptable for common steps as the local "execution requested" transition, but it is wrong for daemon pod steps, because daemon `Running` is supposed to mean "pod bootstrap is ready" and is already reported by `SupervisorFinishInit(Running)`.

This causes the duplicated / premature daemon `Running` warning in the supervisor logs.

### Why not fix only in Ctld

An alternative is to populate daemon `task_res_map[0]` in ctld and keep supervisor unchanged.

This is not the preferred primary fix because:

- it expands the contract of daemon `StepToD`
- it couples daemon pod bootstrap to the newer task resource model
- pod bootstrap does not actually need `task_res_map` today
- the regression was introduced in supervisor semantics, so the lowest-risk compatibility repair is also in supervisor

## Chosen Fix

### Fix 1. Restore the synthetic task 0 for pod daemon steps

In supervisor-side `StepInstance`:

- derive `task_ids` from `task_res_map` as before
- if the step is a pod daemon step (`DAEMON` + `pod_meta`) and the result is
  empty, create the synthetic task `{0}`

This is not justified by "historical behavior" alone. The key point is that pod
bootstrap is implemented as a supervisor task, while ctld does not populate
`task_res_map` for daemon pod steps. Therefore the supervisor must synthesize
task `0` specifically for the pod daemon path, without changing non-pod daemon
semantics.

### Fix 2. Add a hard guard for executable steps with empty `task_ids`

Inside `EvGrpcExecuteTaskCb_()`:

- if `task_ids` is empty, fail immediately instead of silently returning success
- for common / primary steps, schedule supervisor shutdown after reporting the error back to Craned
- for daemon init, just return error to the init path; supervisor init will then publish `Failed`

This prevents future "empty success" regressions.

### Fix 3. Keep daemon `Running` transition only in `SupervisorFinishInit()`

Inside `EvGrpcExecuteTaskCb_()`:

- do not call `GotNewStatus(Running)` for daemon steps
- keep the current behavior for non-daemon steps

This removes the duplicate daemon `Running` transition and aligns local state with actual pod readiness.

## Explicit Non-Goals

- Do not redesign Ctld daemon `StepToD` resource semantics in this patch.
- Do not change common-step multi-task scheduling behavior.
- Do not change container `task_res_map` usage for task cgroups.

## Validation Plan

### Functional

- Run standalone `ccon run` as `root` from `/tmp`
- Run standalone `ccon run` as test user `2401210647` from `/tmp`
- Confirm `.<node>.bin` and `.<node>.lock` are created before the container step starts
- Confirm container step no longer fails in `LoadPodSandboxInfo_()`

### Regression

- Run a normal `cbatch` job
- Run a container job
- Run at least one multi-task common step path to ensure `task_res_map`-based task creation still works
- Confirm no daemon log warns about receiving `Running` while already `Running`

## Implementation Order

1. Add supervisor-side task id initialization helper with pod daemon fallback
2. Add `EvGrpcExecuteTaskCb_()` empty-task guard
3. Restrict the local `Running` transition in `EvGrpcExecuteTaskCb_()` to non-daemon steps
4. Build
5. Run e2e tests
