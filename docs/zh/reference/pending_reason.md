# 作业排队原因 (Pending Reason)

## 概述

当作业处于 PENDING（排队）状态时，系统会显示作业无法立即运行的原因。通过 `cqueue` 或 `ccontrol show job` 命令可以查看作业的排队原因，帮助用户了解作业等待的具体情况。

## 查看排队原因

### 使用 cqueue 查看

```bash
cqueue
```

输出示例：
```
JOBID    PARTITION  NAME     USER   ST   TIME     NODES  NODELIST(REASON)
101      CPU        job1     user1  PD   0:00     2      (Priority)
102      CPU        job2     user1  PD   0:00     4      (Resource)
103      GPU        job3     user2  PD   0:00     1      (Dependency)
104      CPU        job4     user1  PD   0:00     2      (Held)
```

### 使用 ccontrol show job 查看

```bash
ccontrol show job 101
```

输出示例：
```
JobId=101
...
State=PENDING
Reason=Priority
```

## 排队原因说明

排队原因按照判断顺序从高到低排列。如果作业同时满足多个条件，将显示排在前面的原因。

| 原因 | 说明 | 何时出现 |
|------|------|----------|
| `Held` | 作业被 hold | 作业以 hold 状态提交或被设置为 hold，需手动释放 |
| `BeginTime` | 未到开始时间 | 作业设置了延迟开始时间（`--begin` 参数），需等待到达指定时间 |
| `DependencyNeverSatisfied` | 依赖永远无法满足 | 要求依赖作业成功，实际失败，导致依赖条件无法满足 |
| `Dependency` | 等待依赖满足 | 作业依赖的其他作业尚未满足条件（如未完成、未开始等）|
| `Resource changed` | 资源配置已更改 | 节点资源在作业调度期间发生了变化，等待重新调度 |
| `Reservation deleted` | 预留资源已删除 | 作业原本分配的预留资源已被删除 |
| `Reservation changed` | 预留资源已更改 | 预留资源在调度期间已发生变化，等待重新调度 |
| `License` | 许可证不足 | 作业请求的许可证资源当前数量不足 |
| `Resource` | 资源不足 | 集群当前没有足够的资源（CPU、内存、GPU等）满足作业需求 |
| `Resource Reserved` | 资源已被预留 | 作业需要的资源在未来时间段已被其他预留占用 |
| `Priority` | 优先级不足 | 作业优先级低于其他排队作业，或达到并发作业数限制 |