# crun任务退出码通知功能 - 设计文档

## 概述

本文档描述了CraneSched中crun任务退出码通知机制的设计和实现。当crun任务中的任何任务失败（以非零退出码退出）时，supervisor会主动通知crun所有相关任务的退出码，实现任务失败时的详细状态同步。

## 背景

### 当前行为
目前，当具有多个任务的crun作业完成时，仅向crun客户端报告最终的作业级状态。如果个别任务以不同的退出码失败，crun客户端无法接收关于哪些任务失败以及具体退出码的详细信息。

### 需求
1. Supervisor必须监控crun作业中所有任务的退出码
2. 当任何任务以非零退出码退出时，supervisor必须通知crun所有任务的退出码
3. 通知必须包含task_id、exit_code以及任务是否被信号终止
4. 该机制必须与现有的crun-cfored-supervisor通信兼容

## 架构

### 通信流程
```
crun客户端 (前端) 
    ↕ CrunStream
Cfored (前端)
    ↕ TaskIOStream  
Supervisor (后端)
    ↓ 监控
任务（计算节点上的进程）
```

### 修改的组件

#### 1. 协议缓冲区 (protos/Crane.proto)

**添加到 StreamTaskIORequest:**
- 新请求类型: `TASK_EXIT_CODE_NOTIFICATION = 4`
- 新消息: `TaskExitCodeNotificationReq`
  - 包含重复的 `TaskExitInfo`，包括:
    - `task_id`: uint32
    - `exit_code`: uint32
    - `is_terminated_by_signal`: bool

**添加到 StreamCrunReply:**
- 新回复类型: `TASK_EXIT_CODE_NOTIFICATION = 7`
- 新消息: `TaskExitCodeNotification`
  - 包含重复的 `TaskExitInfo`（与上述结构相同）

#### 2. Supervisor (src/Craned/Supervisor/)

**TaskManager.h - StepInstance类:**
- 添加: `m_task_exit_codes_` - 存储所有任务退出码的映射表
- 添加: `m_task_exit_codes_mtx_` - 用于线程安全访问的互斥锁
- 添加方法:
  - `RecordTaskExitCode(task_id, exit_info)` - 存储任务退出码
  - `HasAnyTaskFailed()` - 检查是否有任务具有非零退出码
  - `GetAllTaskExitCodes()` - 检索所有记录的退出码

**TaskManager.cpp:**
- 修改 `EvCleanSigchldQueueCb_()`:
  - 任务退出时记录退出码
  - 检测非零退出码
  - 触发包含所有退出码的通知

**CforedClient.h/cpp:**
- 添加: `SendTaskExitCodeNotification(exit_codes)` 方法
  - 将任务退出码序列化为protobuf消息
  - 通过TaskIOStream发送至Cfored

## 实现细节

### 退出码记录
当任务进程退出时:
1. `HandleSigchld()` 从waitpid()捕获退出状态
2. 提取退出码（WEXITSTATUS或WTERMSIG）
3. 对于crun任务，`RecordTaskExitCode()` 存储信息
4. 如果退出码非零，触发通知

### 通知触发
```cpp
if (m_step_.IsCrun()) {
  m_step_.RecordTaskExitCode(task_id, exit_info.value());
  
  if (exit_info->value != 0) {
    auto all_exit_codes = m_step_.GetAllTaskExitCodes();
    if (!all_exit_codes.empty()) {
      m_step_.GetCforedClient()->SendTaskExitCodeNotification(all_exit_codes);
    }
  }
}
```

### 消息序列化
通知通过现有的TaskIOStream作为定界protobuf消息发送:
1. 创建类型为 `TASK_EXIT_CODE_NOTIFICATION` 的 `StreamTaskIORequest`
2. 用所有任务退出信息填充 `TaskExitCodeNotificationReq`
3. 使用 `SerializeDelimitedToCodedStream()` 序列化
4. 通过 `TaskOutPutForward()` 转发至Cfored

## 前端要求

**注意:** 本仓库中的后端实现已完成。前端（CraneSched-FrontEnd仓库）需要实现相应的处理程序以完成该功能。

### 在 Cfored 中（前端仓库）
1. 接收类型为 `TASK_EXIT_CODE_NOTIFICATION` 的 `StreamTaskIORequest`
2. 从请求中提取任务退出码信息
3. 创建类型为 `TASK_EXIT_CODE_NOTIFICATION` 的 `StreamCrunReply`
4. 通过CrunStream转发到已连接的crun客户端

### 在 crun 客户端中（前端仓库）
1. 接收类型为 `TASK_EXIT_CODE_NOTIFICATION` 的 `StreamCrunReply`
2. 解析任务退出码
3. 向用户显示详细的退出码信息或适当处理

## 测试策略

### 单元测试
- 测试具有多个任务的 `RecordTaskExitCode()`
- 测试具有各种退出码组合的 `HasAnyTaskFailed()`
- 测试 `GetAllTaskExitCodes()` 返回正确信息

### 集成测试
1. **单任务失败**: 提交包含1个任务的crun作业，验证退出码通知
2. **多任务成功**: 提交包含多个任务的crun作业，全部成功，无通知
3. **混合结果**: 提交包含多个任务的crun作业，部分失败，验证发送所有退出码
4. **信号终止**: 使用信号杀死任务，验证is_terminated_by_signal标志

### 测试场景示例
```bash
# 在2个节点上提交每节点4个任务的crun作业
crun -N 2 --ntasks-per-node 4 -c 1 test_script.sh

# test_script.sh根据task_id以不同代码退出
# 预期: 当第一个任务失败时，crun接收包含所有8个任务退出码的通知
```

## 向后兼容性

- 更改是向后兼容的
- 新消息类型是添加，不是修改
- 当没有任务失败时，保留现有行为
- 如果尚未实现，前端可以忽略新消息类型

## 安全考虑

- 退出码中没有敏感数据
- 退出码已经可以通过作业统计观察到
- 不需要认证/授权更改
- 消息序列化使用标准protobuf，无注入风险

## 性能影响

- 最小: 仅在任务失败时触发
- 内存: n个任务的退出码存储为O(n)
- 网络: 每个失败作业一条额外消息
- 对任务执行性能无影响

## 所需文档更新

### 用户文档
1. 更新 `docs/en/command/crun.md` - 添加退出码通知部分
2. 更新 `docs/zh/command/crun.md` - 中文版本
3. 更新 `docs/en/reference/exit_code.md` - 解释详细退出码报告
4. 更新 `docs/zh/reference/exit_code.md` - 中文版本

### 开发者文档
1. 添加此设计文档（中英文）
2. 使用新protobuf消息更新API文档
3. 在代码中添加解释该机制的注释

## 代码更改摘要

### 修改的文件

1. **protos/Crane.proto**
   - 添加 `TASK_EXIT_CODE_NOTIFICATION` 枚举值
   - 添加 `TaskExitCodeNotification` 和 `TaskExitCodeNotificationReq` 消息
   - 添加 `TaskExitInfo` 嵌套消息

2. **src/Craned/Supervisor/TaskManager.h**
   - 向 `StepInstance` 添加退出码跟踪成员
   - 添加退出码管理的公共方法

3. **src/Craned/Supervisor/TaskManager.cpp**
   - 实现退出码记录和检索方法
   - 修改SIGCHLD处理程序以触发通知

4. **src/Craned/Supervisor/CforedClient.h**
   - 添加 `SendTaskExitCodeNotification()` 声明

5. **src/Craned/Supervisor/CforedClient.cpp**
   - 实现通知发送逻辑

### 代码行数更改
- 添加: 约100行
- 修改: 约20行
- 总计: 约120行

## 未来增强

1. **可配置通知**: 添加启用/禁用通知的选项
2. **基于阈值的通知**: 仅在失败率超过阈值时通知
3. **聚合统计**: 在摘要中包含最小/最大/平均退出码
4. **持久日志**: 在作业数据库中存储详细退出码

## 参考

- [退出码文档](../en/reference/exit_code.md)
- [crun命令文档](../en/command/crun.md)
- [Protobuf语言指南](https://protobuf.dev/)
- [CraneSched架构](../../README.md)

## 版本历史

- v1.0 (2025-11) - 初始设计和实现
