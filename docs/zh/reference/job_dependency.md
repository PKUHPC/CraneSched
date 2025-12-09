# 作业依赖 (Job Dependency)

## 功能概述

CraneSched-FrontEnd 的 dependency 功能允许作业根据其他作业的状态来控制执行时机，实现作业间的依赖管理。通过依赖关系，可以构建复杂的工作流，确保作业按照正确的顺序执行。

## 支持的命令

dependency 功能在以下命令中可用：

- `cbatch` - 批处理作业提交
- `calloc` - 交互式资源分配
- `crun` - 交互式作业运行

## 命令行参数

```bash
--dependency, -d <dependency_string>
```

在提交作业时使用 `--dependency` 或 `-d` 参数指定依赖关系。

## 依赖字符串格式

### 基本语法

```
<type>:<job_id>[+<delay>][:<job_id>[+<delay>]]...
```

### 依赖类型

| 类型 | 说明 | 触发条件 |
|------|------|----------|
| `after` | 在指定作业开始或取消后启动 | 依赖作业脱离 Pending 状态 |
| `afterok` | 在指定作业执行成功后启动 | 依赖作业以退出码 0 完成 |
| `afternotok` | 在指定作业执行失败后启动 | 依赖作业以非 0 退出码完成（包括超时、节点错误等） |
| `afterany` | 在指定作业结束后启动 | 依赖作业结束（无论成功或失败）|

### 延迟时间 (delay)

可选的延迟参数，支持以下格式：

- **纯数字**（默认单位为分钟）
  - 示例：`10` = 10分钟

- **带单位的时间**
  - `s`, `sec`, `second`, `seconds` - 秒
  - `m`, `min`, `minute`, `minutes` - 分钟
  - `h`, `hour`, `hours` - 小时
  - `d`, `day`, `days` - 天
  - `w`, `week`, `weeks` - 周

- **D-HH:MM:SS 格式**
  - 示例：`1-01:30:00` = 1天1小时30分钟

### 多依赖组合

#### AND 逻辑（所有条件都满足）

使用 `,` 分隔不同的依赖条件：

```bash
after:100,afterok:101
```

作业会等待作业 100 开始 **并且** 作业 101 成功完成。

#### OR 逻辑（任一条件满足）

使用 `?` 分隔不同的依赖条件：

```bash
afterok:100?afterok:101
```

作业会在作业 100 **或** 作业 101 任一成功完成后启动。

!!! warning "注意"
    不能在同一个依赖字符串中混用 `,` 和 `?`，系统会返回错误。

## 使用示例

### 1. 基本依赖

```bash
# 等待作业 100 开始后再运行
cbatch --dependency after:100 my_script.sh

# 等待作业 100 成功完成后再运行
cbatch --dependency afterok:100 my_script.sh

# 等待作业 100 失败后再运行
cbatch --dependency afternotok:100 my_script.sh

# 等待作业 100 完成后再运行（无论成功或失败）
cbatch --dependency afterany:100 my_script.sh
```

### 2. 带延迟的依赖

```bash
# 等待作业 100 成功完成后，延迟 30 分钟再运行
cbatch --dependency afterok:100+30 my_script.sh

# 等待作业 100 成功完成后，延迟 10 秒钟再运行
cbatch --dependency afterok:100+10s my_script.sh

# 使用 HH:MM:SS 格式指定延迟
cbatch --dependency afterok:100+01:30:00 my_script.sh
```

### 3. 多依赖

```bash
# 等待作业 100 开始且作业 101、102 都成功完成
cbatch --dependency after:100,afterok:101:102 my_script.sh

# 等待作业 100 开始 10 分钟且作业 101 成功完成 30 分钟
cbatch --dependency after:100+10m,afterok:101+30m my_script.sh

# 等待作业 100 成功或作业 101 失败
cbatch --dependency afterok:100?afternotok:101 my_script.sh

# 等待作业 100、101 都成功完成后延迟 2 小时，或作业 102 开始后立即运行
cbatch --dependency afterok:100:101+2h?after:102 my_script.sh
```

### 4. 在批处理脚本中使用

在批处理脚本中也可以使用 `#CBATCH` 指令：

```bash
#!/bin/bash
#CBATCH --dependency afterok:100
#CBATCH --nodes 2
#CBATCH --time 1:00:00
#CBATCH --output job-%j.out

echo "This job starts after job 100 completes successfully"
# 你的作业代码
```

### 5. 在交互式命令中使用

```bash
# calloc 中使用依赖
calloc --dependency afterok:100 -n 4 -N 2

# crun 中使用依赖
crun --dependency after:100 -n 1 hostname
```

## 查看依赖状态

使用 `ccontrol show job <job_id>` 命令查看作业的依赖状态：

```bash
ccontrol show job 105
```

### 输出示例

```
JobId=105
...
Dependency=PendingDependencies=afterok:100+01:00:00 Status=WaitForAll
```

### 依赖状态字段说明

| 字段 | 说明 |
|------|------|
| `PendingDependencies` | 尚未触发的依赖条件 |
| `DependencyStatus` | 依赖满足状态（见下表）|

### 依赖状态值

| 状态 | 说明 |
|------|------|
| `WaitForAll` | 等待所有依赖满足（AND 逻辑）|
| `WaitForAny` | 等待任一依赖满足（OR 逻辑）|
| `ReadyAfter <time>` | 将在指定时间后就绪 |
| `SomeFailed` | 某些依赖失败（AND 逻辑，无法满足）|
| `AllFailed` | 所有依赖都失败（OR 逻辑，无法满足）|

## 错误处理

系统会在以下情况返回错误：

| 错误情况 | 说明 | 示例 |
|---------|------|------|
| 混用分隔符 | 不能同时使用 `,` 和 `?` | `afterok:100,afterok:101?afterok:102` |
| 格式错误 | 依赖字符串不符合语法规则 | `afterok:` 或 `after100` |
| 延迟格式无效 | 延迟时间格式不正确 | `afterok:100+invalid` |
| 重复依赖 | 同一作业 ID 出现多次 | `afterok:100:100` |
| 作业 ID 不存在或已结束 | 依赖的作业不存在（运行时检查）| `afterok:99999` |

### 错误示例

```bash
# 错误：混用 AND 和 OR 分隔符
$ cbatch --dependency afterok:100,afterok:101?afterok:102 job.sh

# 错误：延迟格式无效
$ cbatch --dependency afterok:100+invalid job.sh

# 错误：重复的作业 ID
$ cbatch --dependency afterok:100,afternotok:100 job.sh
```

## 相关命令

- [cbatch](../command/cbatch.md) - 提交批处理作业
- [crun](../command/crun.md) - 运行交互式任务
- [calloc](../command/calloc.md) - 分配资源并创建交互式 Shell
- [cqueue](../command/cqueue.md) - 查看作业队列状态
- [ccontrol](../command/ccontrol.md) - 控制作业和系统资源
- [ccancel](../command/ccancel.md) - 取消作业