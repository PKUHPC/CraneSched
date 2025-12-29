# Prolog/Epilog 配置指南

Crane 支持多种 `prolog` 和 `epilog` 程序。请注意，出于安全原因，
这些程序没有设置搜索路径。你需要在程序中指定完整路径，或者设置 PATH 环境变量。
下表说明了在作业分配时可用的 `prolog` 和 `epilog` 及其运行的时间和位置。

| 参数                           | 位置                | 调用者       | 用户                   | 执行时机    |
|------------------------------|-------------------|-----------|----------------------|--------------|
| Prolog（config.yaml）          | 计算节点              | craned    | CranedUser（通常为 root） | 该节点首次启动作业步骤时 |
| CraneCtldProlog（config.yaml） | 主节点（CraneCtld 所在） | cranectld | CranectldUser        | 作业分配时   |
| Epilog（config.yaml）          | 计算节点              | craned    | CranedUser（通常为 root） | 作业结束时   |
| CraneCtldEpilog（config.yaml） | 主节点（CraneCtld 所在） | cranectld | CranectldUser        | 作业结束时   |

下表说明了在作业步骤分配时可用的 `prolog` 和 `epilog` 及其运行的时间和位置。

| 参数                                      | 位置        | 调用者        | 用户            | 执行时机                    |
|-----------------------------------------|-----------|------------|---------------|-----------------------------|
| CrunProlog（config.yaml 或 crun --prolog） | crun 调用节点 | crun 命令    | 执行 crun 命令的用户 | 作业步骤启动前              |
| TaskProlog（config.yaml）                 | 计算节点      | csupervisor | 执行 crun 命令的用户 | 作业步骤启动前              |
| crun --task-prolog                      | 计算节点      | csupervisor | 执行 crun 命令的用户 | 作业步骤启动前              |
| TaskEpilog（config.yaml）                 | 计算节点      | csupervisor | 执行 crun 命令的用户 | 作业步骤完成时              |
| crun --task-epilog                      | 计算节点      | csupervisor | 执行 crun 命令的用户 | 作业步骤完成时              |
| CrunEpilog（config.yaml 或 crun --epilog） | crun 调用节点 | crun 命令    | 执行 crun 命令的用户 | 作业步骤完成时              |

默认情况下，`Prolog` 脚本只会在某个节点首次收到来自新分配的作业步骤时运行；
它不会在分配刚被授予时立即运行 `Prolog`。
如果某个分配的作业步骤从未在某节点运行，则该节点不会为该分配运行 `Prolog`。
此行为可通过 `PrologFlags` 参数进行更改。
而 `Epilog` 脚本则始终在分配释放时于每个节点上运行。

如果指定了多个 `prolog` 或 `epilog` 脚本（例如 "/etc/crane/prolog.d/*"），
它们将按字母逆序（z-a -> Z-A -> 9-0）运行。

`Prolog` 和 `Epilog` 脚本应尽可能简短，且不应调用 Crane 命令
（如 `cqueue`、`ccontrol`、`cacctmgr` 等）。
长时间运行的脚本会导致作业启动或结束缓慢，影响调度。
脚本中调用 Crane 命令可能导致性能问题，应避免使用。

`TaskProlog` 以与用户任务相同的环境执行。该程序的标准输出会被读取并按如下方式处理：

- `export name=value` ：为用户任务设置环境变量
- `unset name` ：清除用户任务中的环境变量
- `print ...` ：写入任务的标准输出

`TaskProlog` 脚本可以是 `bash` 脚本，以下是一个简单示例：
```bash
#!/bin/bash

# TaskProlog 可用于作业步骤运行前的准备工作，也可用于修改用户环境。主要有两种机制，通过向 stdout 打印命令实现：

# 设置变量供用户使用
echo "export VARIABLE_1=HelloWorld"

# 清除用户的变量
echo "unset MANPATH"

# 也可打印消息
echo "print This message has been printed with TaskProlog"
```
上述功能仅限于 `task prolog` 脚本。

## 故障处理

- 如果 `Prolog` 失败（返回非零退出码），该节点会被置为 `DRAIN` 状态且作业会被重新排队。
  如果 `Epilog` 失败（返回非零退出码），该节点会被置为 `DRAIN` 状态。
- 如果 `CraneCtldProlog` 失败（返回非零退出码），
  作业会被重新排队。只有批处理作业能被重新排队。交互式作业（`calloc` 和 `crun`）
  在 `CraneCtldProlog` 失败时会被取消。
  如果 `CraneCtldEpilog` 失败（返回非零退出码），仅会记录日志。
- 如果 `task prolog` 失败（返回非零退出码），
  该任务会被取消。如果 `crun prolog` 失败（返回非零退出码），
  该步骤会被取消。如果 `task epilog` 或 `crun epilog` 失败（返回非零退出码），
  仅会记录日志。

## Prolog 和 Epilog 配置
* PrologTimeout: Prolog 脚本的运行超时时间（秒）。
* EpilogTimeout： Epilog 脚本的运行超时时间（秒）。
* PrologEpilogTimeout: Prolog 和 Epilog 脚本的运行超时时间（秒）。
当同时设置时，此参数会覆盖 PrologTimeout 和 EpilogTimeout。
* PrologFlags：用于控制 Prolog 脚本的执行方式，通过不同的标志实现更灵活的作业生命周期管理。可配置一个或多个标志，多个标志用逗号分隔。

配置示例：
```yaml
JobLifecycleHook:
  Prolog: /path/to/prolog.sh
  PrologTimeout: 60
  # PrologFlags: Alloc  # Alloc, Contain, NoHold, RunInJob, Serial
  Epilog: /path/to/epilog.sh
  EpilogTimeout: 60 
  PrologEpilogTimeout: 120
  CranectldProlog: /path/to/cranectld_prolog.sh
  CranectldEpilog: /path/to/cranectld_epilog.sh
  CrunProlog: /path/to/srun_prolog.sh
  CrunEpilog: /path/to/srun_epilog.sh
  TaskProlog: /path/to/task_prolog.sh
  TaskEpilog: /path/to/task_epilog.sh
```

## Prolog 标志
用于控制 Prolog 行为的标志。默认情况下没有设置任何标志。可以用逗号分隔的列表指定多个标志。目前支持的选项包括：

- **Contain**  
  在作业分配时，Prolog 脚本会在作业的cgroup下执行。

- **ForceRequeueOnFail**  
  当批处理作业因 Prolog 失败而无法启动时，即使作业请求不重排队，也会自动将其重新排队。

- **RunInJob**  
  使 Prolog/Epilog 在 extern cranestepd 进程中运行。
  这将把其包含为作业的进程之一。
  如果配置了 cgroup，则会被包含在 cgroup 中。
  设置 RunInJob 标志会隐式地设置 Contain 标志。

- **Serial**  
  默认情况下，Prolog 和 Epilog 脚本会在每个节点上并发运行。
  此标志会强制这些脚本在每个节点上串行运行，但会显著降低每个节点上的作业吞吐量。  
  **注意：** 这与 RunInJob 不兼容。

## 示例
**/etc/crane/prolog.sh** 
需先保证脚本有可执行权限，并确保脚本正确
```bash
#!/bin/bash

LOG_FILE="/var/crane/prolog.log"
JOB_ID=$CRANE_JOB_ID
ACCOUNT=$CRANE_JOB_ACCOUNT
NODE_NAME=$CRANE_JOB_NODELIST
DATE=$(date "+%Y-%m-%d %H:%M:%S")

echo "[$DATE] === Prolog Start ===" >> $LOG_FILE
echo "JOB_ID: $JOB_ID" >> $LOG_FILE
echo "ACCOUNT: $ACCOUNT" >> $LOG_FILE
echo "NODE: $NODE_NAME" >> $LOG_FILE

# 检查节点健康状况（示例）
FREE_MEM_MB=$(free -m | awk 'NR==2 {print $4}')
if (( FREE_MEM_MB < 200 )); then
    echo "Node memory low: ${FREE_MEM_MB}MB → reject job" >> $LOG_FILE
    exit 1  # 非 0 → 阻止作业执行
fi

# 5. 输出结束标识
echo "=== Prolog End ===" >> $LOG_FILE
echo "" >> $LOG_FILE

exit 0
```

**`/etc/crane/config.yaml`配置**
```yaml
JobLifecycleHook:
  Prolog: /etc/crane/prolog.sh
```