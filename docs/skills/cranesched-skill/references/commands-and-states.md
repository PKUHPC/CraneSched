# 命令、状态与错误速查

## 目录

- [命令选择](#命令选择)
- [稳定的只读查询](#稳定的只读查询)
- [作业与作业步标识](#作业与作业步标识)
- [作业状态](#作业状态)
- [Pending 原因](#pending-原因)
- [退出码](#退出码)
- [提交错误分类](#提交错误分类)
- [版本差异处理](#版本差异处理)

## 命令选择

| 命令 | 面向用户的用途 | 状态影响 |
|---|---|---|
| `cbatch` | 提交批处理脚本 | 创建作业 |
| `calloc` | 申请资源并进入交互式 shell | 创建并占用分配 |
| `crun` | 交互运行程序；在已有分配内创建 step | 创建作业或作业步 |
| `cqueue` | 查看活动作业和作业步 | 只读 |
| `cinfo` | 解释站点明确公开的分区、节点和资源状态 | 只读，但范围由站点政策决定 |
| `ccontrol show` | 查看用户自己的作业或步骤详情 | 只读，但受权限限制 |
| `cacct` | 查看活动和历史作业/步骤统计 | 只读 |
| `ceff` | 查看 CPU 和内存使用效率 | 只读 |
| `ccancel` | 取消作业或单个作业步 | 立即改变状态 |
| `ccontrol hold/release/update job` | 管理用户自己的活动作业 | 改变状态或属性，受权限限制 |
| `cattach` | 附加到自己的交互式作业步 | 转发输入输出，不默认终止 step |
| `ccon` | 提交、查看和操作容器作业 | 取决于子命令 |
| `cacctmgr show user <current_user> -P` | 查询本人账户与分区限制 | 只读，仅限本人可见范围 |

不要向普通用户提供节点、分区、账户、QoS、预留、许可证或服务的管理查询与写操作。需要这些信息时请用户联系管理员。

## 稳定的只读查询

### 活动作业

```bash
cqueue --self -F
cqueue -j <job_id> -F
cqueue --step -j <job_id>
cqueue --self --partition=<partition> -F
```

始终使用 `--self` 或明确属于用户自己的作业 ID；不要查看其他用户作业。默认最大行数在文档和版本间存在差异，检查自己的大量作业时用当前 `cqueue --help` 核对 `--max-lines`，不要把截断误判成作业缺失。

### 作业详情和历史

```bash
ccontrol show job <job_id>
ccontrol show step <job_id>.<step_id>
cacct -j <job_id> -F
ceff <job_id>
```

`cacct -j` 支持普通作业、数组子任务和作业步标识。`ceff` 可接受逗号分隔的多个作业 ID；运行中作业的效率统计可能不完整。

### 站点明确公开的资源概况

`cinfo` 的通用文档会展示集群范围的分区、节点和故障原因，但没有承诺这些信息在每个站点都对普通用户公开。默认只解释用户已经提供的输出。只有站点用户文档或管理员明确确认普通用户可查时，才按当前 `cinfo --help` 选择最小范围；不要通过试运行来探测权限。

站点没有明确公开时，使用本人作业详情中的 Partition、Reason 和已分配节点作为升级证据，请管理员核对相应资源。分区配置、节点故障原因、账户关联和 QoS 策略仍由管理员确认。

### 本人账户记录

仅在当前版本和站点允许普通用户查询本人记录时使用：

```bash
current_user=$(id -un) || exit 1
printf 'current_user=%s\n' "$current_user"
cacctmgr show user "$current_user" -P
```

先让用户确认打印出的有效身份就是本人。不要使用可能陈旧或被覆盖的 `$USER`，不要省略用户名，也不要查询其他用户、账户或全局 QoS。若被拒绝、结果不足或需要任何修改，请携带原始报错联系管理员。

### JSON 与完整字段

`cqueue`、`cinfo`、`cacct`、`ceff` 和 `ccontrol` 的当前文档均提供 JSON 输出。先用 `<command> --help` 核对标志位置；例如：

```bash
cacct --json -j <job_id>
ccontrol --json show job <job_id>
```

人工诊断保留表格或键值输出；自动化才优先 JSON。不要用脆弱的列位置解析截断表格。

## 作业与作业步标识

| 对象 | 格式 | 示例 |
|---|---|---|
| 普通作业 | `jobid` | `123` |
| 普通作业步 | `jobid.stepid` | `123.2` |
| 数组子任务 | `jobid_arraytaskid` | `229_0` |
| 数组子任务步骤 | `jobid_arraytaskid.stepid` | `229_0.1` |
| 数组聚合行 | `anchor_[start-end]` | `229_[0-9]` |

对取消、附加或详情查询，确认使用的是父作业、数组子任务还是单个 step。`ccancel 123` 与 `ccancel 123.2` 影响范围不同。

## 作业状态

| 状态 | 用户含义 | 首要动作 |
|---|---|---|
| `Pending` | 等待调度 | 查看 Reason |
| `Running` | 已分配资源并执行 | 查看节点和应用输出 |
| `Completed` | 退出码为 0，成功完成 | 查看输出与效率 |
| `Failed` | 非 0 退出 | 结合 ExitCode 与 stderr |
| `ExceedTimeLimit` | 超出申请时限 | 查耗时、checkpoint 与时限 |
| `Cancelled` | 被用户或管理员取消 | 确认取消来源和时间 |
| `OutOfMemory` | 超过分配内存并被终止 | 查峰值和应用内存行为 |
| `Configuring` | 正在进行启动前配置 | 观察持续时间与节点范围 |
| `Starting` | 资源就绪、任务准备启动 | 观察是否正常过渡 |
| `Completing` | 正在执行结束处理 | 观察持续时间与影响范围 |

不要将 `Completed` 之外的状态都归为“调度器故障”。应用失败、用户取消、时限和内存超限是不同类别。

## Pending 原因

### 常规原因

| Reason | 含义 | 下一步 |
|---|---|---|
| `Held` | 作业被 hold | 核对来源；明确授权后 release |
| `BeginTime` | 未到延迟开始时间 | 核对时间与时区 |
| `Dependency` | 等待依赖 | 查看上游状态和依赖详情 |
| `DependencyNeverSatisfied` | 依赖已不可能满足 | 修正依赖并重新提交 |
| `License` | 许可证当前不足 | 核对名称、数量和占用 |
| `Resource` | 当前没有足够匹配资源 | 比较请求、容量、GRES、节点状态 |
| `Resource Reserved` | 资源被预留 | 核对预留和预计可用性 |
| `Priority` | 优先级或并发约束使其继续排队 | 解释政策，不承诺启动时刻 |
| `Resource changed` | 调度期间资源配置变化 | 重新查询，观察是否恢复 |
| `Reservation deleted/changed` | 依赖的预留被删除或改变 | 核对预留和作业请求 |

系统按判断顺序显示当前原因。解除一个原因后可能出现下一个原因。

### QoS 限制原因

- `QosCpuResourceLimit`：用户/账户的 CPU 或 TRES 限制。
- `QosMemResourceLimit`：内存 TRES 限制。
- `QosGresResourceLimit`：GRES TRES 限制。
- `QosJobsResourceLimit`：用户或账户运行作业数限制。
- `QosWallTimeLimit`：累计墙钟时间限制。
- `QosEntryNotFound`、`QosMetaNotFound`：统计状态异常，请联系管理员。

### Partition 限制原因

- `PartitionCpuResourceLimit`、`PartitionMemResourceLimit`、`PartitionGresResourceLimit`：分区 TRES 限制。
- `UserPartitionJobsLimit`、`AccPartitionJobsLimit`：分区内用户或账户运行作业数限制。
- `UserPartitionWallTimeLimit`、`AccPartitionWallTimeLimit`：分区累计墙钟时间限制。
- `PartitionEntryNotFound`：分区统计状态异常，请联系管理员。
- `UserMetaNotFound`、`AccountMetaNotFound`：用户或账户统计状态异常，请联系管理员。

## 退出码

`cacct` 的 `ExitCode` 使用 `主代码:次代码`：

- 主代码 `0-255`：程序 `exit` 返回值。
- 次代码 `1-63`：通常为终止信号。
- 次代码 `64`：已终止。
- 次代码 `65`：权限被拒绝。
- 次代码 `66`：Cgroup 错误。
- 次代码 `67`：文件未找到。
- 次代码 `68`：生成进程失败。
- 次代码 `69`：超出时间限制。
- 次代码 `70`：计算节点服务关闭。
- 次代码 `71`：执行错误。
- 次代码 `72`：RPC 失败。

解释顺序：

1. 先看状态是否为 `Failed`、`ExceedTimeLimit`、`OutOfMemory` 或 `Cancelled`。
2. 再读主代码和次代码。
3. 再对齐 stderr 的首次错误与时间线。
4. 不要只凭一个数字断定根因。

JSON 输出对退出值有不同编码范围：`0-255` 为返回值，`256-320` 为信号，`321` 及以上为 CraneSched 定义代码。解析自动化输出时不要直接套用表格的冒号格式。

## 提交错误分类

不要要求普通用户记错误枚举；按可行动类别解释。

### 用户可修正

- 对象不存在：无效用户、账户、QoS、Wckey、资源、分区或作业 ID。核对拼写和可见范围。
- 参数无效：整数、时间、内存、截止时间、节点列表、GRES 或依赖格式错误。
- 请求过大：节点、每任务 CPU、内存、GRES、时限或单作业 TRES 超过分区/QoS 上限。
- 数量限制：用户、账户、QoS 或 Partition 的提交/运行作业数达到上限。等待、取消不需要的作业或申请调整。
- 作业步资源超限：step 请求超过父作业，缩小 step 或重新申请父作业。

### 需要联系管理员

- 用户不属于账户、账户不允许该 QoS、默认 QoS 不可用。
- 账户不在分区允许列表或在拒绝列表。
- 用户/账户被阻止、身份不匹配、证书签发问题。
- 单作业或累计 TRES/时限确有业务需要但超过政策。

### 集群侧异常

- 连接超时、连接中断、RPC/流错误。
- 内部元数据条目缺失、数据库插入/更新失败。
- Cgroup、PMIx、节点服务或容器运行时错误。
- 多用户、多个简单作业在同一时间共同失败。

遇到策略或集群侧异常时保留错误原文、时间、作业 ID、节点和影响范围并联系管理员。不要提供管理员命令、受限文件路径，也不要让用户通过高频重试放大问题。

## 版本差异处理

命令文档与已安装版本可能不同。始终使用：

```bash
<command> --version
<command> --help
```

处理原则：

- 短选项不确定时优先采用当前帮助中明确的长选项。
- 不使用其他调度器的参数习惯推断 CraneSched 支持，例如数组 `%并发数`。
- 不把文档示例中的返回文案当稳定接口；提取作业 ID 时优先使用当前版本 JSON 输出，若没有则让用户人工记录。
- 站点策略优先于通用示例；命令语法以实际安装版本为准。
- 发现冲突时明确写出“文档基线”和“现场输出”分别是什么，再给兼容分支。
