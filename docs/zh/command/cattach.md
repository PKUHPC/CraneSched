# cattach - 附加到作业步

cattach 用于将当前终端附加到一个正在运行的交互式作业步，实现对作业步标准输入/输出的实时转发。用户的输入将被转发到作业步的任务，任务的输出将被转发回用户终端。cattach 需要在有 cfored 运行的节点上使用。

## 用法

```
cattach [flags] <jobid>.<stepid>
```

其中 `<jobid>.<stepid>` 为目标作业步的标识符，可通过 `cqueue --step` 查询正在运行的作业步。

## 选项

**-h, --help**

:   显示 cattach 命令的帮助信息。

**-v, --version**

:   显示 cattach 版本信息。

**-C, --config=&lt;path&gt;**

:   配置文件路径。默认值：`/etc/crane/config.yaml`。

**--output-filter=&lt;task_id&gt;**

:   仅显示指定任务的标准输出（task_id 从 0 开始计数）。默认值：`-1`（显示所有任务的标准输出）。

**--error-filter=&lt;task_id&gt;**

:   仅显示指定任务的标准错误输出（task_id 从 0 开始计数）。默认值：`-1`（显示所有任务的标准错误）。

**--input-filter=&lt;task_id&gt;**

:   仅将标准输入发送到指定任务（task_id 从 0 开始计数）。默认值：`-1`（广播到所有任务）。

**--label**

:   在标准输出和标准错误的每行前添加任务编号前缀，格式为 `<task_id>: <line>`。

**--layout**

:   打印作业步的任务布局信息后退出，不附加到任务。

**--quiet**

:   静默模式，抑制提示性信息输出。

## 使用示例

### 基本用法

查询正在运行的作业步：

```bash
$ cqueue --step -j 42
```

附加到作业 42 的步 0：

```bash
$ cattach 42.0
Task io forward ready, waiting input.
hello
hello from task 0
```

按 `Ctrl+C` 可断开 cattach 连接，作业步本身不受影响，仍继续运行。

### 查看任务布局

打印作业步的节点和任务分布后退出，不附加到任务：

```bash
$ cattach --layout 42.0
Job step layout:
        1 tasks, 1 nodes (crane01)
```

### 多任务步 I/O 过滤

对于包含多个任务的作业步，可以过滤特定任务的输出或将输入定向到指定任务：

```bash
# 仅显示任务 0 的标准输出
$ cattach --output-filter 0 42.0

# 仅显示任务 1 的标准输出
$ cattach --output-filter 1 42.0

# 仅将标准输入发送到任务 0
$ cattach --input-filter 0 42.0

# 同时过滤输出并定向输入
$ cattach --output-filter 0 --input-filter 0 42.0
```

### 带标签的输出

为每行输出添加任务编号前缀，适合多任务步的调试与观察：

```bash
$ cattach --label 42.0
0: Hello from task 0
1: Hello from task 1
0: Done
```

### 静默模式

附加时不打印提示信息（如 "Task io forward ready, waiting input."）：

```bash
$ cattach --quiet 42.0
```

## 注意事项

- cattach 必须在运行有 cfored 的节点上执行。
- **PTY 模式**由作业步的配置自动决定，无需手动指定。若目标作业步通过 `crun --pty` 启动，cattach 将自动以 PTY 模式运行。
- **只读模式**：当目标作业步通过 `crun --input=<task_id>` 启动时（即标准输入已独占路由到特定任务），cattach 将自动进入只读模式，仅显示任务输出，不转发终端输入。
- 在非 PTY 模式下，按 `Ctrl+C` 将断开 cattach 连接，不会终止作业步本身。
- `--error-filter` 选项当前暂不生效，原因是标准错误消息在协议层面暂未携带任务 ID，将在后续版本中支持。

## 相关命令

- [crun](crun.md) - 提交交互式任务
- [calloc](calloc.md) - 分配资源供交互式使用
- [cqueue](cqueue.md) - 查看作业队列和作业步
- [ccontrol](ccontrol.md) - 控制和查询作业/作业步
- [ccancel](ccancel.md) - 取消作业和作业步
