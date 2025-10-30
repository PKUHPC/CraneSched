# ccancel - 取消作业

**ccancel 终止队列中正在运行或挂起的作业。**

您可以通过作业 ID 取消作业，或使用过滤条件一次取消多个作业。

## 命令语法

```bash
ccancel [作业ID[,作业ID...]] [选项]
```

## 命令行选项

### 作业选择
- **job_id[,job_id...]**: 要取消的作业 ID（逗号分隔的列表）。格式：`<job_id>` 或 `<job_id>,<job_id>,<job_id>...`

### 过滤选项
- **-n, --name string**: 取消具有指定作业名的作业
- **-u, --user string**: 取消指定用户提交的作业
- **-A, --account string**: 取消指定账户下的作业
- **-p, --partition string**: 取消指定分区中的作业
- **-t, --state string**: 取消指定状态的作业。有效状态：`PENDING`（P）、`RUNNING`（R）、`ALL`（不区分大小写）
- **-w, --nodes strings**: 取消在指定节点上运行的作业（逗号分隔的列表）

### 输出选项
- **--json**: 以 JSON 格式输出

### 其他选项
- **-C, --config string**: 配置文件路径（默认：`/etc/crane/config.yaml`）
- **-h, --help**: 显示帮助信息
- **-v, --version**: 显示版本号

## 过滤规则

!!! important
    必须提供至少一个条件：作业 ID 或至少一个过滤选项。

使用多个过滤器时，将取消匹配**所有**指定条件的作业（AND 逻辑）。

## 使用示例

### 按作业 ID 取消

取消单个作业：
```bash
ccancel 30686
```

**运行结果：**

![ccancel](../images/ccancel/ccancel_1.png)
![ccancel](../images/ccancel/ccancel_2.png)
![ccancel](../images/ccancel/ccancel_3.png)

取消多个作业：
```bash
ccancel 30686,30687,30688
```

### 按作业名取消

取消所有名为 "test1" 的作业：
```bash
ccancel -n test1
```

**运行结果：**

![ccancel](../images/ccancel/ccancel_n1.png)
![ccancel](../images/ccancel/ccancel_n2.png)
![ccancel](../images/ccancel/ccancel_n3.png)

### 按分区取消

取消 GPU 分区中的所有作业：
```bash
ccancel -p GPU
```

**运行结果：**

![ccancel](../images/ccancel/ccancel_p1.png)
![ccancel](../images/ccancel/ccancel_p2.png)
![ccancel](../images/ccancel/ccancel_p3.png)

### 按节点取消

取消在 crane02 上运行的所有作业：
```bash
ccancel -w crane02
```

![ccancel](../images/ccancel/ccancel_w.png)

### 按状态取消

取消所有挂起的作业：
```bash
ccancel -t Pending
```

![ccancel](../images/ccancel/ccancel_t.png)

取消所有正在运行的作业：
```bash
ccancel -t Running
```

取消所有作业（挂起和运行）：
```bash
ccancel -t All
```

### 按账户取消

取消 PKU 账户下的所有作业：
```bash
ccancel -A PKU
```

![ccancel](../images/ccancel/ccancel_A.png)

### 按用户取消

取消用户 ROOT 提交的所有作业：
```bash
ccancel -u ROOT
```

![ccancel](../images/ccancel/ccancel_u.png)

### 组合过滤器

取消 CPU 分区中所有挂起的作业：
```bash
ccancel -t Pending -p CPU
```

取消用户 alice 在 GPU 分区中的所有作业：
```bash
ccancel -u alice -p GPU
```

取消特定节点上正在运行的作业：
```bash
ccancel -t Running -w crane01,crane02
```

### JSON 输出

以 JSON 格式获取取消结果：
```bash
ccancel 30686 --json
```

使用过滤器和 JSON 输出取消：
```bash
ccancel -p GPU -t Pending --json
```

## 示例概览

![ccancel](../images/ccancel/ccancel.png)

## 取消后的行为

作业取消后：

1. **进程终止**：如果分配的节点上没有该用户的其他作业，作业调度系统将终止这些节点上的所有用户进程

2. **撤销 SSH 访问**：将撤销用户对分配节点的 SSH 访问权限

3. **资源释放**：所有分配的资源（CPU、内存、GPU）立即释放，可供其他作业使用

4. **作业状态更新**：作业状态在作业历史记录中变为 `CANCELLED`

## 权限要求

- **普通用户**：只能取消自己的作业
- **调度员**：可以取消其账户内的作业
- **操作员/管理员**：可以取消系统中的任何作业

## 重要说明

1. **立即生效**：作业取消立即生效。默认情况下，运行中的作业会立即终止，没有宽限期

2. **多个作业**：您可以使用逗号分隔的作业 ID 或过滤条件一次取消多个作业

3. **无确认提示**：没有确认提示。命令执行后立即取消作业

4. **状态过滤**：使用 `-t` 针对特定作业状态，以避免意外取消处于非预期状态的作业

5. **作业 ID 格式**：作业 ID 必须遵循格式 `<job_id>` 或 `<job_id>,<job_id>,<job_id>...`，不能有空格

## 错误处理

常见错误：

- **无效的作业 ID**：如果作业 ID 不存在或您没有取消权限，将返回错误
- **无匹配作业**：如果过滤条件不匹配任何作业，返回成功但取消 0 个作业
- **无效状态**：状态必须是以下之一：PENDING、RUNNING、ALL（不区分大小写）

## 另请参阅

- [cbatch](cbatch.md) - 提交批处理作业
- [crun](crun.md) - 运行交互式任务
- [calloc](calloc.md) - 分配交互式资源
- [cqueue](cqueue.md) - 查看作业队列
- [cacct](cacct.md) - 查看作业统计信息
- [ccontrol](ccontrol.md) - 控制作业和系统资源
