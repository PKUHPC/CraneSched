# cwrapper - Slurm 命令兼容

`cwrapper` 为熟悉 Slurm 的用户提供基本的命令兼容性。可以直接调用兼容入口：

```bash
cwrapper sacct [选项]
cwrapper squeue [选项]
cwrapper sinfo [选项]
```

管理员也可以按照[前端部署指南](../deployment/frontend/frontend.md)配置 Slurm 命令别名。

兼容入口按 Slurm 的含义解析选项，部分短选项和格式符与鹤思原生命令不同。以下列出 `sacct`、`squeue` 和 `sinfo` 的主要差异；完整选项与格式符以各入口的 `--help` 输出为准，例如 `cwrapper sinfo --help`。如需使用鹤思的高级特性，请使用原生命令。

## sacct

`cwrapper sacct` 查询作业记账信息，对应原生命令 [`cacct`](cacct.md)。

| 功能 | Slurm 兼容入口 | 原生命令 `cacct` |
|------|----------------|-----------------|
| 隐藏表头 | `-n/--noheader` | `-N` |
| 按分区筛选 | `-r/--partition` | `-p` |
| 按状态筛选 | `-s/--state` | `-t` |
| 按节点筛选 | `-N/--nodelist` | `-w` |
| 按作业名筛选 | `--name` | `-n` |
| 按提交时间筛选 | `--submittime` | `-s` |

兼容入口的 `-p/--parsable` 和 `-P/--parsable2` 均输出以 `|` 分隔的结果。`-p` 在每行末尾保留 `|`，`-P` 则不添加末尾分隔符。

## squeue

`cwrapper squeue` 查询作业队列，对应原生命令 [`cqueue`](cqueue.md)。

兼容入口使用 `-h/--noheader` 隐藏表头，帮助仅通过 `--help` 显示；原生命令使用 `cqueue -N` 隐藏表头。

兼容入口的 `-o` 格式符含义如下：

| 格式符 | 含义 |
|--------|------|
| `%T` | 长状态名，如 `PENDING` |
| `%t` | 短状态名，如 `PD` |
| `%R` | 排队作业的排队原因（带括号），或已分配的节点列表 |
| `%r` | 排队原因 |

原生命令 `cqueue` 的 `%T` 表示作业类型，`%t` 表示作业状态。

## sinfo

`cwrapper sinfo` 查询节点与分区状态，对应原生命令 [`cinfo`](cinfo.md)。

| 功能 | Slurm 兼容入口 | 原生命令 `cinfo` |
|------|----------------|-----------------|
| 隐藏表头 | `-h/--noheader`；帮助仅通过 `--help` 显示 | `-N` |
| 自定义格式 | `-o` 使用 Slurm 格式符，如 `%P %a %l %D %t %T %N %E %F` | `-o` 使用 `%p %a %n %s %l` |

兼容入口还支持以下输出选项：

- `-O` 使用字段名指定输出，例如 `Partition,Nodes:.5,StateLong`。
- `-N` 按节点显示，每个节点与分区组合输出一行。
- `-R` 默认显示 `REASON NODELIST` 两列。

兼容入口默认显示以下六列，其中 `TIMELIMIT` 固定为 `infinite`：

```text
PARTITION AVAIL TIMELIMIT NODES STATE NODELIST
```

原生命令 `cinfo` 默认显示五列：

```text
PARTITION AVAIL NODES STATE NODELIST
```
