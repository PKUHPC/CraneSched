# ceff 查看作业运行实况

**ceff用来显示作业运行过程中的实时情况。**

查看任务实时情况：

```bash
$ ceff <作业ID[,作业ID,...]>
```

**ceff运行结果展示**

```bash
$ ceff 125
JobId: 125
Qos: test_qos
User/Group: root(0)/root(0)
Account: PKU
JobState: Running
Cores: 1.00
CPU Utilized: 00:00:00
CPU Efficiency: 0.38% of 00:00:26 core-walltime
Job Wall-clock time: 00:00:26
Memory Utilized: 0.59 MB (estimated maximum)
Memory Efficiency: 0.06% of 1024.00 MB (1024.00 MB/node)
WARNING: Efficiency statistics may be misleading for RUNNING jobs.
```

## 主要输出项

- **JobId**: 作业的唯一标识符
- **Qos**: 作业运行所在的QoS级别
- **User/Group**: 提交作业的用户和用户组
- **Account**：账户名
- **State**: 作业的当前状态（例如，COMPLETED、FAILED、CANCELLED 等）
- **Cores**: 作业使用的核心数量
- **Nodes**: 作业分配的节点数量
- **Cores per node**: 每个节点分配的核心数量
- **CPU Utilized**: 作业实际使用的CPU时间
- **CPU Efficiency**: CPU使用效率，通常表示为作业实际使用的CPU时间占分配的核心墙时间的百分比
- **Job Wall-clock time**: 作业的墙钟时间，即作业从开始到结束的总时间
- **Memory Utilized**: 作业实际使用的内存量
- **Memory Efficiency**: 内存使用效率，通常表示为作业实际使用的内存量占分配内存的百分比

## 主要参数

- **-h/--help**: 显示帮助
- **-C/--config string**: 配置文件路径（默认为"/etc/crane/config.yaml"）
- **--json**: 以JSON格式输出后端返回的任务信息
- **-v/--version**: 显示ceff的版本

## 使用示例

**查询作业效率：**

```bash
$ ceff <作业ID>
```

**查询多个作业：**

```bash
# 用逗号分隔多个作业ID进行查询
$ ceff 1234,1235,1236
```

**JSON输出：**

```bash
$ ceff 125 --json
{
    "job_id": 125,
    "qos": "test_qos",
    "uid": 0,
    "user_name": "root",
    "gid": 0,
    "group_name": "root",
    "account": "PKU",
    "job_state": 2,
    "nodes": 1,
    "cores_per_node": 1,
    "cpu_utilized_str": "00:00:00",
    "cpu_efficiency": 0.099378559,
    "run_time_str": "00:01:40",
    "total_mem_mb": 0.59375,
    "mem_efficiency": 0.0579833984375,
    "total_malloc_mem_mb": 1024,
    "malloc_mem_mb_per_node": 1024
}
```

## 理解效率指标

### CPU效率

CPU效率表示您的作业对分配的CPU资源的有效利用程度：

- **高效率 (>80%)**：作业是CPU密集型的，很好地利用了分配的核心
- **中等效率 (40-80%)**：作业可能有I/O等待时间或未完全并行化
- **低效率 (<40%)**：作业可能是I/O密集型的、等待资源或不适合并行执行

### 内存效率

内存效率显示实际使用了多少分配的内存：

- **高效率 (>80%)**：内存分配与实际使用情况紧密匹配
- **低效率 (<40%)**：考虑在将来的作业中请求更少的内存以提高资源利用率

## 使用场景

1. **作业后分析**：查看已完成的作业以了解资源利用情况
2. **资源优化**：识别未来作业提交时过度或不足分配的资源
3. **故障排查**：调查作业失败或性能不佳的原因
4. **报告生成**：为项目或用户资源使用生成效率报告

## 相关命令

- [cacct](cacct.zh.md) - 查询已完成作业的计费信息
- [cqueue](cqueue.zh.md) - 查看队列中的活动作业
- [cbatch](cbatch.zh.md) - 提交批处理作业
- [crun](crun.zh.md) - 运行交互式作业
- [creport](creport.md) - 查询作业相关统计信息
