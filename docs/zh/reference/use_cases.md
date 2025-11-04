# 使用案例

本指南专注于鹤思中最常见的日常工作流程，并从作业提交到结果检索逐步讲解每个场景。对于专门或大规模工作负载，请参考"下一步"部分链接的高级指南。

---

## 用例 1：基础批处理作业

**场景**：在单个节点上运行简单的计算任务。

### 任务描述

您有一个处理数据并将结果输出到文件的脚本。这是批处理最常见的用例。

### 步骤 1：准备脚本

创建名为 `simple_job.sh` 的文件：

```bash
#!/bin/bash
#CBATCH --job-name=simple_test
#CBATCH --partition=CPU
#CBATCH --nodes=1
#CBATCH --ntasks-per-node=1
#CBATCH --cpus-per-task=1
#CBATCH --mem=100M
#CBATCH --time=0:05:00
#CBATCH --output=job_%j.out

# 打印作业信息
echo "作业开始时间: $(date)"
echo "运行节点: $(hostname)"
echo "作业 ID: $CRANE_JOB_ID"

# 模拟一些计算工作
echo "正在处理数据..."
sleep 5
echo "数据处理完成!"

# 打印作业完成信息
echo "作业结束时间: $(date)"
```

### 步骤 2：提交作业

```bash
cbatch simple_job.sh
```

预期输出：
```
Job <12345> is submitted to partition <CPU>
```

### 步骤 3：检查作业状态

```bash
cqueue -j 12345
```

或查看你的所有作业：

```bash
cqueue --self
```

### 步骤 4：查看结果

作业完成后，查看输出文件：

```bash
cat job_12345.out
```

---

## 用例 2：交互式会话

**场景**：需要在计算节点上交互式地测试代码或调试应用程序。

### 任务描述

不提交批处理作业，而是直接申请计算节点的交互式资源并进入命令行。

### 步骤 1：申请交互式会话

```bash
crun -p CPU -N 1 -c 2 --mem 500M -t 1:00:00 /bin/bash
```

这将分配：
- CPU 分区的 1 个节点
- 2 个 CPU 核心
- 500 MB 内存
- 1 小时时间限制

### 步骤 2：交互式工作

连接后，你会在计算节点上看到 shell 提示符：

```bash
# 查看当前位置
hostname

# 查看分配资源
echo $CRANE_JOB_NODELIST

# 运行你的命令
python test_script.py
./my_program --input data.txt

# 完成后退出
exit
```

退出时资源会自动释放。

---

## 常见模式

### 查看作业历史

作业完成后，使用 `cacct` 查看作业统计：

```bash
# 查看你最近的作业
cacct --user $USER --starttime $(date -d "7 days ago" +%Y-%m-%d)

# 查看某个作业详情
cacct --job <job_id>
```

### 取消作业

```bash
# 取消指定作业
ccancel <job_id>

# 取消你所有处于排队中的作业
ccancel --state=PENDING --user=$USER
```

### 资源效率

查看作业对分配资源的使用效率：

```bash
ceff <job_id>
```

---

## 提示与最佳实践

1. 从小开始：先用最小资源测试，再逐步扩大
2. 设置准确的时间限制：高估浪费资源，低估会导致作业被取消
3. 使用作业名称：便于在队列中识别
4. 监控资源使用：作业完成后用 `ceff` 优化后续提交
5. 错误处理：建议单独指定 `--error` 文件以捕获错误输出
6. 检查点：长作业建议实现 checkpoint 以便故障恢复

---

## 下一步

- 浏览高级场景：

    - [多节点并行作业](use_case_multi_node.zh.md)
    - [GPU 加速训练](use_case_gpu.zh.md)
    <!-- - [参数扫描（作业数组）](use_case_param_sweep.zh.md) -->
    <!-- - [带依赖的作业链](use_case_pipeline.zh.md) -->
    
- 查看[命令参考](../command/cbatch.md)获取详细参数
- 了解[集群配置](../deployment/configuration/config.md)
- 排查失败时参考[退出代码](exit_code.md)
