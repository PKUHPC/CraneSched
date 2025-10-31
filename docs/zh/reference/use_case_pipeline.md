# 用例：带依赖的作业链

**场景**：运行一个各阶段依次依赖的流水线，每一步都要在上一阶段成功完成后才能执行。

通过作业依赖，可以为多阶段工作流（例如 预处理 → 训练 → 评估）建立严格的执行顺序。

## 步骤 1：提交第一个作业

```bash
# 预处理作业
job1=$(cbatch preprocess.sh | grep -oP '\d+')
echo "Preprocessing job: $job1"
```

## 步骤 2：提交依赖作业

```bash
# 训练作业（等待预处理完成）
# 注意：依赖语法可能因版本而异
job2=$(cbatch --dependency=afterok:$job1 train.sh | grep -oP '\d+')
echo "Training job: $job2"

# 评估作业（等待训练完成）
job3=$(cbatch --dependency=afterok:$job2 evaluate.sh | grep -oP '\d+')
echo "Evaluation job: $job3"
```

## 步骤 3：监控流水线

```bash
# 同时查看三个作业
cqueue -j $job1,$job2,$job3
```

## 提示

- 组合使用 `after`、`afterok`、`afterany` 等依赖类型，可同时表达严格顺序和宽松顺序。
- 将作业 ID 写入文件，方便后续自动化流程（CI/CD、笔记本）查询作业状态。
- 如果后续阶段依赖前序作业生成的产物，请将结果写入集群内的共享存储，以确保所有节点都能访问。
