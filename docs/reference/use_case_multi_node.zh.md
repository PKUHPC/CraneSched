# 用例：多节点并行作业

**场景**：在多台计算节点上运行 MPI 应用，实现并行计算。

多节点作业适用于紧耦合的工作负载，例如 MPI 程序。本指南假设集群节点上已经提供可用的 MPI 工具链。

## 步骤 1：准备 MPI 作业脚本

创建 `mpi_job.sh`：

```bash
#!/bin/bash
#CBATCH --job-name=mpi_test
#CBATCH --partition=CPU
#CBATCH --nodes=3
#CBATCH --ntasks-per-node=4
#CBATCH --cpus-per-task=1
#CBATCH --mem=2G
#CBATCH --time=1:00:00
#CBATCH --output=mpi_job_%j.out

# 加载 MPI 模块（根据实际环境调整）
module load mpich/4.0

# 根据分配的节点生成 machinefile
echo "$CRANE_JOB_NODELIST" | tr ";" "\n" > machinefile

# 显示分配到的节点
echo "Running on nodes:"
cat machinefile

# 运行 MPI 应用
# 总进程数为 12（3 个节点 × 每节点 4 个任务）
mpirun -n 12 -machinefile machinefile ./my_mpi_program

echo "MPI job completed at $(date)"
```

## 步骤 2：编译 MPI 程序（如需）

```bash
mpicc -o my_mpi_program my_mpi_program.c
```

## 步骤 3：提交作业

```bash
cbatch mpi_job.sh
```

## 步骤 4：监控进度

```bash
# 查看作业状态
cqueue -j <job_id>

# 作业运行时查看实时输出
tail -f mpi_job_<job_id>.out
```

## 提示

- 确认每个节点请求的任务数与应用程序设计保持一致。
- 保留生成的 `machinefile` 以便事后诊断，它记录了实际分配的节点。
- 作业结束后结合 `ceff` 评估 CPU 利用效率。
