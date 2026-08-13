# 使用集群共享 Codex

集群共享 Codex 可以帮助你编写鹤思作业脚本、理解作业状态，以及根据命令输出排查
常见问题。管理员已在共享节点上配置模型服务和鹤思 Skill，你不需要获取共享 API Key。

## 启动

登录管理员指定的节点，将尖括号占位符替换为实际路径，进入自己的工作目录后直接运行：

```bash
cd <你的工作目录>
codex
```

进入对话后直接用自然语言描述目标，并尽量提供程序入口、资源需求、作业 ID、完整报错
和相关命令输出。若找不到 `codex` 命令或无法连接模型服务，请联系集群管理员；不要尝试
读取或修改系统 Codex 配置。

以下对话均为示例，不是来自真实集群。实际回答会随你的输入、本站策略和已安装版本而变化。

## 示例一：生成作业脚本

**你：**

> 请为 `train.py` 写一个鹤思批处理脚本：使用 1 张 GPU、4 个 CPU、16G 内存，
> 最长运行 2 小时。我还不知道本站的 GPU 分区名。

**Codex（示例响应）：**

> 先保留分区占位符，请按本站文档替换 `<gpu_partition>`：
>
> ```bash
> #!/bin/bash
> #CBATCH --job-name=train
> #CBATCH --partition=<gpu_partition>
> #CBATCH --nodes=1
> #CBATCH --ntasks-per-node=1
> #CBATCH --cpus-per-task=4
> #CBATCH --mem=16G
> #CBATCH --gres=gpu:1
> #CBATCH --time=02:00:00
> #CBATCH --output=train_%j.out
> #CBATCH --error=train_%j.err
>
> python train.py
> ```
>
> 保存为 `train.sh`，先用 `bash -n train.sh` 检查 shell 语法，再运行
> `cbatch train.sh`。如果本站要求账户、QoS 或环境模块，请按用户手册补充；不要猜测这些值。

## 示例二：理解排队原因

**你：**

> 我的作业 4812 一直是 `Pending`，`cqueue -j 4812 -F` 显示
> `Reason=Resources`。这是提交失败了吗？

**Codex（示例响应）：**

> 这不表示提交失败。已确认的信息是：作业仍在队列中，当前最高优先级阻塞原因与可用
> 资源有关；仅凭这一行无法判断准确开始时间。可以继续查看本人作业详情：
>
> ```bash
> ccontrol show job 4812
> ```
>
> 核对请求的 GPU、节点数、内存和时限是否符合预期。若原因长期不变，请把作业 ID、
> 提交时间、资源请求和上述输出交给管理员，不要反复重投同一个作业。

## 示例三：排查失败作业

**你：**

> `cacct -j 4930 -F` 显示 `State=Failed`、`ExitCode=1:0`，错误文件中第一条错误是
> `ModuleNotFoundError: No module named 'torch'`。应该先查什么？

**Codex（示例响应）：**

> `ExitCode=1:0` 表示应用返回了退出码 1，没有信号终止的证据。结合第一条错误，较可能是
> 批作业环境没有加载包含 `torch` 的 Python 环境。先检查脚本是否显式加载本站推荐的模块，
> 或激活位于共享存储上的虚拟环境，例如：
>
> ```bash
> # 按本站实际环境选择一种方式
> module load <python_or_ai_module>
> # 或
> source <shared_venv>/bin/activate
> python -c 'import torch; print(torch.__version__)'
> python train.py
> ```
>
> 不要猜模块名或环境路径。若你提供当前脚本和本站环境说明，我可以帮你把初始化步骤放到
> 正确位置；请勿粘贴密码、令牌或完整环境变量。

## 使用边界

- Codex 给出的命令和脚本应在执行前由你检查，站点文档和命令的 `--help` 优先。
- 提供最少且相关的输出；不要发送密码、API Key、私钥或完整环境变量。
- Codex 不会获得额外的 CraneSched 权限，也不能代替管理员确认分区、账户、QoS 或集群状态。
- 取消作业、提交作业或修改文件前，确认目标只属于你且影响范围符合预期。
