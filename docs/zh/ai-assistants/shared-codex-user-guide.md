# 使用集群共享 Codex

集群共享 Codex 可以帮助你编写鹤思作业脚本、查询本人作业、解释调度状态，并根据
命令输出和工作目录中的文件排查问题。管理员已配置模型服务和鹤思 Skill，你不需要
获取共享 API Key。

## 启动

登录管理员指定的节点，将尖括号占位符替换为实际路径，进入自己的工作目录后直接运行：

```bash
cd <你的工作目录>
codex
```

进入对话后直接描述目标。提供作业 ID、程序入口和预期行为即可；缺少的本人作业信息，
Codex 可以按需查询。

## Agent 如何工作

集群版默认允许 Codex 执行以下只读命令，无需每次请求批准：

- `cqueue`：查询当前队列和本人活动作业；
- `cacct`：查询作业历史、状态和退出码；
- `ccontrol show job` / `ccontrol show step`：查看作业或作业步详情。

这些命令仍以你的身份运行，CraneSched 服务端权限决定可见范围。站点或用户可以配置
更严格的规则。Codex 应优先查询明确的作业 ID 或本人作业，避免持续刷新或宽范围查询。

Codex 还可以在当前工作目录中查找脚本、日志和已有环境，并运行本地检查；这些操作受
Codex 沙箱、文件权限和站点策略约束；操作需要额外访问权限时，命令行是否提示审批取决于
生效的配置。

写入脚本、执行 `cbatch`、取消作业或修改作业状态会产生副作用，不属于上述默认只读
名单。处理这类任务时，应要求 Codex 先说明操作和影响，并等待你的明确确认。命令行是否
还会显示 Codex 内置审批界面，取决于生效的站点和用户配置。你可以拒绝或要求修改方案。

以下对话均为示例，不是来自真实集群；命令输出和站点名称仅用于展示工作方式。

## 示例一：解释基本状态

**你：**

> 作业显示 `Running`，是否说明训练已经成功？

**Codex（示例响应）：**

> `Running` 只表示资源已分配且作业正在执行，不能证明训练结果正确。应继续检查 stdout、
> stderr 和应用指标；作业结束后再结合 `cacct` 的 `State` 与 `ExitCode` 判断是否成功。

## 示例二：生成脚本草稿

**你：**

> 为 `train.py` 生成一个脚本草稿。本站参数是分区 `gpu-a100`、账户 `ml`、QoS
> `normal`；申请 1 张 A100、4 个 CPU、16G 内存和 2 小时。先不要写文件或提交。

**Codex（示例响应）：**

> 我会只在对话中生成草稿，不修改文件或提交作业：
>
> ```bash
> #!/bin/bash
> #CBATCH --job-name=train
> #CBATCH --partition=gpu-a100
> #CBATCH --account=ml
> #CBATCH --qos=normal
> #CBATCH --nodes=1
> #CBATCH --ntasks-per-node=1
> #CBATCH --cpus-per-task=4
> #CBATCH --mem=16G
> #CBATCH --gres=gpu:a100:1
> #CBATCH --time=02:00:00
> #CBATCH --output=train_%j.out
> #CBATCH --error=train_%j.err
>
> python train.py
> ```

## 示例三：主动查询当前作业

**你：**

> 帮我看看现在有哪些作业需要关注。

**Codex 调用只读工具：**

```bash
cqueue --self -o="%j %P %n %t %l %N %L"
ccontrol show job 4812
```

**工具返回的关键信息（示例）：**

```text
JOBID PARTITION NAME       STATUS  TIMELIMIT NODES NODELIST(REASON)
4812  gpu-a100  train      Pending 02:00:00  1     (Resource)
4815  cpu       preprocess Running 01:00:00  1     cn032

JobId=4812 JobState=Pending Account=ml Qos=normal Partition=gpu-a100
Reason=Resource TimeLimit=02:00:00 ReqRes:node=1 cpu=4 mem=16G gres=gpu:a100:1
```

**Codex（示例响应）：**

> 你有 2 个活动作业：
>
> - `4815` 正在 `cpu` 分区的 `cn032` 上运行；当前没有调度异常。
> - `4812` 已成功进入队列，但在等待能够满足所示完整请求的资源，其中包括 1 张 A100、
>   4 CPU 和 16G 内存；当前原因是 `Resource`，不是提交失败。账户 `ml`、QoS `normal`
>   和 2 小时时限也已由作业详情确认。
>
> 当前无需重投。`Resource` 只说明此刻没有满足整个请求的可用资源，不能据此承诺开始时间。

## 示例四：组合历史、详情和文件定位失败

**你：**

> 我的作业 4930 失败了，请直接帮我查原因。

**Codex 查询作业数据并检查工作目录：**

```bash
cacct -j 4930 -F -o "%j %t %e %P %a"
pwd
rg --files | rg '4930|train.*\.(out|err)$'
sed -n '1,160p' train.sh
sed -n '1,120p' train_4930.err
rg -n 'venvs/.*/bin/python|source .*/bin/activate' --glob '*.sh'
test -x /home/alice/venvs/torch/bin/python
```

**为便于阅读而分组的关键结果（示例）：**

```text
JOBID STATE  EXITCODE PARTITION ACCOUNT
4930  Failed 1:0      gpu-a100  ml

/home/alice/project
train_4930.err
```

```bash
#!/bin/bash
#CBATCH --job-name=train
#CBATCH --partition=gpu-a100
#CBATCH --output=train_%j.out
#CBATCH --error=train_%j.err

python train.py
```

```text
ModuleNotFoundError: No module named 'torch'
tools/check_gpu.sh:8:/home/alice/venvs/torch/bin/python -c 'import torch; print(torch.__version__)'
候选解释器可执行（`test` 退出状态为 0）。
```

**Codex（示例响应）：**

> 已确认作业进入 `gpu-a100` 分区并由应用返回退出码 1，没有信号终止的证据。错误发生在
> Python 导入阶段；完整的 `train.sh` 使用未限定路径的 `python`，没有显式加载模块、激活
> 虚拟环境或指定解释器路径。因此当前最早且直接的失败原因是该脚本选中的 Python 环境
> 无法导入 `torch`，不是排队或超时。
>
> 项目的另一份脚本已使用可执行的 `/home/alice/venvs/torch/bin/python` 检查 `torch`；
> 这是有项目文件依据的候选方案，但还不能证明它在计算节点可用。建议把训练命令改为：
>
> ```bash
> /home/alice/venvs/torch/bin/python train.py
> ```
>
> 这会修改 `train.sh`，但不会提交作业。是否批准我修改并运行 `bash -n train.sh` 验证？
> 修改后仍需通过新作业确认该环境在计算节点可用。

## 示例五：查找配置、写入脚本并提交

**你：**

> 参考我最近成功的同类训练作业，为当前 `train.py` 准备脚本并提交。每一步变更先让我确认。

**Codex 先查询历史并检索项目文件：**

```bash
id -un
cacct -u alice -n train -t completed -m 10 \
  --type Batch -o "%j %n %P %a %q %C %M %l %t %e %s"
rg -n '^#CBATCH --(gres|partition|account|qos|cpus-per-task|mem|time)=' \
  /home/alice/project --glob '*.sh'
```

**工具返回的关键信息（示例）：**

```text
alice
JOBID JOBNAME PARTITION ACCOUNT QOS    REQCPUS REQMEM TIMELIMIT STATE     EXITCODE SUBMITTIME
4701  train   gpu-a100  ml      normal 4.00    16G    02:00:00  Completed 0:0      2026-08-10 09:42:11
4638  train   gpu-a100  ml      normal 4.00    16G    02:00:00  Completed 0:0      2026-08-07 16:18:03
/home/alice/project/train_4701.sh:4:#CBATCH --partition=gpu-a100
/home/alice/project/train_4701.sh:5:#CBATCH --account=ml
/home/alice/project/train_4701.sh:6:#CBATCH --qos=normal
/home/alice/project/train_4701.sh:10:#CBATCH --gres=gpu:a100:1
```

**Codex（示例响应）：**

> 返回记录中提交时间最新的成功同类作业是 `4701`，状态 `Completed`、退出码 `0:0`；
> `cacct` 确认其使用 `gpu-a100` 分区、`ml` 账户、`normal` QoS、4 CPU、16G 内存和
> 2 小时。项目中的
> `train_4701.sh` 还确认了 `#CBATCH --gres=gpu:a100:1`。我准备以这些有来源的值创建
> `train.sh`，入口为当前目录的 `train.py`，暂不提交。是否批准写入该文件？

**你：**

> 批准。

**Codex 写入并验证后响应：**

> 已创建 `train.sh`，`bash -n train.sh` 通过。提交会创建作业并消耗集群资源。
> 是否批准执行 `cbatch train.sh`？

**你：**

> 批准提交。

**Codex 提交并自动验证：**

```bash
cbatch train.sh
cqueue -j 5012 -o="%j %P %n %t %l %N %L"
ccontrol show job 5012
```

**工具返回的关键信息（示例）：**

```text
Job id allocated: 5012
JOBID PARTITION NAME  STATUS  TIMELIMIT NODES NODELIST/REASON
5012  gpu-a100  train Pending 02:00:00  1     Priority
JobId=5012 JobName=train
        User=alice(1001) Account=ml
        JobState=Pending TimeLimit=02:00:00 Partition=gpu-a100
        Priority=874 Qos=normal
        ReqRes:node=1 cpu=4.00 mem=16G gres=gpu:a100:1
```

**Codex（示例响应）：**

> 提交成功，新作业 ID 为 `5012`。查询确认脚本使用 `gpu-a100` / `ml` / `normal`，
> 请求 1 张 A100、4 CPU、16G 内存和 2 小时；当前状态为 `Pending`，原因是 `Priority`。
> 作业已正常进入队列，无需再次提交。

## 使用边界

- Codex 可以依据实际查询结果给出具体结论，但不会获得超出你账户的 CraneSched 权限。
- 站点文档和当前命令的 `--help` 优先；执行前仍应检查生成的脚本和变更摘要。
- 不要提供密码、API Key、私钥或完整环境变量。
- 管理节点、分区、账户、QoS、服务和系统配置仍由管理员负责。
