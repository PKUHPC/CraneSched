# 作业状态

CraneSched 作业包含以下状态：

| 状态名称 | 描述 |
| :--- | :--- |
| **Pending** | 作业正在等待资源分配。 |
| **Running** | 作业已获得资源分配，正在执行中。 |
| **Completed** | 作业已成功完成执行（退出码为 0）。 |
| **Failed** | 作业执行结束，退出码非 0。 |
| **ExceedTimeLimit** | 作业因超出申请的时间限制而被终止。 |
| **Cancelled** | 作业已被用户或管理员取消。 |
| **OutOfMemory** | 作业负载遇到 OOM。子进程被 OOM killer 终止后，批处理 shell 仍可能以退出码 0 结束。 |
| **Configuring** | 作业资源正在配置中（例如运行 prolog 脚本）。 |
| **Starting** | 作业资源已配置完成，准备就绪。 |
| **Completing** | 作业正在结束过程中（例如运行 epilog 脚本）。 |

作业状态与退出码分别记录。OOM 检测保留主进程的实际退出码，包括 0，不会伪造退出码 137。
仅 `Completed` 且退出码为 0 满足 `afterok`；`OutOfMemory` 且退出码为 0 满足
`afternotok` 和 `afterany`，控制器重启后恢复依赖时也遵循这一规则。

缺少 OOM-kill 计数器的旧版 cgroup v1 内核只能通知 OOM 状况，不能准确定位被杀进程。
作业级通知可将批处理主步骤标记为 `OutOfMemory`，不会据此把所有兄弟步骤标为 OOM 受害者。
同一 supervisor 内，显式取消、超时、截止时间和 Cfored 断连保留更高优先级；跨执行节点的结果
继续遵循控制器原有的聚合规则。
