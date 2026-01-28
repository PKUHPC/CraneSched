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
| **OutOfMemory** | 作业因使用的内存超过申请量而被 OOM killer 终止。 |
| **Configuring** | 作业资源正在配置中（例如运行 prolog 脚本）。 |
| **Starting** | 作业资源已配置完成，准备就绪。 |
| **Completing** | 作业正在结束过程中（例如运行 epilog 脚本）。 |
