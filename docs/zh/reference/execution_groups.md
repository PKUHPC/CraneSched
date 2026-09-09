# 执行组列表

鹤思在每个作业和作业步请求中使用一个有序的 GID 列表：

```text
gids[0] = 有效（主）GID
gids[1:] = supplementary GID
```

前端收集有效 GID 和进程的 supplementary group，去重后保证有效 GID
位于首项。对于显式容器参数 `--user uid:gid`，提交前通过 NSS 查询目标
用户的组列表。

后端在每个执行节点执行最终校验。`gids[0]` 必须存在于该节点的 NSS
组集合中；主 GID 缺失时在 payload 启动前失败。supplementary GID 只取
与节点实际组的交集，缺失项记录有界 warning 后继续执行；节点存在但前端
未请求的额外组不会自动加入。

多节点作业步必须先由所有分配节点完成主 GID 校验，才能确认分配成功。
任一节点失败都会阻止部分 payload 启动，并继续执行原有状态上报、cgroup
清理和资源释放。

这是一次破坏性协议升级。Backend、FrontEnd、Craned、Cfored 和客户端必须
在同一个版本窗口切换，禁止旧 scalar GID 客户端与新 Backend 混跑，也不要
在部署过程中混用不同版本。
