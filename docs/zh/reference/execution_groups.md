# 执行组列表

鹤思在每个作业和作业步请求中使用一个有序的 GID 列表：

```text
gids[0] = 有效（主）GID
gids[1:] = supplementary GID
```

前端收集有效 GID 和进程的 supplementary group，去重后保证有效 GID
位于首项。`job/step.uid + gids` 始终表示提交者的宿主机执行身份，
包括容器作业的批处理脚本和通过 `ccon run` 提交的子步骤。

后端在每个执行节点按 `job/step.uid` 执行最终校验。`gids[0]` 必须存在于该用户在节点的 NSS
组集合中；主 GID 缺失时在 payload 启动前失败。supplementary GID 只取
与节点实际组的交集，缺失项记录有界 warning 后继续执行；节点存在但前端
未请求的额外组不会自动加入。

多节点作业步必须先由所有分配节点完成主 GID 校验，才能确认分配成功。
任一节点失败都会阻止部分 payload 启动，并继续执行原有状态上报、cgroup
清理和资源释放。

## 容器身份

`job/step.uid + gids` 表示提交者的宿主机身份，`pod_meta.run_as_user/run_as_group`
表示容器内身份。`ccon --user` 和 `cbatch --pod-user` 不会覆盖提交者的组列表，
也不会按容器 UID 查询宿主机 NSS。

- **未启用 user namespace**：容器以提交者的 UID、有效 GID 和节点校验后的补充组启动。
  显式指定的 UID/GID 必须与提交者 UID/EGID 一致，root 提交者也遵循此规则。
- **启用 user namespace**：默认容器身份为 `0:0`，可用 `--user` / `--pod-user`
  指定节点映射范围内的 UID/GID。提交组列表中只要存在不同于
  EGID 的组，就拒绝提交，不会静默丢弃；重复的 EGID 不算额外补充组。
  容器 UID/GID 无须存在于宿主机 NSS。

只指定 UID 时，省略的 GID 使用该模式的默认值：userns 为 0，非 userns 为提交者 EGID。
两个模式均设置 CRI `SupplementalGroupsPolicy=Strict`，避免镜像的
`/etc/group` 自动追加组；容器运行时须支持此策略。

独立容器作业在前端取得提交身份、解析脚本和命令行中的 userns 选项后立即校验。
容器子步骤继承父作业的 Pod 设置，在调度端取得父作业后校验。
调度端和执行节点均有校验，执行节点在 NSS 组交集处理前拒绝 userns 的额外组，
避免不支持的组被交集处理掩盖。

userns 的 UID/GID mapping 使用该账户现有的 SubUID/SubGID 区间；SubGID 自动分配
仍以账户 NSS 主 GID 选择区间，不因某次提交的 EGID 改变而重新分配。
namespace 映射始终从容器 ID 0 开始，不随指定的运行身份变化；节点在启动 payload 前，
分别按实际 UID/GID 映射区间校验指定值。
原生 idmapped mount 和 bindfs 均把提交者 UID/EGID 对应到指定的容器 UID/GID（默认 `0:0`），
挂载转换和新文件创建组不再假定 EGID 等于 NSS 主 GID。
目前不支持 userns 下继承宿主机补充组的挂载权限。
