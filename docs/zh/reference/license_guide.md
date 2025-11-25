# 许可证使用指南

Crane 可以通过在调度时为作业分配可用的许可证来帮助管理软件许可证。
如果许可证不可用，作业将保持排队状态，直到许可证可用为止。
在 Crane 中，许可证本质上是共享资源，意思是这类配置的资源并不绑定到某一台特定主机，
而是与整个集群相关联。

在 Crane 中，许可证目前可以通过下面方式进行配置：

* 本地许可证：指在某个 `crane/config.yaml` 中配置的许可证，只在使用该 `crane/config.yaml` 的集群内本地有效。


## 本地许可证

本地许可证在 `crane/config.yaml` 中使用 `Licenses` 选项定义。

```Yaml
Licenses:
  - name: fluent
    quantity: 30
  - name: ansys
    quantity: 100
```

可以使用 ccontrol 命令查看已配置的许可证。
```bash
ccontrol show lic
```
![ccontrol](../../images/ccontrol/ccontrol_showlic.png)


通过提交选项 `-L` 或 `--licenses` 来请求许可证，支持同时请求多个许可证。
```bash
cbatch -L fluent:2 script.sh
# 与关系，同时满足所有条件才能运行
cbatch -L fluent:2,ansys:1 script.sh 
# 或关系，任意条件满足即可运行。
# 调度器会优先尝试匹配列表中的第一个许可证，如果不可用，则尝试第二个，以此类推。
cbatch -L fluent:2|ansys:1 script.sh
```
