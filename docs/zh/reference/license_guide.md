# 许可证使用指南

Crane 可以通过在调度时为作业分配可用的许可证来帮助管理软件许可证。
如果许可证不可用，作业将保持排队状态，直到许可证可用为止。
在 Crane 中，许可证本质上是共享资源，意思是这类配置的资源并不绑定到某一台特定主机，
而是与整个集群相关联。

在 Crane 中，许可证目前可以通过下面方式进行配置：

* 本地许可证：指在某个 `/etc/crane/config.yaml` 中配置的许可证，只在使用该 `/etc/crane/config.yaml` 的集群内本地有效。
* 远程许可证：远程许可证由数据库提供，并通过 cacctmgr 命令进行配置。
远程许可证具有动态特性。

## 本地许可证

本地许可证在 `/etc/crane/config.yaml` 中使用 `Licenses` 选项定义。

```Yaml
Licenses:
  - name: fluent
    quantity: 30
  - name: ansys
    quantity: 100
```

可以使用 ccontrol 命令查看已配置的许可证。
```bash
$ ccontrol show lic
LicenseName=ansys
        Total=100 Used=0  Free=100
LicenseName=fluent
        Total=30 Used=0  Free=30
```


通过提交选项 `-L` 或 `--licenses` 来请求许可证，支持同时请求多个许可证。
```bash
cbatch -L fluent:2 script.sh
# 与关系，同时满足所有条件才能运行
cbatch -L fluent:2,ansys:1 script.sh 
# 或关系，任意条件满足即可运行。
# 调度器会优先尝试匹配列表中的第一个许可证，如果不可用，则尝试第二个，以此类推。
cbatch -L fluent:2|ansys:1 script.sh
```

## 远程许可证
远程许可证（Remote licenses）本身不提供与第三方许可证管理器的任何集成。
在创建这些许可证时使用 "Server" 和 "ServerType" 参数仅用于信息记录，
并不意味着 Crane 会与这些服务器进行任何自动的许可证管理。
系统管理员有责任自行实现与这些系统的集成。 
例如，这包括确保只有通过 Crane 请求远程许可证的用户才能从许可证服务器检出许可证，
或者确保 Crane 的许可证数量与许可证服务器保持同步（参见下面动态许可证）。

### 使用场景
某站点有两个许可证服务器，一个由 FlexNet 提供 100 个 Nastran 许可证，
另一个由 Reprise License Management 提供 50 个 Matlab 许可证。
该站点有两个集群 “fluid” 和 “pdf”，用于运行两个软件的模拟作业。
管理员希望将 Nastran 许可证在两个集群之间平均分配，
而 Matlab 许可证的 70% 分配给 “pdf” 集群，剩下的 30% 分配给 “fluid” 集群。

使用 cacctmgr 命令添加许可证时，需要指定许可证总数，
以及应当分配给每个集群的百分比。此操作可以一步完成，也可以多步完成。

**一步方式：**
```bash
$ cacctmgr add resource nastran cluster=fluid,pdf \
  server=flex_host servertype=flexlm \
  count=100 allowed=50 type=license
Resource added successfully
```

**多步方式：**
```bash
$ cacctmgr add resource matlab count=50 server=rlm_host \
  servertype=rlm type=license
Resource added successfully
$ cacctmgr add resource matlab server=rlm_host \
  cluster=pdf allowed=70
Resource added successfully 
$ cacctmgr add resource matlab server=rlm_host \
  cluster=fluid allowed=30
Resource added successfully
```

cacctmgr 命令现在将显示许可证的总数。
```bash
$ cacctmgr show resource
|---------|-----------|---------|-------|--------------|-----------|------------|-------|
| NAME    | SERVER    | TYPE    | COUNT | LASTCONSUMED | ALLOCATED | SERVERTYPE | FLAGS |
|---------|-----------|---------|-------|--------------|-----------|------------|-------|
| matlab  | rlm_host  | License | 50    | 0            | 100       | rlm        |       |
| nastran | flex_host | License | 100   | 0            | 100       | flexlm     |       |
|---------|-----------|---------|-------|--------------|-----------|------------|-------|
$ cacctmgr show resource withclusters
|---------|-----------|---------|-------|--------------|-----------|------------|----------|---------|-------|
| NAME    | SERVER    | TYPE    | COUNT | LASTCONSUMED | ALLOCATED | SERVERTYPE | CLUSTERS | ALLOWED | FLAGS |
|---------|-----------|---------|-------|--------------|-----------|------------|----------|---------|-------|
| matlab  | rlm_host  | License | 50    | 0            | 100       | rlm        | fluid    | 30      |       |
| matlab  | rlm_host  | License | 50    | 0            | 100       | rlm        | pdf      | 70      |       |
| nastran | flex_host | License | 100   | 0            | 100       | flexlm     | fluid    | 50      |       |
| nastran | flex_host | License | 100   | 0            | 100       | flexlm     | pdf      | 50      |       |
|---------|-----------|---------|-------|--------------|-----------|------------|----------|---------|-------|
```
配置好的许可证现在可以通过 scontrol 命令在两个集群上查看。
```bash
# On cluster "pdf"
$ scontrol show lic
LicenseName=matlab@rlm_host
    Total=35 Used=0 Free=35 Reserved=0 Remote=yes
    LastConsumed=0 LastDeficit=0 LastUpdate=2025-11-28T13:05:44
LicenseName=nastran@flex_host
    Total=50 Used=0 Free=50 Reserved=0 Remote=yes
    LastConsumed=0 LastDeficit=0 LastUpdate=2025-11-28T13:05:44
    
# On cluster "fluid"
$ scontrol show lic
LicenseName=matlab@rlm_host
    Total=15 Used=0 Free=15 Reserved=0 Remote=yes
    LastConsumed=0 LastDeficit=0 LastUpdate=2025-11-28T13:05:44
LicenseName=nastran@flex_host
    Total= Used=0 Free=50 Reserved=0 Remote=yes
    LastConsumed=0 LastDeficit=0 LastUpdate=2025-11-28T13:05:44
```

提交需要远程许可证的作业时，必须指定许可证的名称和服务器。
```bash
$ cbatch -L nastran@flex_host script.sh
```

许可证的百分比和数量可以按照如下方式进行修改：
```bash
$ cacctmgr modify resource name=matlab server=rlm_host set \
  count=200
Modify information succeeded
$ cacctmgr modify resource name=matlab server=rlm_host \
  cluster=pdf set allowed=60
Modify information succeeded
$ cacctmgr show resource withclusters
|---------|-----------|---------|-------|--------------|--------------|------------|----------|------------|-------|
| NAME    | SERVER    | TYPE    | COUNT | LASTCONSUMED | ALLOCATED(%) | SERVERTYPE | CLUSTERS | ALLOWED(%) | FLAGS |
|---------|-----------|---------|-------|--------------|--------------|------------|----------|------------|-------|
| matlab  | rlm_host  | License | 200   | 0            | 90           | rlm        | pdf      | 60         |       |
| matlab  | rlm_host  | License | 200   | 0            | 90           | rlm        | fluid    | 30         |       |
| nastran | flex_host | License | 100   | 0            | 100          | flexlm     | fluid    | 50         |       |
| nastran | flex_host | License | 100   | 0            | 100          | flexlm     | pdf      | 50         |       |
|---------|-----------|---------|-------|--------------|--------------|------------|----------|------------|-------|
```

许可证可以按如下方式在某个集群上删除，也可以全部一起删除：
```bash
$ cacctmgr delete resource matlab server=rlm_host cluster=fluid
Successfully deleted Resource 'matlab@rlm_host'.
$ cacctmgr delete resource nastran server=flex_host
Successfully deleted Resource 'nastran@flex_host'.
$ cacctmgr show resource withclusters
|--------|----------|---------|-------|--------------|--------------|------------|----------|------------|-------|
| NAME   | SERVER   | TYPE    | COUNT | LASTCONSUMED | ALLOCATED(%) | SERVERTYPE | CLUSTERS | ALLOWED(%) | FLAGS |
|--------|----------|---------|-------|--------------|--------------|------------|----------|------------|-------|
| matlab | rlm_host | License | 200   | 0            | 60           | rlm        | pdf      | 60         |       |
|--------|----------|---------|-------|--------------|--------------|------------|----------|------------|-------|
```

如果设置 Absolute 标志，表示每个集群的许可证 allowed 值将被视为绝对数量，而不是百分比。

以下是一些使用该标志进行许可证管理的简单示例。

```bash
$ cacctmgr add resource deluxe cluster=fluid,pdf count=150 allowed=70 \
  server=flex_host servertype=flexlm flags=absolute

$ cacctmgr show resource withclusters

$ cacctmgr update resource deluxe set allowed=25 where cluster=fluid

$ cacctmgr show resource withclusters

```
你也可以通过在 `/etc/crane/config.yaml` 中添加 `AllLicenseResourcesAbsolute=yes`，
将其设为所有新建许可证的默认设置（并重启 CraneCtld 使更改生效）。

## 动态许可证
远程许可证的 `LastConsumed` 字段被设计为定期从许可证服务器获取并更新当前的使用数量。
下面提供了一个用于 `FlexLM` 的 `lmstat` 命令的示例脚本——对于其他许可证管理系统，也可以很容易地编写类似的脚本。

```bash
#!/bin/bash

set -euxo pipefail

LMSTAT=/opt/foobar/bin/lmstat
LICENSE=foobar@db

consumed=$(${LMSTAT} | grep "Users of ${LICENSE}"|sed "s/.*Total of \([0-9]\+\) licenses in use)/\1/")

cacctmgr update resource ${LICENSE} set lastconsumed=${consumed}
```

当通过 `cacctmgr` 修改 `LastConsumed` 值时，更新会自动推送到 `Crane` 控制器。
控制器会使用该值计算 `LastDeficit` 值——这个值表示从集群角度来看“丢失”的许可证数量，需要暂时预留出来。

例如，在该集群上有 100 个 "foobar" 许可证可用，我们正在为 "blackhole" 集群分配其中的 80 个许可证：
```bash
$ cacctmgr add resource foobar server=db count=100 flags=absolute cluster=blackhole allowed=80

$ ccontrol show lic

```
现在，我们的定时任务（cron job）将 LastConsumed 值更新为 30，而集群尚未为作业分配任何许可证：
```bash
$ cacctmgr update resource foobar@db set lastconsumed=30

$ ccontrol show lic
LicenseName=foobar@db
    Total=80 Used=0 Free=70 Reserved=0 Remote=yes
    LastConsumed=30 LastDeficit=10 LastUpdate=2025-11-28T16:39:27
```

请注意，集群现在已经计算出有 10 个许可证的缺口，并且已经意识到目前最多只能调度 70 个许可证。
集群知道，最多有 20 个许可证被保留给其他集群或外部使用。
然而，由于 LastConsumed 被设置为 30，这意味着还有额外的 10 个许可证“异常丢失”，
其使用情况无法被追踪。因此，集群不能将这些许可证分配给任何等待中的作业，
因为这些作业很可能无法获得所需的许可证，从而导致作业失败。


如果之后的更新（通常由定时任务驱动）将 LastConsumed 数量减少到 10，那么许可证缺口就被认为已经消失，
集群会再次将全部 80 个分配的许可证开放使用：

```bash
$ scacctmgr update resource foobar@db set lastconsumed=20

$ ccontrol show lic
LicenseName=foobar@db
    Total=80 Used=0 Free=80 Reserved=0 Remote=yes
    LastConsumed=20 LastDeficit=0 LastUpdate=2025-11-28T16:44:26
```