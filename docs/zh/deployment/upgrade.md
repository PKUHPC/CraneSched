# 升级指南

本文面向控制节点管理员，说明当前鹤思数据库升级的实际流程：升级前备份什么、
按什么顺序停机和启动、如何验证，以及失败时如何回滚。适用场景是从 v1.1.3
数据库升级到包含 v0 到 v1 schema 迁移的版本。

!!! warning
    当前版本不能保证 pending 或 running 作业的前向兼容。停止 CraneCtld
    前必须排空或取消所有活动作业。当前流程不会从旧 EmbeddedDB 恢复活动作业的
    运行时状态。后续前向兼容性请关注
    [issue #950](https://github.com/PKUHPC/CraneSched/issues/950)。

## 需要备份的数据

创建一个带时间戳的备份目录，确认空间足够，并在升级验证完成前保留它。

| 数据 | 备份内容 | 用途 |
| --- | --- | --- |
| 配置 | `/etc/crane/`，包括 `config.yaml`、`database.yaml`、`plugin.yaml`、证书和本地覆盖配置 | 恢复原服务和数据库设置 |
| MongoDB | `database.yaml` 中 `DbName` 指定的整个数据库和全部 collection | 保存作业历史、账户、用户、QoS 及迁移源数据 |
| EmbeddedDB | `CraneCtldDbPath` 对应的全部文件 | 保存运行时状态和 next-ID 计数器，用于回滚 |

如果 `CraneCtldDbPath` 是相对路径，要先根据 `config.yaml` 中的
`CraneBaseDir` 解析；Keepalived 环境使用 `CraneSharedBaseDir`。RocksDB 默认
实际目录为 `<解析后的 CraneCtldDbPath>.rocksdb/`。v1.1.3 使用 Unqlite 或
BerkeleyDB legacy 后端时，对应的三个文件是：

```text
<解析后的 CraneCtldDbPath>var
<解析后的 CraneCtldDbPath>fix
<解析后的 CraneCtldDbPath>resv
```

上面三个文件就是 v1.1.3 完整的 legacy EmbeddedDB 文件集合。以运行中实际存在
的路径为准；不确定时备份整个父目录。MongoDB 备份示例（替换占位符，密码可
交互式输入）：

```bash
mongodump --host <DbHost> --port <DbPort> \
  --username <DbUser> --authenticationDatabase admin \
  --db <DbName> --out <备份目录>/mongodb
```

把鹤思版本、软件包/Git 版本、`DbName`、解析后的 EmbeddedDB 路径和备份时间
一并记录，不能覆盖已有备份。

## 升级流程

以下步骤必须在同一个维护窗口内完成。

### 1. 阻止新提交并排空作业

在登录网关或权限系统暂时阻止 `cbatch`、`crun`、`calloc`、`ccon` 等提交入口，
在验证全部通过前保持阻止状态。

先取消尚未启动的作业；可以等待运行作业自然结束：

```bash
ccancel -t Pending
cqueue -t all --json
```

`cqueue` 命令必须返回空的 `job_info_list`。如果运行作业无法在维护窗口
内结束，再显式取消并重新检查：

```bash
ccancel -t Running
cqueue -t all --json
```

仍有 pending 或 running 作业时不得继续。

### 2. 停止鹤思服务

在对应主机执行。先停止提交和交互前端，再停止计算节点守护进程、调度器和
插件服务：

```bash
sudo systemctl stop cfored
sudo systemctl stop craned       # 每个计算节点
sudo systemctl stop cranectld
sudo systemctl stop cplugind
```

确认没有旧 Crane 进程仍打开 EmbeddedDB。不要停止 `mongod`，它可以继续运行
以便导出数据库。

### 3. 备份并清理 EmbeddedDB

进程停止后，复制 `/etc/crane/`，对 `DbName` 执行 `mongodump`，并复制全部
EmbeddedDB 文件。优先把在线数据库移动到带日期的备份位置，不要直接删除：

```bash
sudo mv <解析后的 CraneCtldDbPath>.rocksdb \
  <备份目录>/embedded.db.rocksdb.v1.1.3
```

使用 legacy 后端时同样移动 `var`、`fix` 和 `resv` 三个文件。本次升级会以空
EmbeddedDB 启动新版本，不要在启动前恢复旧的运行时数据库。

### 4. 启动新版本并恢复 ID 计数器

安装新版本软件包或二进制；除非发行说明要求，否则保留现有配置。按依赖顺序
启动：

```bash
sudo systemctl daemon-reload
sudo systemctl start cplugind
sudo systemctl start cranectld
sudo systemctl start craned       # 每个计算节点
sudo systemctl start cfored
```

启动时 CraneCtld 会自动把 MongoDB schema 从 v0 迁移到 v1：复制并转换源数据，
然后交换为 `job_table`，原 collection 保留为 `task_table_backup_v0`。升级验收
和备份保留期结束前不要删除该备份 collection。

由于 EmbeddedDB 已被清理，开放新提交前要查询 MongoDB 中已持久化的最大 ID。
`cranectld` 软件包安装的只读工具要求 `PATH` 中存在 `python3` 和 `mongosh`；
指定 `--username` 时，`mongosh` 会提示输入密码：

```bash
crane-query-next-job-ids \
  --host <DbHost> --port <DbPort> \
  --username <DbUser> --database <DbName>
```

如果数据库中两个最大 ID 都是 `31`，输出末尾会给出：

```bash
ccontrol reset next-job-id 32
ccontrol reset next-job-db-id 32
```

核对后执行工具输出的两条命令。collection 为空时工具使用 `1`；MongoDB 未启用
认证时省略 `--username`。

清空 EmbeddedDB 也会清除计数器；不执行这一步，新作业可能复用 MongoDB 中已有
的 ID。

## 验证

解除提交阻止前完成以下全部检查。

### 服务和 schema

```bash
systemctl is-active cranectld craned cfored cplugind
journalctl -u cranectld -b --no-pager | \
  rg "schema version|Migrating schema v0 -> v1|Schema migration v0 -> v1 completed|Migrated db schema"
```

日志必须显示 v0 到 v1 成功，不能有 migration error。直接检查版本和 collection
交换结果：

```javascript
use <DbName>
db.metadata_table.findOne({_id: "db_schema_version"})
db.getCollectionNames().filter(n => /^(task_table|job_table)(_backup_v0)?$/.test(n))
```

metadata 文档必须包含 `version: 1`；`job_table` 必须存在；
`task_table_backup_v0` 应保留用于回滚。

### 队列、cacct 和一致性

```bash
cqueue -t all --json
cacct -t all -F
cacct -t all --json
```

至少检查一个已完成的历史作业，确认 job ID、状态、CPU、内存、提交时间和结束
时间正确。

### 新作业冒烟测试

提交一个 held canary，确认编号大于所有历史 ID，然后取消并用 `cacct` 读取：

```bash
cbatch --hold --json -J upgrade-canary --wrap 'true'
cqueue -t all --json
ccancel <canary-job-id>
cacct -j <canary-job-id> --json
```

canary 必须能在 `cqueue` 中看到，取消成功，且取消后的记录能通过 `cacct` 读取。
全部通过后才解除提交阻止。

## 回滚

出现以下任一情况必须回滚：CraneCtld 无法启动、日志报告 migration error、
schema 不是 `1`，或历史作业无法读取：

1. 重新阻止提交，停止新版本的 `cfored`、`craned`、`cranectld`、`cplugind`。
2. 从备份恢复 `/etc/crane/`。
3. 先确认数据库名称，再只删除配置中的 `<DbName>`，从 MongoDB dump 恢复该
   数据库，避免 dump 中不存在的旧 collection 残留：

   ```bash
   mongosh --host <DbHost> --port <DbPort> <DbName> \
     --eval 'db.dropDatabase()'
   mongorestore --host <DbHost> --port <DbPort> \
     --db <DbName> <备份目录>/mongodb/<DbName>
   ```

4. 把 EmbeddedDB 文件恢复到原始绝对路径。
5. 启动旧版本服务，确认 `cqueue` 和 `cacct` 正常后再开放提交。

回滚必须同时恢复备份的配置、MongoDB 数据库和 EmbeddedDB 快照。不要把 v1.1.3
EmbeddedDB 放入新版本后继续运行，造成混合状态。
