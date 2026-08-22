# 从 v1.1.3 升级

本文是控制节点管理员从 CraneSched v1.1.3 升级到当前版本的操作规程。

!!! warning
    当前版本不能保证 pending 或 running 作业的前向兼容。停止 CraneCtld 前必须
    排空或取消所有活动作业。本流程不会把 v1.1.3 EmbeddedDB 中的活动作业状态
    恢复到新版本。后续兼容性工作见
    [issue #950](https://github.com/PKUHPC/CraneSched/issues/950)。

## 准备

升级前，把目标版本源码中的 `scripts/upgrade_data.py` 单独复制到 v1.1.3
控制节点，例如保存为 `/root/crane-upgrade/upgrade_data.py`。该脚本不随
CraneSched RPM/DEB 安装。控制节点需要安装 `python3-pyyaml`（RHEL 系）或
`python3-yaml`（Debian 系），并且必须能执行 `mongosh`、`mongodump`、
`mongorestore` 和 `ccontrol`。以下命令块使用 Bash 语法，并假定在同一个 root
shell 中执行：

```bash
crane_upgrade_tool=/root/crane-upgrade/upgrade_data.py
test -r "$crane_upgrade_tool"
```

工具默认读取 `/etc/crane/config.yaml`，再读取其中 `DbConfigPath` 指定的数据库
配置。`DbName` 是该配置中的 MongoDB 数据库名，v1.1.3 默认值为 `crane_db`。
管理员不需要把 `DbHost`、`DbPort`、`DbUser`、`DbName` 或
`CraneCtldDbPath` 填到命令行。

一次成功的备份包含：

| 路径 | 内容 |
| --- | --- |
| `etc-crane/` | `/etc/crane/` 的完整副本 |
| `database.yaml.active` | `DbConfigPath` 实际指向的数据库配置 |
| `mongodb/` | `DbName` 指定数据库的 `mongodump` |
| `cranectld-runtime/` | `CraneCtldDbPath` 所在的整个 CraneCtld 运行目录 |
| `manifest.json` | 实际配置路径、数据库连接信息、最大 ID 和下一 ID |

备份目录必须位于 CraneCtld 运行目录之外，并保留到升级验收和备份保留期结束。

## 升级 SOP

### 1. 阻止提交并排空作业

在登录网关或权限系统阻止 `cbatch`、`crun`、`calloc`、`ccon` 等提交入口，直到
本 SOP 的验证步骤全部通过。

先取消 pending 作业，允许 running 作业自然结束：

```bash
ccancel -t Pending
cqueue -t all --json
```

如果 running 作业无法在维护窗口内结束，再取消并复查：

```bash
ccancel -t Running
cqueue -t all --json
```

`cqueue` 必须返回空的 `job_info_list`，否则不得继续。

### 2. 停止服务

在对应主机停止服务。每个计算节点都要停止 `craned`：

```bash
sudo systemctl stop cfored
sudo systemctl stop craned
sudo systemctl stop cranectld
sudo systemctl stop cplugind
systemctl is-active cfored cranectld cplugind || true
systemctl is-active craned || true
```

以上检查必须逐项输出 `inactive`。备份脚本还会检查是否存在遗留的 `cranectld`
进程。不要停止 `mongod`，备份工具需要连接它。

### 3. 备份数据库并记录 ID

在 v1.1.3 控制节点的同一个 shell 中执行：

```bash
crane_backup_dir="/var/backups/crane/upgrade-$(date -u +%Y%m%dT%H%M%SZ)"
python3 "$crane_upgrade_tool" --output "$crane_backup_dir"
```

输出目录必须事先不存在，工具会以 `0700` 权限创建它。工具会拒绝在
`cranectld` 仍运行时备份。MongoDB 启用认证时，`mongosh` 和 `mongodump` 会
分别提示输入数据库密码。

工具自动完成以下操作：

1. 从默认 Crane 配置解析 MongoDB 和 EmbeddedDB 实际路径。
2. 查询历史数据中的最大作业 ID 和最大作业数据库 ID。
3. 备份配置、整个 MongoDB 数据库和整个 CraneCtld 运行目录。
4. 在 `manifest.json` 中记录两个独立的最大 ID 和下一可用 ID。

脚本会为配置解析、ID 查询、配置复制、MongoDB 导出、运行目录复制和清单写入分别
打印 `[INFO] Starting`/`[INFO] Completed` 或对应结果。完成时必须看到
`SUCCESS: Backup completed: <备份目录>` 以及两个最大 ID、两个下一 ID。任一步
失败时会打印 `error: <原因>` 并保留 `BACKUP_INCOMPLETE`。

备份完成后，让同一脚本重新读取并校验备份：

```bash
python3 "$crane_upgrade_tool" \
  --validate-backup "$crane_backup_dir"
```

校验完成时必须看到 `SUCCESS: Backup validated: <备份目录>`，以及 MongoDB
数据库名、CraneCtld 运行目录和两个下一 ID。`manifest.json` 不包含 MongoDB
密码。ID 已由脚本查询并保存在清单中，不需要管理员了解 MongoDB 内部表结构、
手工查询 ID 或拼接恢复命令。

### 4. 清空旧 CraneCtld 运行目录

不要手工读取清单或拼接 EmbeddedDB 文件名。让脚本校验备份、确认 `cranectld`
已经停止，并清空清单中记录的运行目录：

```bash
python3 "$crane_upgrade_tool" \
  --clear-runtime-from "$crane_backup_dir"
```

脚本只清空运行目录内容，保留目录本身、属主和权限，并拒绝路径过宽、配置目录与
运行目录重叠、备份不完整或 `cranectld` 仍在运行等不安全情况。继续前必须看到
`[INFO] Confirmed empty CraneCtld runtime directory: <路径>` 和
`SUCCESS: CraneCtld runtime directory cleared: <路径>`。不要在启动新版本前恢复
旧 EmbeddedDB。

### 5. 安装并启动新版本

安装目标版本软件包或二进制，保留已备份的现场配置。先只启动插件服务和
CraneCtld：

```bash
sudo systemctl daemon-reload
sudo systemctl start cplugind
sudo systemctl start cranectld
systemctl is-active cranectld
```

CraneCtld 首次启动时会按需自动升级历史作业数据库。检查服务状态和本次启动的
完整日志：

```bash
systemctl is-active cranectld
journalctl -u cranectld -b --no-pager
```

`cranectld` 必须保持 `active`，日志中不能出现数据库初始化或升级错误。最终是否
升级成功，以第 7 步能够通过 `cacct` 读取升级前历史作业为准。验收和备份保留期
结束前不要删除第 3 步生成的备份目录。

### 6. 恢复下一作业 ID

EmbeddedDB 已清空，必须在开放提交前恢复两个计数器。它们独立计算，不要求
相等。让备份脚本读取、校验清单并直接执行两次 `ccontrol reset`：

```bash
python3 "$crane_upgrade_tool" \
  --restore-job-ids-from "$crane_backup_dir"
```

脚本会拒绝不完整备份以及最大 ID、下一 ID 不一致的清单。继续前必须看到两条
`[INFO] Completed: restore ... ID`、`SUCCESS: Job ID counters restored`，以及脚本
打印的两个下一 ID。

然后启动每个计算节点的 `craned` 和交互前端：

```bash
sudo systemctl start craned
sudo systemctl start cfored
systemctl is-active craned
systemctl is-active cfored
```

以上检查必须输出 `active`。

### 7. 验证

在解除提交阻止前完成以下检查。

检查各主机上的服务和历史作业读取：

```bash
systemctl is-active cranectld cplugind
systemctl is-active craned
systemctl is-active cfored
cqueue -t all --json
cacct -t all -F
cacct -t all --json
```

`cqueue` 应为空。至少核对一个 v1.1.3 已完成作业的 job ID、状态、CPU、内存、
提交时间和结束时间。

保持提交入口关闭，让脚本提交一个 held canary、核对其 ID、取消作业并通过
`cacct` 读取取消后的记录：

```bash
python3 "$crane_upgrade_tool" \
  --run-canary-from "$crane_backup_dir"
cqueue -t all --json
```

脚本必须打印 ID 匹配、取消成功、`cacct` 可读三项 `[INFO]` 信息，最后打印
`SUCCESS: Upgrade canary passed: <job-id>` 和 `cacct` JSON；随后 `cqueue` 必须
为空。全部通过后才解除提交阻止。

## 回滚 SOP

CraneCtld 无法启动、数据库升级报错、历史作业无法由 `cacct` 读取，或 canary 验证
失败时，执行完整回滚：

1. 重新阻止提交并停止新版本的 `cfored`、`craned`、`cranectld`、`cplugind`。
2. 重新安装升级前保留的 v1.1.3 软件包或二进制，但不要启动服务。
3. 让脚本从同一个备份恢复配置、MongoDB 和 CraneCtld 运行目录：

   ```bash
   python3 "$crane_upgrade_tool" \
     --rollback-from "$crane_backup_dir"
   ```

   脚本会先恢复清单指定的整个 MongoDB 数据库，再恢复配置和运行目录；被替换的
   新版本配置和运行目录会移动到带时间戳的 `failed-upgrade` 路径。MongoDB 启用
   认证时，命令会交互式要求密码。每项操作都会打印 `[INFO] Starting` 和
   `[INFO] Completed`。必须看到 `SUCCESS: Rollback data restored` 及恢复后的
   MongoDB、配置、运行目录和 `failed-upgrade` 路径；任一步报错时不要启动服务。

4. 启动 v1.1.3 服务并检查输出：

   ```bash
   sudo systemctl start cplugind cranectld
   sudo systemctl start craned
   sudo systemctl start cfored
   systemctl is-active cplugind cranectld
   systemctl is-active craned
   systemctl is-active cfored
   cqueue -t all --json
   cacct -t all --json
   ```

   所有服务必须输出 `active`，`cqueue` 必须符合回滚前的预期状态，且 `cacct`
   必须能读取升级前历史作业，之后才能开放提交。

回滚必须同时恢复 v1.1.3 二进制、配置、MongoDB 和 CraneCtld 运行目录，不能混用
新旧版本的数据。
