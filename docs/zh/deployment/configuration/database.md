# 数据库配置

本指南介绍如何为鹤思安装和配置所有数据库相关组件，包括 MongoDB、嵌入式数据库和 Vault 集成。

鹤思涉及以下三类数据存储/证书组件：

- **MongoDB**：外部数据库，用于持久化存储作业历史、账户信息等。仅在**控制节点**上需要。
- **嵌入式数据库（EmbeddedDb）**：内嵌在 CraneCtld 进程中，用于快速存取运行时状态（待执行队列、运行队列等），支持崩溃恢复。
- **Vault（可选）**：用于 PKI/TLS 证书管理。仅在启用 TLS 加密通信时需要。

## 完整配置参考

`/etc/crane/database.yaml` 完整配置示例：

??? example "展开查看 /etc/crane/database.yaml 完整配置示例"
    ```yaml
    # ============ 嵌入式数据库配置 ============
    # 嵌入式数据库后端类型（默认 Unqlite）
    CraneEmbeddedDbBackend: Unqlite

    # CraneCtld 嵌入式数据库文件路径
    # 相对于 CraneBaseDir（如果配置了 Keepalived，则相对于 CraneSharedBaseDir）
    CraneCtldDbPath: cranectld/embedded.db

    # ============ MongoDB 配置 ============
    DbUser: admin
    DbPassword: "123456"
    DbHost: localhost
    DbPort: 27017
    DbReplSetName: crane_rs
    DbName: crane_db

    # 作业聚合超时时间（毫秒），用于 MongoDB 聚合操作
    JobAggregationTimeoutMs: 600000

    # 作业聚合批次大小，用于 MongoDB 批量操作
    JobAggregationBatchSize: 100

    # ============ Vault 配置（可选，用于 PKI/TLS） ============
    Vault:
      Enabled: false
      Addr: 127.0.0.1
      Port: 8200
      Username: admin
      Password: "123456"
      Tls: false
      ExpirationMinutes: 30
    ```

---

## 1. 嵌入式数据库配置

嵌入式数据库直接运行在 CraneCtld 进程内部，无需额外安装，用于快速持久化调度器的运行时状态，实现崩溃后快速恢复。

### 配置参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `CraneEmbeddedDbBackend` | 字符串 | `Unqlite` | 嵌入式数据库引擎，默认使用 `Unqlite` |
| `CraneCtldDbPath` | 路径 | `cranectld/embedded.db` | 数据库文件存储路径 |

### 默认后端：Unqlite

[Unqlite](https://unqlite.org/) 是一个嵌入式的 NoSQL 数据库引擎，零配置、无需外部依赖。

```yaml
CraneEmbeddedDbBackend: Unqlite
```

!!! note
    Unqlite 对单次事务中的记录数有约 900,000 条的上限限制，这会影响待执行队列的最大容量。

### 数据库路径

```yaml
CraneCtldDbPath: cranectld/embedded.db
```

- **相对路径**：相对于 `config.yaml` 中的 `CraneBaseDir`（默认 `/var/crane/`），即实际路径为 `/var/crane/cranectld/embedded.db`
- **绝对路径**：也可以指定绝对路径，如 `/data/crane/embedded.db`
- **Keepalived 环境**：如果配置了 Keepalived 高可用，路径相对于 `CraneSharedBaseDir`

!!! tip
    请确保 CraneCtld 进程（使用 systemd 启动时为 `crane` 用户）对数据库文件所在目录具有读写权限。

---

## 2. 安装和配置 MongoDB

!!! info
    MongoDB 仅在**控制节点**上需要。计算节点不需要 MongoDB。

### 2.1 安装 MongoDB 社区版

请按照官方 MongoDB 安装指南为您的操作系统安装：

[MongoDB 社区版安装指南](https://www.mongodb.com/docs/manual/administration/install-community/)

!!! warning "CentOS 7 用户"
    CentOS 7 已达到生命周期终止（EOL），它支持的最新 MongoDB 版本是 **MongoDB 7.0**。
    
    请参阅 [MongoDB 7.0 Red Hat/CentOS 7 安装指南](https://www.mongodb.com/docs/v7.0/tutorial/install-mongodb-on-red-hat/)。

#### 示例：Rocky Linux 9

对于 Rocky Linux 9，安装过程通常包括：

```bash
# 创建仓库文件（查看 MongoDB 文档以获取最新版本）
cat > /etc/yum.repos.d/mongodb-org.repo << 'EOF'
[mongodb-org-8.2]
name=MongoDB Repository
baseurl=https://repo.mongodb.org/yum/redhat/9/mongodb-org/8.2/x86_64/
gpgcheck=1
enabled=1
gpgkey=https://pgp.mongodb.com/server-8.0.asc
EOF

dnf makecache
dnf install -y mongodb-org
systemctl enable mongod
systemctl start mongod
```

#### 示例：Debian/Ubuntu

对于 Debian/Ubuntu 系统：

```bash
# 按照官方指南获取最新说明
# 典型步骤包括导入 GPG 密钥和添加仓库
curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | \
   sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg \
   --dearmor

echo "deb [ signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] http://repo.mongodb.org/apt/debian bookworm/mongodb-org/8.2 main" | sudo tee /etc/apt/sources.list.d/mongodb-org-8.2.list

# 添加仓库并安装
sudo apt-get update
sudo apt-get install -y mongodb-org
sudo systemctl enable mongod
sudo systemctl start mongod
```

!!! tip
    上述示例可能会过时。请始终参考官方 MongoDB 文档以获取最新的安装说明。

### 2.2 生成密钥文件

为副本集身份验证创建密钥文件：

```bash
openssl rand -base64 756 | sudo -u mongod tee /var/lib/mongo/mongo.key
sudo -u mongod chmod 400 /var/lib/mongo/mongo.key
```

### 2.3 创建管理员用户

连接到 MongoDB 并创建管理员用户：

```bash
mongosh
```

```javascript
use admin
db.createUser({
    user: 'admin', 
    pwd: '123456',  // 在生产环境中请更改此密码！
    roles: [{ role: 'root', db: 'admin' }]
})
db.shutdownServer()
quit
```

!!! warning
    在生产环境中为管理员用户使用强密码。

### 2.4 启用身份验证和复制

编辑 `/etc/mongod.conf`：

```yaml
security:
  authorization: enabled
  keyFile: /var/lib/mongo/mongo.key
replication:
  replSetName: crane_rs
```

重启 MongoDB：

```bash
systemctl restart mongod
```

### 2.5 初始化副本集

```bash
mongosh
```

```javascript
use admin
db.auth("admin", "123456")
config = {
  "_id": "crane_rs",
  "members": [
    {
      "_id": 0,
      "host": "<hostname>:27017"  // 替换为您的控制节点主机名
    }
    // 如果您有多个 MongoDB 节点，在此处添加更多成员
  ]
}
rs.initiate(config)
quit
```

!!! tip "主机名"
    `<hostname>` 应替换为控制节点的实际主机名或 IP 地址。可以通过 `hostname` 命令获取当前主机名。

### 2.6 MongoDB 连接配置

在 `/etc/crane/database.yaml` 中配置 MongoDB 连接参数：

```yaml
DbUser: admin
DbPassword: "123456"
DbHost: localhost
DbPort: 27017
DbReplSetName: crane_rs
DbName: crane_db
```

**MongoDB 配置参数说明：**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `DbUser` | 字符串 | — | MongoDB 认证用户名 |
| `DbPassword` | 字符串 | — | MongoDB 认证密码 |
| `DbHost` | 字符串 | `localhost` | MongoDB 服务地址 |
| `DbPort` | 字符串 | `27017` | MongoDB 服务端口 |
| `DbReplSetName` | 字符串 | — | MongoDB 副本集名称，**必须配置** |
| `DbName` | 字符串 | `crane_db` | 鹤思使用的数据库名称 |

!!! warning "副本集必需"
    鹤思要求 MongoDB 以**副本集模式**运行，即使只有单个 MongoDB 实例也需要配置副本集。这是因为鹤思使用了 MongoDB 的事务功能，而事务需要副本集支持。

---

## 3. 作业聚合参数

作业聚合参数控制鹤思将已完成作业批量写入 MongoDB 的行为，影响系统性能和数据库写入效率。

```yaml
JobAggregationTimeoutMs: 600000
JobAggregationBatchSize: 100
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `JobAggregationTimeoutMs` | 整数 | `600000`（10 分钟） | 作业聚合超时时间（毫秒）。超过此时间后，即使未达到批次大小也会触发写入 |
| `JobAggregationBatchSize` | 整数 | `100` | 作业聚合批次大小。累积到此数量的已完成作业后触发批量写入 |

!!! tip "调优建议"
    - **高吞吐量集群**（大量短作业）：可增大 `JobAggregationBatchSize`（如 500）以减少 MongoDB 写入频率
    - **低吞吐量集群**：可减小 `JobAggregationTimeoutMs`（如 60000，即 1 分钟）以更快地看到作业历史记录
    - 两个条件满足其一即触发写入：超时 **或** 达到批次大小

---

## 4. Vault 集成（可选）

Vault 用于 PKI/TLS 证书管理，为鹤思集群提供证书签发与轮换能力。这是一个**可选**配置，仅在启用 TLS 加密通信时需要。

Vault 的安装、初始化、策略与 `database.yaml` 相关字段说明，请参阅 [PKI 安全配置](pki.md)。

---

## 5. 文件权限与安全

`database.yaml` 包含数据库密码等敏感信息，请确保正确设置文件权限：

```bash
# 仅 root 或 crane 用户可读
chmod 600 /etc/crane/database.yaml
chown crane:crane /etc/crane/database.yaml
```

!!! warning
    - `database.yaml` 应仅存在于**控制节点**上
    - 请勿在生产环境中使用默认密码 `123456`
    - 建议使用强密码并定期轮换

---

## 故障排除

### MongoDB 相关

**连接失败**：

- 确保 MongoDB 正在运行：`systemctl status mongod`
- 检查防火墙是否允许端口 27017：`firewall-cmd --list-ports`
- 验证 MongoDB 监听地址：`ss -tlnp | grep 27017`

**身份验证失败**：

- 验证 `database.yaml` 中的用户名和密码是否与 MongoDB 用户匹配
- 确认用户是在 `admin` 数据库中创建的
- 检查 MongoDB 日志：`journalctl -u mongod`

**副本集错误**：

- 检查 `mongod.conf` 中的 `replSetName` 是否与 `database.yaml` 中的 `DbReplSetName` 一致
- 通过 `mongosh` 验证副本集状态：`rs.status()`
- 确保密钥文件权限正确（400，属主为 mongod）

### 嵌入式数据库相关

**启动时报数据库后端错误**：

- 检查 `CraneEmbeddedDbBackend` 拼写是否正确（区分大小写：`Unqlite`）

**数据库文件权限问题**：

- 确保 CraneCtld 进程对 `CraneCtldDbPath` 指定的目录具有读写权限
- 使用 systemd 启动时，确保 `crane` 用户有权限访问该路径

### Vault 相关

**TLS 已启用但 Vault 未启用**：

- CraneCtld 会拒绝启动并报错。请在 `database.yaml` 中启用 Vault，或在 `config.yaml` 中禁用 TLS

**Vault 连接失败**：

- 确保 Vault 服务已启动且已解封（`vault status`）
- 验证 `Addr` 和 `Port` 配置是否正确
- 每次 Vault 重启后需要执行解封操作
