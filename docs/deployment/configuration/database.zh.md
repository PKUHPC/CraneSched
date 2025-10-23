# 数据库配置

本指南介绍如何为 CraneSched 安装和配置 MongoDB。

!!! info
    MongoDB 仅在**控制节点**上需要。计算节点不需要 MongoDB。

## 1. 安装 MongoDB

### 安装 MongoDB 社区版

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

#### 示例：Ubuntu/Debian

对于 Ubuntu/Debian 系统：

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

## 2. 配置 MongoDB

### 2.1 生成密钥文件

为副本集身份验证创建密钥文件：

```bash
openssl rand -base64 756 | sudo -u mongod tee /var/lib/mongo/mongo.key
sudo -u mongod chmod 400 /var/lib/mongo/mongo.key
```

### 2.2 创建管理员用户

连接到 MongoDB 并创建管理员用户：

```bash
mongosh
use admin
db.createUser({
    user: 'admin', 
    pwd: '123456',  # 在生产环境中更改此密码！
    roles: [{ role: 'root', db: 'admin' }]
})
db.shutdownServer()
quit
```

!!! warning
    在生产环境中为管理员用户使用强密码。

### 2.3 启用身份验证和复制

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

### 2.4 初始化副本集

```bash
mongosh
use admin
db.auth("admin","123456")
config = {
  "_id": "crane_rs",
  "members": [
    {
      "_id": 0,
      "host": "<hostname>:27017"  # 替换为您的控制节点主机名
    }
    # 如果您有多个 MongoDB 节点，在此处添加更多成员
  ]
}
rs.initiate(config)
quit
```

## 3. 配置 CraneSched 数据库连接

**仅在控制节点**上创建 `/etc/crane/database.yaml`：

```yaml
DbUser: admin
DbPassword: "123456"
DbHost: localhost
DbPort: 27017
DbReplSetName: crane_rs
DbName: crane_db
```

!!! warning
    此文件应仅存在于控制节点上，并且仅对 `root` 或 `crane` 用户可读：
    
    ```bash
    chmod 600 /etc/crane/database.yaml
    ```

## 故障排除

**连接失败**：确保 MongoDB 正在运行，并且防火墙允许端口 27017。

**身份验证失败**：验证 `database.yaml` 中的用户名和密码是否与 MongoDB 用户匹配。

**副本集错误**：检查 `mongod.conf` 中的 `replSetName` 是否与 `database.yaml` 中的值匹配。
