# Database Configuration

This guide covers the installation and configuration of all database-related components for CraneSched, including MongoDB, the embedded database, and Vault integration.

CraneSched uses two types of databases:

- **MongoDB**: An external database for persistent storage of job history, account information, etc. Required only on the **control node**.
- **Embedded Database (EmbeddedDb)**: Built into the CraneCtld process for fast access to runtime state (pending queue, running queue, etc.), enabling crash recovery.

## Full Configuration Reference

Complete `/etc/crane/database.yaml` example:

```yaml
# ============ Embedded Database Settings ============
# Embedded database backend: Unqlite (default) or BerkeleyDB
CraneEmbeddedDbBackend: Unqlite

# File path of CraneCtld embedded DB
# Relative to CraneBaseDir (when Keepalived is set, relative to CraneSharedBaseDir)
CraneCtldDbPath: cranectld/embedded.db

# ============ MongoDB Settings ============
DbUser: admin
DbPassword: "123456"
DbHost: localhost
DbPort: 27017
DbReplSetName: crane_rs
DbName: crane_db

# Job aggregation timeout in milliseconds for MongoDB operations
JobAggregationTimeoutMs: 600000

# Job aggregation batch size for MongoDB operations
JobAggregationBatchSize: 100

# ============ Vault Settings (Optional, for PKI/TLS) ============
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

## 1. Embedded Database Configuration

The embedded database runs directly inside the CraneCtld process — no separate installation is needed. It provides fast persistence of the scheduler's runtime state, enabling rapid recovery after crashes.

### Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `CraneEmbeddedDbBackend` | String | `Unqlite` | Embedded database engine: `Unqlite` or `BerkeleyDB` |
| `CraneCtldDbPath` | Path | `cranectld/embedded.db` | Database file storage path |

### Backend Selection

#### Unqlite (Default, Recommended)

[Unqlite](https://unqlite.org/) is an embedded NoSQL database engine — zero-configuration with no external dependencies.

```yaml
CraneEmbeddedDbBackend: Unqlite
```

- **Advantages**: Zero configuration, works out of the box, lightweight
- **Use case**: Most deployment environments

!!! note
    Unqlite has a limit of approximately 900,000 records per transaction, which affects the maximum pending queue capacity.

#### BerkeleyDB

[BerkeleyDB](https://www.oracle.com/database/technologies/related/berkeleydb.html) is a high-performance embedded database provided by Oracle.

```yaml
CraneEmbeddedDbBackend: BerkeleyDB
```

- **Advantages**: More mature transaction support, suitable for large-scale deployments
- **Prerequisites**: BerkeleyDB support must be enabled at compile time, and BerkeleyDB development libraries must be installed

!!! warning
    Using BerkeleyDB requires that CraneSched was compiled with BerkeleyDB support (CMake detects it automatically). If BerkeleyDB was not detected at compile time, selecting this backend at runtime will cause a startup failure.

### Database Path

```yaml
CraneCtldDbPath: cranectld/embedded.db
```

- **Relative path**: Relative to `CraneBaseDir` in `config.yaml` (default `/var/crane/`), so the actual path would be `/var/crane/cranectld/embedded.db`
- **Absolute path**: You can also specify an absolute path, e.g., `/data/crane/embedded.db`
- **Keepalived environments**: If Keepalived HA is configured, the path is relative to `CraneSharedBaseDir`

!!! tip
    Ensure the CraneCtld process (the `crane` user when started via systemd) has read/write permissions on the database file directory.

---

## 2. Install and Configure MongoDB

!!! info
    MongoDB is only required on the **control node**. Compute nodes do not need MongoDB.

### 2.1 Install MongoDB Community Edition

Please follow the official MongoDB installation guide for your operating system:

[MongoDB Community Edition Installation Guide](https://www.mongodb.com/docs/manual/administration/install-community/)

!!! warning "CentOS 7 Users"
    CentOS 7 has reached End of Life (EOL) and the latest MongoDB version it supports is **MongoDB 7.0**.
    
    Please refer to the [MongoDB 7.0 installation guide for Red Hat/CentOS 7](https://www.mongodb.com/docs/v7.0/tutorial/install-mongodb-on-red-hat/).

#### Example: Rocky Linux 9

For Rocky Linux 9, the installation process typically involves:

```bash
# Create repository file (check MongoDB docs for the latest version)
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

#### Example: Debian/Ubuntu

For Debian/Ubuntu systems:

```bash
# Follow the official guide for the latest instructions
# Typical steps include importing GPG key and adding repository
curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | \
   sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg \
   --dearmor

echo "deb [ signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] http://repo.mongodb.org/apt/debian bookworm/mongodb-org/8.2 main" | sudo tee /etc/apt/sources.list.d/mongodb-org-8.2.list

# Add repository and install
sudo apt-get update
sudo apt-get install -y mongodb-org
sudo systemctl enable mongod
sudo systemctl start mongod
```

!!! tip
    The examples above may become outdated. Always refer to the official MongoDB documentation for the most current installation instructions.

### 2.2 Generate Key File

Create a key file for replica set authentication:

```bash
openssl rand -base64 756 | sudo -u mongod tee /var/lib/mongo/mongo.key
sudo -u mongod chmod 400 /var/lib/mongo/mongo.key
```

### 2.3 Create Admin User

Connect to MongoDB and create an admin user:

```bash
mongosh
```

```javascript
use admin
db.createUser({
    user: 'admin', 
    pwd: '123456',  // Change this in production!
    roles: [{ role: 'root', db: 'admin' }]
})
db.shutdownServer()
quit
```

!!! warning
    Use a strong password for the admin user in production environments.

### 2.4 Enable Authentication and Replication

Edit `/etc/mongod.conf`:

```yaml
security:
  authorization: enabled
  keyFile: /var/lib/mongo/mongo.key
replication:
  replSetName: crane_rs
```

Restart MongoDB:

```bash
systemctl restart mongod
```

### 2.5 Initialize Replica Set

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
      "host": "<hostname>:27017"  // Replace with your control node hostname
    }
    // Add more members here if you have multiple MongoDB nodes
  ]
}
rs.initiate(config)
quit
```

!!! tip "Hostname"
    Replace `<hostname>` with the actual hostname or IP address of the control node. You can get the current hostname with the `hostname` command.

### 2.6 MongoDB Connection Configuration

Configure the MongoDB connection parameters in `/etc/crane/database.yaml`:

```yaml
DbUser: admin
DbPassword: "123456"
DbHost: localhost
DbPort: 27017
DbReplSetName: crane_rs
DbName: crane_db
```

**MongoDB Configuration Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `DbUser` | String | — | MongoDB authentication username |
| `DbPassword` | String | — | MongoDB authentication password |
| `DbHost` | String | `localhost` | MongoDB server address |
| `DbPort` | String | `27017` | MongoDB server port |
| `DbReplSetName` | String | — | MongoDB replica set name, **must be configured** |
| `DbName` | String | `crane_db` | Database name used by CraneSched |

!!! warning "Replica Set Required"
    CraneSched requires MongoDB to run in **replica set mode**, even with a single MongoDB instance. This is because CraneSched uses MongoDB transactions, which require replica set support.

---

## 3. Job Aggregation Parameters

Job aggregation parameters control how CraneSched batches completed jobs for writing to MongoDB, affecting system performance and database write efficiency.

```yaml
JobAggregationTimeoutMs: 600000
JobAggregationBatchSize: 100
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `JobAggregationTimeoutMs` | Integer | `600000` (10 min) | Job aggregation timeout in milliseconds. A write is triggered after this timeout even if the batch size has not been reached |
| `JobAggregationBatchSize` | Integer | `100` | Job aggregation batch size. A bulk write is triggered after accumulating this many completed jobs |

!!! tip "Tuning Recommendations"
    - **High-throughput clusters** (many short jobs): Increase `JobAggregationBatchSize` (e.g., 500) to reduce MongoDB write frequency
    - **Low-throughput clusters**: Decrease `JobAggregationTimeoutMs` (e.g., 60000 = 1 minute) to see job history sooner
    - A write is triggered when **either** condition is met: timeout **or** batch size reached

---

## 4. Vault Integration (Optional)

Vault integration provides PKI/TLS certificate management for encrypted communication within the CraneSched cluster. This is an **optional** configuration, required only when TLS encryption is enabled.

```yaml
Vault:
  Enabled: false
  Addr: 127.0.0.1
  Port: 8200
  Username: admin
  Password: "123456"
  Tls: false
  ExpirationMinutes: 30
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `Enabled` | Boolean | `false` | Whether to enable Vault integration |
| `Addr` | String | `127.0.0.1` | Vault server address |
| `Port` | String | `8200` | Vault server port |
| `Username` | String | — | Vault authentication username, **required when enabled** |
| `Password` | String | — | Vault authentication password, **required when enabled** |
| `Tls` | Boolean | `false` | Whether the Vault server itself uses TLS. Recommended for production |
| `ExpirationMinutes` | Integer | `30` | Certificate validity period in minutes |

!!! note
    TLS requires Vault to be enabled. If TLS is enabled in `config.yaml` but Vault is not enabled, CraneCtld will refuse to start.

For complete Vault installation and configuration instructions, see the [PKI Security Configuration](pki.md) guide.

---

## 5. File Permissions and Security

`database.yaml` contains sensitive information such as database passwords. Ensure proper file permissions:

```bash
# Readable only by root or the crane user
chmod 600 /etc/crane/database.yaml
chown crane:crane /etc/crane/database.yaml
```

!!! warning
    - `database.yaml` should only exist on the **control node**
    - Do not use the default password `123456` in production
    - Use strong passwords and rotate them regularly

---

## Troubleshooting

### MongoDB Issues

**Connection failed**:

- Ensure MongoDB is running: `systemctl status mongod`
- Check that the firewall allows port 27017: `firewall-cmd --list-ports`
- Verify MongoDB is listening: `ss -tlnp | grep 27017`

**Authentication failed**:

- Verify the username and password in `database.yaml` match the MongoDB user
- Confirm the user was created in the `admin` database
- Check MongoDB logs: `journalctl -u mongod`

**Replica set errors**:

- Check that `replSetName` in `mongod.conf` matches `DbReplSetName` in `database.yaml`
- Verify replica set status via `mongosh`: `rs.status()`
- Ensure the key file permissions are correct (400, owned by mongod)

### Embedded Database Issues

**Database backend error at startup**:

- Check that `CraneEmbeddedDbBackend` is spelled correctly (case-sensitive: `Unqlite` or `BerkeleyDB`)
- If using `BerkeleyDB`, confirm it was included during compilation

**Database file permission issues**:

- Ensure the CraneCtld process has read/write access to the directory specified by `CraneCtldDbPath`
- When using systemd, ensure the `crane` user has permission to access the path

### Vault Issues

**TLS enabled but Vault not enabled**:

- CraneCtld will refuse to start with an error. Either enable Vault in `database.yaml` or disable TLS in `config.yaml`

**Vault connection failed**:

- Ensure the Vault service is running and unsealed (`vault status`)
- Verify the `Addr` and `Port` configuration is correct
- Remember to unseal Vault after every restart
