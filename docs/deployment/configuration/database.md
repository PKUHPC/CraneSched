# Database Configuration

This guide covers MongoDB installation and configuration for CraneSched.

!!! info
    MongoDB is only required on the **control node**. Compute nodes do not need MongoDB.

## 1. Install MongoDB

### Install MongoDB Community Edition

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

#### Example: Ubuntu/Debian

For Ubuntu/Debian systems:

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

## 2. Configure MongoDB

### 2.1 Generate Key File

Create a key file for replica set authentication:

```bash
openssl rand -base64 756 | sudo -u mongod tee /var/lib/mongo/mongo.key
sudo -u mongod chmod 400 /var/lib/mongo/mongo.key
```

### 2.2 Create Admin User

Connect to MongoDB and create an admin user:

```bash
mongosh
use admin
db.createUser({
    user: 'admin', 
    pwd: '123456',  # Change this in production!
    roles: [{ role: 'root', db: 'admin' }]
})
db.shutdownServer()
quit
```

!!! warning
    Use a strong password for the admin user in production environments.

### 2.3 Enable Authentication and Replication

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

### 2.4 Initialize Replica Set

```bash
mongosh
use admin
db.auth("admin","123456")
config = {
  "_id": "crane_rs",
  "members": [
    {
      "_id": 0,
      "host": "<hostname>:27017"  # Replace with your control node hostname
    }
    # Add more members here if you have multiple MongoDB nodes
  ]
}
rs.initiate(config)
quit
```

## 3. Configure CraneSched Database Connection

Create `/etc/crane/database.yaml` on the **control node only**:

```yaml
DbUser: admin
DbPassword: "123456"
DbHost: localhost
DbPort: 27017
DbReplSetName: crane_rs
DbName: crane_db
```

!!! warning
    This file should only exist on the control node and be readable only by `root` or the `crane` user:
    
    ```bash
    chmod 600 /etc/crane/database.yaml
    ```

## Troubleshooting

**Connection failed**: Ensure MongoDB is running and the firewall allows port 27017.

**Authentication failed**: Verify the username and password in `database.yaml` match the MongoDB user.

**Replica set errors**: Check that the `replSetName` in `mongod.conf` matches the value in `database.yaml`.
