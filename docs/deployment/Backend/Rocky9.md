# Rocky Linux 9
!!! tip
    This tutorial is primarily designed for **Rocky Linux 9**, but it should be compatible with **Rocky Linux 8** and **Fedora**.

    The instructions are tailored for the **x86-64** architecture. For other architectures, such as ARM64, ensure you modify the download links and commands as needed.

Please run all commands as the root user throughout this tutorial.

## 1. Environment Preparation

### 1.1 Add EPEL Repository

```bash
dnf install -y yum-utils epel-release
dnf config-manager --set-enabled crb
```

### 1.2 Enable Time Synchronization

```bash
dnf install -y chrony
systemctl restart systemd-timedated
timedatectl set-timezone Asia/Shanghai
timedatectl set-ntp true
```

### 1.3 Configure Firewall

!!! tip
    If you have multiple nodes, perform this step on **each node**. Otherwise, inter-node communication will fail.

    Please see the config file `/etc/crane/config.yaml` for port configuration details.

```bash
systemctl stop firewalld
systemctl disable firewalld
```

If your cluster requires the firewall to remain active, open the following ports:

```bash
firewall-cmd --add-port=10013/tcp --permanent --zone=public
firewall-cmd --add-port=10012/tcp --permanent --zone=public
firewall-cmd --add-port=10011/tcp --permanent --zone=public
firewall-cmd --add-port=10010/tcp --permanent --zone=public
firewall-cmd --add-port=873/tcp --permanent --zone=public
firewall-cmd --reload
```

### 1.4 Disable SELinux

```bash
# Temporary (will be reset after reboot)
setenforce 0

# Permanent (survives reboot)
sed -i s#SELINUX=enforcing#SELINUX=disabled# /etc/selinux/config
```

### 1.5 Select CGroup Version (Optional)

Rocky 9 uses **CGroup v2** by default.
CraneSched uses **CGroup v1** by default.

If you prefer to enable CGroup v2 support, you’ll need [additional configuration](eBPF.md),
or you can switch the system to use CGroup v1.

#### 1.5.1 Configure CGroup v1

If your system is already using CGroup v1, skip this section.

```bash
# Set kernel boot parameters to switch to CGroup v1
grubby --update-kernel=/boot/vmlinuz-$(uname -r) \
  --args="systemd.unified_cgroup_hierarchy=0 systemd.legacy_systemd_cgroup_controller"

# Reboot to apply changes
reboot

# Verify version
mount | grep cgroup
```

#### 1.5.2 Configure CGroup v2

```bash
# Check if sub-cgroups have access to resources (expect to see cpu, io, memory, etc.)
cat /sys/fs/cgroup/cgroup.subtree_control

# Grant resource permissions to subgroups
echo '+cpuset +cpu +io +memory +pids' > /sys/fs/cgroup/cgroup.subtree_control
```

Additionally, if you plan to use GRES with CGroup v2, please refer to the [eBPF guide](eBPF.md) for setup instructions.

## 2. Install Toolchain

### 2.1 Version Requirements

The toolchain must meet the following version requirements:

* `cmake` ≥ **3.24**
* If using **clang**, version ≥ **19**
* If using **g++**, version ≥ **14**

### 2.2 Install Build Tools

```bash
dnf install -y \
    gcc-toolset-14 \
    cmake \
    patch \
    flex \
    bison \
    ninja-build

echo 'source /opt/rh/gcc-toolset-14/enable' >> /etc/profile.d/extra.sh
source /etc/profile.d/extra.sh
```

### 2.3 Install Common Utilities

```bash
dnf install -y tar curl unzip git
```

## 3. Install Project Dependencies

```bash
dnf install -y \
    libstdc++-devel \
    libstdc++-static \
    openssl-devel \
    curl-devel \
    pam-devel \
    zlib-devel \
    libaio-devel \
    systemd-devel \
    libcurl-devel \
    automake
```

## 4. Install and Configure MongoDB

> This step should only be done on the **storage/control node**.
> Other nodes do not need MongoDB.

### 4.1 Install MongoDB

#### 4.1.1 **Add MongoDB YUM repository:**

```bash
cat > /etc/yum.repos.d/mongodb-org-7.0.repo << 'EOF'
[mongodb-org-7.0]
name=MongoDB Repository
baseurl=https://repo.mongodb.org/yum/redhat/9/mongodb-org/7.0/x86_64/
gpgcheck=1
enabled=1
gpgkey=https://pgp.mongodb.com/server-7.0.asc
EOF

dnf makecache
```

#### 4.1.2 **Install MongoDB and enable on boot:**

```bash
dnf install -y mongodb-org
systemctl enable mongod
systemctl start mongod
```

#### 4.1.3 **Generate key file:**

```bash
openssl rand -base64 756 | sudo -u mongod tee /var/lib/mongo/mongo.key
sudo -u mongod chmod 400 /var/lib/mongo/mongo.key
```

### 4.2 Configure MongoDB

#### 4.2.1 **Create an admin user:**

```bash
mongosh
use admin
db.createUser({
    user: 'admin', pwd: '123456', roles: [{ role: 'root', db: 'admin' }]
})
db.shutdownServer()
quit
```

#### 4.2.2 **Enable authentication and replication:**

Edit `/etc/mongod.conf` and modify:

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

#### 4.2.3 **Initialize replica set:**

```bash
mongosh
use admin
db.auth("admin","123456")
config = {
  "_id": "crane_rs",      // Same as configured `replSetName` above
  "members": [
    {
      "_id": 0,
      "host": "<hostname>:27017" // Replication node host
    }
    // ... Other nodes (if exits)
  ]
}
rs.initiate()
```



## 5. Install and Configure CraneSched

### 5.1 Build Binary Files

```bash
git clone https://github.com/PKUHPC/CraneSched.git
cd CraneSched
mkdir -p build && cd build

# For CGroup v1
cmake -G Ninja ..
cmake --build .

# For CGroup v2
cmake -G Ninja .. -DCRANE_ENABLE_CGROUP_V2=true
cmake --build .
```

### 5.2 Build RPM Package

```bash
cpack -G RPM
```

!!! tip
    Use `-DCMAKE_BUILD_TYPE=Release` for production builds.
    If fetching repositories or dependencies fails, set a proxy.

### 5.2 Configure PAM Module

!!! warning
    Only configure PAM **after the cluster is fully deployed and running**.
    
    **Misconfiguration may prevent SSH login.**

#### 5.2.1 **Copy PAM module:**

```bash
cp build/src/Misc/Pam/pam_crane.so /usr/lib64/security/
```

#### 5.2.2 **Edit `/etc/pam.d/sshd`:**

* Add `account required pam_crane.so` **before** `account include password-auth`.
* Add `session required pam_crane.so` **after** `session include password-auth`.

Example:

```bash
#%PAM-1.0
auth       required     pam_sepermit.so
auth       substack     password-auth
auth       include      postlogin
# Used with polkit to reauthorize users in remote sessions
-auth      optional     pam_reauthorize.so prepare
account    required     pam_crane.so
account    required     pam_nologin.so
account    include      password-auth
password   include      password-auth
# pam_selinux.so close should be the first session rule
session    required     pam_selinux.so close
session    required     pam_loginuid.so
# pam_selinux.so open should only be followed by sessions to be executed in the user context
session    required     pam_selinux.so open env_params
session    required     pam_namespace.so
session    optional     pam_keyinit.so force revoke
session    include      password-auth
session    required     pam_crane.so
session    include      postlogin
# Used with polkit to reauthorize users in remote sessions
-session   optional     pam_reauthorize.so prepare
```

!!! warning
    `session required pam_crane.so` must be placed **after** `session include password-auth`

### 5.3 Configure CraneSched

#### 5.3.1 **Copy example configuration files:**

```bash
mkdir -p /etc/crane
cp etc/config.yaml /etc/crane/config.yaml
cp etc/database.yaml /etc/crane/database.yaml

# For CGroup v2
cp build/src/Misc/BPF/cgroup_dev_bpf.o /usr/local/lib64/bpf/
```

#### 5.3.2 **Edit `/etc/crane/config.yaml` to define cluster and partitions.**
Example:

```yaml
ControlMachine: crane01

Nodes:
  - name: "crane[01-04]"
    cpu: 2
    memory: 2G

Partitions:
  - name: CPU
    nodes: "crane[01-02]"
    priority: 5
  - name: GPU
    nodes: "crane[03-04]"
    priority: 3
    DefaultMemPerCpu: 0
    MaxMemPerCpu: 0

DefaultPartition: CPU
```

#### 5.3.3 **Configure MongoDB connection in `/etc/crane/database.yaml`:**

!!! warning
    Only need to configure on Control Node. It is recommended to set it as accessible only by the `root` or `crane` user.

```yaml
DbUser: admin
DbPassword: "123456"
DbHost: localhost
DbPort: 27017
DbReplSetName: crane_rs
DbName: crane_db
```

## 6. Start CraneSched

Run manually (foreground):

```bash
cranectld
craned
```

Or use systemd:

```bash
systemctl daemon-reload
systemctl enable cranectld --now
systemctl enable craned --now
```

## Appendix 1: Common Issues

1. **CMake cannot find libcgroup** → Install `libcgroup` from release source.
2. **libcgroup.so.0 missing** → Add its path to `LD_LIBRARY_PATH` or fix pkg-config.
3. **CraneCtld and Craned run, but `cinfo` shows no nodes** → Likely firewall not disabled.

## Appendix 2: Multi-node Deployment

### Using SCP

```bash
ssh crane02 "mkdir -p /etc/crane"
scp CraneSched-1.1.2-Linux-x86_64-craned.rpm crane02:/tmp
ssh crane02 "rpm -ivh /tmp/CraneSched-1.1.2-Linux-x86_64-craned.rpm"
scp /etc/crane/config.yaml crane02:/etc/crane/
```

### Using PDSH

```bash
pdcp -w cranectl CraneSched-1.1.2-Linux-x86_64-cranectld.rpm /tmp
pdsh -w cranectl rpm -ivh /tmp/CraneSched-1.1.2-Linux-x86_64-cranectld.rpm
pdsh -w cranectl systemctl start cranectld

pdcp -w crane[01-04] CraneSched-1.1.2-Linux-x86_64-craned.rpm /tmp
pdsh -w crane[01-04] rpm -ivh /tmp/CraneSched-1.1.2-Linux-x86_64-craned.rpm
pdsh -w crane[01-04] systemctl start craned
```