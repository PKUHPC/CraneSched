# Rocky Linux 9

> This tutorial is based on **Rocky Linux 9**, but it is also applicable to **Rocky Linux 8** and **Fedora**.  
> The software used in this guide targets the **x86-64** architecture.  
> If you are using another architecture such as **ARM64**, please adjust the download links accordingly.  
>  
> ⚠️ All commands should be executed as the **root** user.

---

## 1. Environment Preparation

### 1.1 Add EPEL Repository

```bash
dnf install -y yum-utils epel-release
dnf config-manager --set-enabled crb
````

---

### 1.2 Enable Time Synchronization

```bash
dnf install -y chrony
systemctl restart systemd-timedated
timedatectl set-timezone Asia/Shanghai
timedatectl set-ntp true
```

---

### 1.3 Disable Firewall

To disable the firewall completely:

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

> **Note:** If you have multiple nodes, perform this step on **each node**. Otherwise, inter-node communication will fail.

---

### 1.4 Disable SELinux

```bash
# Temporary (will be reset after reboot)
setenforce 0

# Permanent (survives reboot)
sed -i s#SELINUX=enforcing#SELINUX=disabled# /etc/selinux/config
```

---

### 1.5 Select CGroup Version (Optional)

Rocky 9 uses **CGroup v2** by default.
CraneSched uses **CGroup v1** by default.

If you prefer to enable CGroup v2 support, you’ll need additional configuration,
or you can switch the system to use CGroup v1.

---

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

---

#### 1.5.2 Configure CGroup v2

```bash
# Check if sub-cgroups have access to resources (expect to see cpu, io, memory, etc.)
cat /sys/fs/cgroup/cgroup.subtree_control

# Grant resource permissions to subgroups
echo '+cpuset +cpu +io +memory +pids' > /sys/fs/cgroup/cgroup.subtree_control
```

> **Note:**
> In CGroup v2, device control differs from v1.
> v2 uses **eBPF** for device access control.
> Please refer to CGroup v2 [eBPF documentation](EBPF.md) for configuring bpf programs.

---

## 2. Install Toolchain

### 2.1 Version Requirements

The toolchain must meet the following version requirements:

* `cmake` ≥ **3.24**
* If using **clang**, version ≥ **19**
* If using **g++**, version ≥ **14**

---

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

---

### 2.3 Install Common Utilities

```bash
dnf install -y tar curl unzip git
```

---

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

---

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

---

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

---

## 5. Install and Configure CraneSched

### 5.1 Build Binary Files

```bash
git clone https://github.com/PKUHPC/CraneSched.git
cd CraneSched
mkdir -p build && cd build

# For CGroup v1
cmake -G Ninja ..
cmake --build . --target cranectld craned pam_crane csupervisor

# For CGroup v2
cmake -G Ninja .. -DCRANE_ENABLE_CGROUP_V2=true
cmake --build . --target cranectld craned pam_crane csupervisor

```

### 5.2 Build RPM Package

```bash

cpack -G RPM


```

> **Notes:**
>
> * Use `-DCMAKE_BUILD_TYPE=Release` for production builds.
> * If fetching repositories or dependencies fails, set a proxy.

---

### 5.2 Configure PAM Module

> ⚠️ Only configure PAM **after the cluster is fully deployed and running**.
> Misconfiguration may prevent SSH login.
> PAM prevent user without job on the node from logging in. Usually deploy on all compute nodes.

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

> `session required pam_crane.so` must appear **after** `session include password-auth`

---

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
> ⚠️ Only need to configure on Control Node. It is recommended to set it as accessible only by the `root` or `crane` user.

```yaml
DbUser: admin
DbPassword: "123456"
DbHost: localhost
DbPort: 27017
DbReplSetName: crane_rs
DbName: crane_db
```

---

## 6. Start CraneSched

* Run manually (foreground):

```bash
cranectld
craned
```

* Or use systemd:

```bash
systemctl enable cranectld --now
systemctl enable craned --now
```

---

## Appendix 1: Common Issues

1. **CMake cannot find libcgroup** → Install `libcgroup` from release source.
2. **libcgroup.so.0 missing** → Add its path to `LD_LIBRARY_PATH` or fix pkg-config.
3. **CraneCtld and Craned run, but `cinfo` shows no nodes** → Likely firewall not disabled.

---

## Appendix 2: Multi-node Deployment

### Using SCP

```bash
ssh crane02 "mkdir -p /etc/crane"
scp CraneSched-1.1.2-Linux-x86_64-craned.rpm crane02:/tmp
ssh crane02 "rpm -ivh /tmp/CraneSched-1.1.2-Linux-x86_64-craned.rpm"
scp /etc/crane/config.yaml crane02:/etc/crane/
```

> Compute nodes must still install `libcgroup` as described in dependencies.

---

### Using PDSH

```bash
pdcp -w cranectl CraneSched-1.1.2-Linux-x86_64-cranectld.rpm /tmp
pdsh -w cranectl rpm -ivh /tmp/CraneSched-1.1.2-Linux-x86_64-cranectld.rpm
pdsh -w cranectl systemctl start cranectld

pdcp -w crane[01-04] CraneSched-1.1.2-Linux-x86_64-craned.rpm /tmp
pdsh -w crane[01-04] rpm -ivh /tmp/CraneSched-1.1.2-Linux-x86_64-craned.rpm
pdsh -w crane[01-04] systemctl start craned
```

---

## Appendix 3: Automated Build Script

This script automates the **Rocky 9** setup — including toolchain, dependencies, and CraneSched build.

> **Notes:**
>
> * Run this **after completing the environment setup** section.
> * MongoDB installation and CraneSched configuration must still be done manually.
> * Adjust the proxy settings inside the `setp` function.

```bash
#!/bin/bash

# Tested on Rocky Linux 9.3

set -eo pipefail

# Function to set and unset proxy
setp() {
    export https_proxy=http://your_proxy
    export http_proxy=http://your_proxy
    git config --global http.proxy $http_proxy
    git config --global https.proxy $https_proxy
}

unsetp() {
    unset http_proxy
    unset https_proxy
    git config --global --unset http.proxy
    git config --global --unset https.proxy
}

# Tools 
dnf install -y tar unzip git wget curl || {
    echo "Error installing tools" && exit 1
}

# Dependency for libcgroup
dnf install -y bison flex systemd-devel || {
    echo "Error installing dependency" && exit 1
}

# Ensure the installation can be found
export PKG_CONFIG_PATH=/usr/local/lib/pkgconfig:$PKG_CONFIG_PATH

# Check if libcgroup is already installed
if pkg-config --exists libcgroup; then
    echo "libcgroup is already installed."
else
    if [ ! -f "libcgroup-3.1.0.tar.gz" ]; then
        setp
        wget https://github.com/libcgroup/libcgroup/releases/download/v3.1.0/libcgroup-3.1.0.tar.gz || {
            echo "Error downloading libcgroup" && exit 1
        }
        unsetp
    fi

    tar -xzf libcgroup-3.1.0.tar.gz && pushd libcgroup-3.1.0
    (./configure --prefix=/usr/local && make -j && make install) || {
        echo "Error compiling libcgroup" && exit 1
    }
    popd
fi

# Install dependencies and toolchain for Crane
dnf install -y \
    patch \
    ninja-build \
    openssl-devel \
    pam-devel \
    zlib-devel \
    libatomic \
    libstdc++-static \
    libtsan \
    libasan \
    libaio \
    libaio-devel || {
    echo "Error installing toolchain and dependency for craned" && exit 1        
}
# libstdc++-static libatomic for debug
# libtsan for CRANE_THREAD_SANITIZER

# Check if cmake version is higher than 3.24
required_version="3.24"
install_version="3.28.1"
download_url="https://github.com/Kitware/CMake/releases/download/v${install_version}/cmake-${install_version}-linux-x86_64.sh"

current_version=$(cmake --version 2>/dev/null | awk 'NR==1{print $3}')

if [[ -z "$current_version" ]] || [[ "$(printf '%s\n' "$current_version" "$required_version" | sort -V | head -n1)" != "$required_version" ]]; then
    echo "Installing cmake ${install_version}..."
    setp
    wget -O cmake-install.sh "$download_url" || { echo "Error downloading cmake"; exit 1; }
    bash cmake-install.sh --skip-license --prefix=/usr/local || { echo "Error installing cmake"; exit 1; }
    rm cmake-install.sh
    unsetp
else
    echo "Current cmake version ($current_version) meets the requirement."
fi

# Clone the repository
setp
if [ ! -d "CraneSched" ]; then
    git clone https://github.com/PKUHPC/CraneSched.git || {
        echo "Error cloning CraneSched" && exit 1
    }
fi

pushd CraneSched
# git checkout master
git fetch && git pull
unsetp

BUILD_DIR=cmake-build-release
mkdir -p $BUILD_DIR && pushd $BUILD_DIR

if [ -f "/opt/rh/gcc-toolset-14/enable" ]; then
    echo "Enable gcc-toolset-14"
    source /opt/rh/gcc-toolset-14/enable
fi

setp
cmake --fresh -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DENABLE_UNQLITE=ON \
    -DENABLE_BERKELEY_DB=OFF .. || {
    echo "Error configuring with cmake" && exit 1
}
unsetp

cmake --build . --clean-first || {
    echo "Error building" && exit 1
}

popd
popd
```
