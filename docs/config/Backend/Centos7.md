# CraneSched Installation Guide (Centos 7)

> **Note**  
> This tutorial is **for CentOS 7 only**.  
> Since CentOS 7 has reached **EOL (End of Life)** and newer versions of CraneSched require **newer compilers**, this guide **may not work properly**.  


## 1. Configure Build Environment

All commands below should be executed on the **build node** as the **root** user.


Install additional repositories:

```bash
yum install -y epel-release centos-release-scl-rh
yum install -y ninja-build patch devtoolset-11 rh-git218
```

Add to `~/.bash_profile`:

```bash
source scl_source enable devtoolset-11
source scl_source enable rh-git218
```

## 2. Environment Preparation

### 2.1 Disable SELinux

```bash
setenforce 0
sed -i s#SELINUX=enforcing#SELINUX=disabled# /etc/selinux/config
```

### 2.2 Install Certificates

```bash
yum -y install ca-certificates
```

### 2.3 Synchronize System Time


```bash
yum -y install ntp ntpdate
systemctl start ntpd
systemctl enable ntpd
timedatectl set-timezone Asia/Shanghai
```

### 2.4 Disable Firewall or Open Ports

```bash
systemctl stop firewalld
systemctl disable firewalld
```

If your cluster requires the firewall to remain active, open the following ports:

```bash
firewall-cmd --add-port=10013/tcp --permanent --zone=public
firewall-cmd --add-port=10011/tcp --permanent --zone=public
firewall-cmd --add-port=10010/tcp --permanent --zone=public
firewall-cmd --add-port=873/tcp --permanent --zone=public
firewall-cmd --reload
```

> **Note:** If you have multiple nodes, perform this step on **each node**. Otherwise, inter-node communication will fail.

---

## 3. Install Dependencies

```bash
yum install -y openssl-devel libcgroup-devel curl-devel pam-devel zlib-devel zlib-static libaio-devel automake libcurl-devel
```

If `libcgroup-devel` installation fails:

```bash
wget https://github.com/libcgroup/libcgroup/releases/download/v3.1.0/libcgroup-3.1.0.tar.gz
tar -zxvf libcgroup-3.1.0.tar.gz
cd libcgroup-3.1.0
dnf install tar bison flex systemd-devel -y
sudo ./configure
make -j
sudo make install
```

---

## 4. Install Toolchain

### Requirements

* **CMake ≥ 3.24**
* **libstdc++ ≥ 11**
* **clang ≥ 19** or **g++ ≥ 14**

### 4.1 Common Setup

```bash
sudo dnf install -y wget tar
wget https://github.com/Kitware/CMake/releases/download/v3.26.4/cmake-3.26.4-linux-x86_64.sh
bash cmake-3.26.4-linux-x86_64.sh --prefix=/usr/local --skip-license
cmake --version
```

#### 4.2.1 Build GCC 14 Manually

```bash
wget https://ftp.gnu.org/gnu/gcc/gcc-14.1.0/gcc-14.1.0.tar.gz
tar -zxvf gcc-14.1.0.tar.gz
yum install -y bzip2
cd gcc-14.1.0
./contrib/download_prerequisites
mkdir build && cd build
../configure -enable-checking=release -enable-languages=c,c++ -disable-multilib
make -j
sudo make install
```

---

To make `clang` use the custom GCC toolchain:

```bash
-DCMAKE_C_FLAGS_INIT="--gcc-toolchain=/usr/local" \
-DCMAKE_CXX_FLAGS_INIT="--gcc-toolchain=/usr/local"
```

---

## 6. Build Crane Program

```bash
git clone https://github.com/PKUHPC/Crane.git
cd Crane
mkdir build && cd build
cmake -G Ninja -DCMAKE_C_COMPILER=/usr/bin/gcc14 \
               -DCMAKE_CXX_COMPILER=/usr/bin/g++14 ..
cmake --build . 
ninja install
```

---

## 7. Install MongoDB

Create repo:

```bash
sudo tee /etc/yum.repos.d/mongodb-6.0.2.repo << 'EOF'
[mongodb-org-6.0.2]
name=MongoDB 6.0.2 Repository
baseurl=https://repo.mongodb.org/yum/redhat/$releasever/mongodb-org/6.0/x86_64/
gpgcheck=0
enabled=1
EOF
yum makecache
yum install mongodb-org -y
systemctl enable mongod
systemctl start mongod
```

Create key file:

```bash
openssl rand -base64 756 | sudo -u mongod tee /var/lib/mongo/mongo.key
sudo -u mongod chmod 400 /var/lib/mongo/mongo.key
```
> ⚠️ Use strict password in production environment

Create admin user in Mongo shell:

```bash
mongosh
use admin
db.createUser({ user:'admin', pwd:'123456', roles:[{role:'root', db:'admin'}] })
```

Edit `/etc/mongod.conf`:

```yaml
security:
  authorization: enabled
  keyFile: /var/lib/mongo/mongo.key
replication:
  replSetName: crane_rs
```

Restart and initialize:

```bash
systemctl restart mongod
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

## 8. PAM Module Setup

> ⚠️ Only configure PAM **after the cluster is fully deployed and running**.
> Misconfiguration may prevent SSH login.
---

```bash
cp Crane/build/src/Misc/Pam/pam_crane.so /usr/lib64/security/
```

Edit `/etc/pam.d/sshd` as the following lines:

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

> ⚠️ `session optional pam_crane.so` must be placed **after** `session include password-auth` 

---

---

## 9. Run Crane

Edit and copy configuration:

```bash
mkdir -p /etc/crane
cp etc/config.yaml.example /etc/crane/config.yaml
vim /etc/crane/config.yaml
```

Run executables:

```bash
cd build/src
CraneCtld/cranectld
Craned/craned
```

---

## 10. Deploy to Other Nodes

### Using SCP

```bash
ssh crane02 "mkdir -p /etc/crane"
scp /usr/local/bin/craned crane02:/usr/local/bin/
scp /etc/systemd/system/craned.service crane02:/etc/systemd/system/
scp /etc/crane/config.yaml crane02:/etc/crane/
```

### Using PDSH

```bash
yum install -y pdsh
pdsh -w cranectl systemctl stop cranectld
pdcp -w cranectl src/CraneCtld/cranectld /usr/local/bin
pdsh -w cranectl systemctl start cranectld
pdsh -w crane0[1-4] systemctl stop craned
pdcp -w crane0[1-4] Craned/craned /usr/local/bin
pdsh -w crane0[1-4] systemctl start craned
```

