# centos7 Crane环境配置

## 1.环境准备

安装ntp ntpdate同步时钟
```shell
yum install ntp ntpdate
systemctl start ntpd
systemctl enable ntpd

timedatectl set-timezone Asia/Shanghai
```

关闭防火墙，不允许关闭防火墙则考虑开放10011、10010、873端口
```shell
systemctl stop firewalld
systemctl disable firewalld

# 或者开放端口
firewall-cmd --add-port=10011/tcp --permanent --zone=public
firewall-cmd --add-port=10010/tcp --permanent --zone=public
firewall-cmd --add-port=873/tcp --permanent --zone=public
# 重启防火墙(修改配置后要重启防火墙)
firewall-cmd --reload
```

Change yum source to tsinghua repo source:

```shell
rm -rf /etc/yum.repos.d/*
vi /etc/yum.repos.d/CentOS-Base.repo
```

Paste the following contents:

```
# CentOS-Base.repo
#
# The mirror system uses the connecting IP address of the client and the
# update status of each mirror to pick mirrors that are updated to and
# geographically close to the client.  You should use this for CentOS updates
# unless you are manually picking other mirrors.
#
# If the mirrorlist= does not work for you, as a fall back you can try the
# remarked out baseurl= line instead.
#
#

[base]
name=CentOS-$releasever - Base
baseurl=https://mirrors.tuna.tsinghua.edu.cn/centos/$releasever/os/$basearch/
#mirrorlist=http://mirrorlist.centos.org/?release=$releasever&arch=$basearch&repo=os
gpgcheck=1
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7

#released updates
[updates]
name=CentOS-$releasever - Updates
baseurl=https://mirrors.tuna.tsinghua.edu.cn/centos/$releasever/updates/$basearch/
#mirrorlist=http://mirrorlist.centos.org/?release=$releasever&arch=$basearch&repo=updates
gpgcheck=1
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7

#additional packages that may be useful
[extras]
name=CentOS-$releasever - Extras
baseurl=https://mirrors.tuna.tsinghua.edu.cn/centos/$releasever/extras/$basearch/
#mirrorlist=http://mirrorlist.centos.org/?release=$releasever&arch=$basearch&repo=extras
gpgcheck=1
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7

#additional packages that extend functionality of existing packages
[centosplus]
name=CentOS-$releasever - Plus
baseurl=https://mirrors.tuna.tsinghua.edu.cn/centos/$releasever/centosplus/$basearch/
#mirrorlist=http://mirrorlist.centos.org/?release=$releasever&arch=$basearch&repo=centosplus
gpgcheck=1
enabled=0
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7
```

```shell
yum clean all
yum makecache

yum install epel-release
yum install centos-release-scl-rh
```

## 2.安装工具链

```shell
yum install devtoolset-11
yum install rh-git218
```

为了避免每次手动生效， Then, paste the following contents into `~/.bash_profile`

```shell
vim ~/.bash_profile
```

```
source scl_source enable rh-git218
source scl_source enable devtoolset-11
```

Restart bash.

这时用gcc --version查询，可以看到版本已经是11.2系列了

```shell
$ gcc --version
gcc (GCC) 11.2.1 20210728 (Red Hat 11.2.1-1)
Copyright (C) 2021 Free Software Foundation, Inc.
This is free software; see the source for copying conditions.  There is NO
warranty; not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
```

给系统安装个下载命令器
```shell
yum install wget -y
```

安装cmake和ninja

由于需要源码安装，首先选择合适的源码存放位置，可以放在自己账号的目录下。
```shell
su - liulinxing # 用户名
mkdir download
cd download
```

从github下载源码
```shell
wget https://github.com/ninja-build/ninja/releases/download/v1.10.2/ninja-linux.zip
wget https://github.com/Kitware/CMake/releases/download/v3.21.3/cmake-3.21.3.tar.gz
```

解压编译安装
```shell
unzip ninja-linux.zip
cp ninja /usr/bin/

yum install openssl-devel

tar -zxvf cmake-3.21.3.tar.gz
cd cmake-3.12.4
./bootstrap
gmake
gmake install
```

检查安装是否成功
```shell
cmake --version
#cmake version 3.21.3
#
#CMake suite maintained and supported by Kitware (kitware.com/cmake).
```

## 3.安装C++库

下载相关源码压缩包到一个目录，解压所有源码压缩包

```shell
wget https://boostorg.jfrog.io/artifactory/main/release/1.78.0/source/boost_1_78_0.tar.gz
tar xzf boost_1_78_0.tar.gz

wget https://github.com/fmtlib/fmt/archive/refs/tags/8.0.1.tar.gz
tar xzf fmt-8.0.1.tar.gz

wget https://github.com/libevent/libevent/releases/download/release-2.1.12-stable/libevent-2.1.12-stable.tar.gz
tar xzf libevent-2.1.12-stable.tar.gz

wget https://github.com/jarro2783/cxxopts/archive/refs/tags/v2.2.1.tar.gz
tar xzf cxxopts-2.2.1.tar.gz

wget https://github.com/google/googletest/archive/refs/tags/release-1.11.0.tar.gz
tar xzf googletest-release-1.11.0.tar.gz

wget https://dist.libuv.org/dist/v1.42.0/libuv-v1.42.0.tar.gz
tar xzf libuv-1.42.0.tar.gz

wget https://github.com/gabime/spdlog/archive/refs/tags/v1.8.5.tar.gz
tar xzf spdlog-1.8.5.tar.gz
```

安装第三方库
```shell
yum install libcgroup-devel
yum install libcurl-devel
yum install pam-devel

 cd boost_1_78_0
./bootstrap.sh
./b2 install --with=all

cd ..
cd libuv-1.42.0
mkdir build
cd build/
cmake -DCMAKE_INSTALL_PREFIX=/nfs/home/testCrane/Crane/dependencies/online/libuv -DCMAKE_CXX_STANDARD=17 -G Ninja ..
ninja install

# 运行安装脚本
bash ./download_deps.sh
```

## 4.安装mariadb

通过yum安装就行了，安装mariadb-server，默认依赖安装mariadb，一个是服务端、一个是客户端。
```shell
wget https://downloads.mariadb.com/MariaDB/mariadb_repo_setup
chmod +x mariadb_repo_setup
./mariadb_repo_setup
yum install MariaDB-server
yum install mysql-devel #安装链接库

systemctl start mariadb  # 开启服务
systemctl enable mariadb  # 设置为开机自启动服务

mariadb-secure-installation  # 首次安装需要进行数据库的配置
```

配置时出现的各个选项
```shell
Enter current password for root (enter for none):  # 输入数据库超级管理员root的密码(注意不是系统root的密码)，第一次进入还没有设置密码则直接回车

Set root password? [Y/n]  # 设置密码，y

New password:  # 新密码  123456
Re-enter new password:  # 再次输入密码:123456

Remove anonymous users? [Y/n]  # 移除匿名用户， y

Disallow root login remotely? [Y/n]  # 拒绝root远程登录，n，不管y/n，都会拒绝root远程登录

Remove test database and access to it? [Y/n]  # 删除test数据库，y：删除。n：不删除，数据库中会有一个test数据库，一般不需要

Reload privilege tables now? [Y/n]  # 重新加载权限表，y。或者重启服务也许
```

安装过程中遇到Table doesn’t exist报错：
ERROR 1146 (42S02) at line 1: Table ‘mysql.global_priv’ doesn’t exist … Failed!
执行下面的命令后，重启mysql
```shell
mysql_upgrade -uroot -p --force
```

重启数据库
```shell
systemctl restart mariadb
```

登陆数据库
```shell
mysql -uroot -p
Enter password:
Welcome to the MariaDB monitor.  Commands end with ; or \g.
Your MariaDB connection id is 9
Server version: 10.4.8-MariaDB MariaDB Server
```

进入mysql数据库
```shell
use mysql
```

查询user表，可看到多条数据
```shell
select host,user,password from user;
```

删除localhost以外数据
```shell
delete from user where host !='localhost';
```

配置完毕，退出
```shell
exit;
systemctl restart mariadb
```

## 5.安装MongoDB

```shell
# 下载并解压安装包
wget https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-rhel70-5.0.9.tgz
tar -zxvf mongodb-linux-x86_64-rhel70-5.0.9.tgz
# 重命名
mv mongodb-linux-x86_64-rhel70-5.0.9  /opt/mongodb
# 添加环境变量  
vim /etc/profile
```

在配置文件中添加如下内容（路径应对应MongoDB安装路径）
```shell
export MONGODB_HOME=/opt/mongodb
export PATH=$PATH:${MONGODB_HOME}/bin
```

```shell
# 使环境变量生效
source /etc/profile 
# 创建db目录和log目录
cd /opt/mongodb-linux-x86_64-rhel70-5.0.3
mkdir -p ./data/db
mkdir -p ./logs
touch ./logs/mongodb.log
```

创建mongodb.conf配置文件，内容如下（路径应对应之前创建的db/log目录）：
```shell
#端口号
port=27017
#db目录
dbpath=/opt/mongodb/data/db
#日志目录
logpath=/opt/mongodb/logs/mongodb.log
#后台
fork=true
#日志输出
logappend=true
#允许远程IP连接
bind_ip=0.0.0.0
#开启权限验证
auth=true
```

启动测试
```shell
mongod --config /opt/mongodb/mongodb.conf
mongo
```

创建用户
```shell
use admin
db.createUser({
  user:'admin',//用户名
  pwd:'123456',//密码
  roles:[{ role:'root',db:'admin'}]//root 代表超級管理员权限 admin代表给admin数据库加的超级管理员
})

use Crane_db

db.createUser({
  user:"crane",
  pwd:"123456",
  roles:[{role:"dbOwner",db:"Crane_db"}]
})

db.shutdownServer() //重启前先关闭服务器
```

重新启动MongoDB数据库
```shell
mongod --config /opt/mongodb/mongodb.conf
```

编辑开机启动
```shell
vi /etc/rc.local
# 加入如下语句，以便启动时执行：
mongod --config /opt/mongodb/mongodb.conf
```

## 6.安装MongoDB C++驱动

参考 http://mongocxx.org/mongocxx-v3/installation/linux/

安装mongo-c-driver
```shell
wget https://github.com/mongodb/mongo-c-driver/releases/download/1.21.1/mongo-c-driver-1.21.1.tar.gz
tar xzf mongo-c-driver-1.21.1.tar.gz
cd mongo-c-driver-1.21.1
mkdir cmake-build
cd cmake-build
cmake -DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF -DCMAKE_INSTALL_PREFIX=/nfs/home/liulinxing/Crane/dependencies/online/mongo-c-driver ..
sudo make && make install
```

安装mongo-cxx-driver
```shell
curl -OL https://github.com/mongodb/mongo-cxx-driver/archive/r3.6.5.tar.gz
cd mongo-cxx-driver-r3.6.5/build/
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/nfs/home/liulinxing/Crane/dependencies/online/mongo-driver ..
sudo make EP_mnmlstc_core
make
sudo make install
```

## 7.编译程序

首先进入到项目目录下
```shell
mkdir build
cd build/

cmake -DCMAKE_CXX_STANDARD=17 -G Ninja ..
cmake --build .
```

## 8.Pam模块

首次编译完成后需要将pam模块动态链接库放入系统指定位置
```shell
cp Crane/build/src/Misc/Pam/pam_Crane.so /usr/lib64/security/
```

同时计算节点“/etc/security/access.conf”文件禁止非root用户登录

Required pam_access.so

## 9.配置前端go语言环境

安装go语言
```shell
cd download/
wget https://golang.google.cn/dl/go1.17.3.linux-amd64.tar.gz
tar -C /usr/local -xzf go1.17.3.linux-amd64.tar.gz

# 在 /etc/profile中设置环境变量
export PATH=$PATH:/usr/local/go/bin

source /etc/profile

go version

#设置代理
go env -w GOPROXY=https://goproxy.cn,direct 

# 安装插件
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

cp /root/go/bin/protoc-gen-go-grpc /usr/local/bin/
cp /root/go/bin/protoc-gen-go /usr/local/bin/

```

安装protuc
```shell
https://github.com/protocolbuffers/protobuf/releases/download/v3.19.4/protobuf-all-3.19.4.tar.gz
tar -xzf protobuf-all-3.19.4.tar.gz
cd protobuf-3.19.4
./configure -prefix=/usr/local/
make && make install
protoc --version
# libprotoc 3.11.2
```

拉取项目
```shell
git clone https://github.com/RileyWen/Crane-FrontEnd.git # 克隆项目代码

mkdir Crane-FrontEnd/out
mkdir Crane-FrontEnd/generated/protos
```

编译项目
```shell
# 在Crane-FrontEnd/protos目录下
protoc --go_out=../generated --go-grpc_out=../generated ./*

# 在Crane-FrontEnd/out目录下
go build Crane-FrontEnd/cmd/sbatchx/sbatchx.go
```

部署前端命令
```shell
ln -s /nfs/home/testCrane/Crane-FrontEnd/out/sbatchx /usr/local/bin/sbatchx
ln -s /nfs/home/testCrane/Crane-FrontEnd/out/scontrol /usr/local/bin/scontrol
ln -s /nfs/home/testCrane/Crane-FrontEnd/out/sacctmgr /usr/local/bin/sacctmgr
```

## 10.部署文件同步

在所有节点安装rsync
```shell
yum -y install rsync

# 安装完成后，使用rsync –-help命令可查看 rsync 相关信息
```

在CraneCtld安装inotify
```shell
yum install -y epel-release
yum --enablerepo=epel install inotify-tools
```

编写监听脚本，并在后台自动运行
```shell
 vim /etc/Crane/inotifyrsync.sh

#####
inotifywait -mrq --timefmt '%d/%m/%y %H:%M' --format '%T %w%f' -e modify,delete,create,attrib /etc/Crane/ | while read file
do
        for i in {cn01,cn02,cn03,cn04,cn05,cn06,cn07,cn08,cn09,cn10}
        do
                rsync -avPz --progress --delete /etc/Crane/ $i:/etc/Crane/
        done
echo "${file} was synchronized"
done
########

chmod 755 /etc/Crane/inotifyrsync.sh

/etc/Crane/inotifyrsync.sh &
echo "/etc/Crane/inotifyrsync.sh &" >> /etc/rc.local
```
