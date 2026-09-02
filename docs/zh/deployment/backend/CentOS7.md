# CentOS 7 部署指南

!!! warning
    CentOS 7 已达到**生命周期终止（EOL）**。鹤思依赖现代编译器，本指南仅用于必须继续运行 CentOS 7 的存量集群，且不再保证长期维护。请优先使用 Rocky Linux 9 等受支持的发行版。

本指南以 x86-64 架构、CGroup v1 和独立构建节点为基准。以下命令除特别说明外，均应在**构建节点**上以 **root** 用户执行。构建产物需要分发到其他节点时，应在集群中最旧的 CPU 型号上构建，或按本文关闭 `-march=native`。

## 1. 配置构建环境

CentOS 7 的公共软件源可能已经不可用。请先将 Yum 配置为可用的 Vault 或内部镜像，再安装附加仓库和完整的构建工具：

```bash
yum install -y epel-release centos-release-scl-rh
yum install -y \
    ca-certificates wget curl tar gzip bzip2 xz unzip make which \
    patch autoconf automake libtool flex bison pkgconfig ninja-build \
    devtoolset-11 rh-git218
```

启用较新的引导编译器和 Git：

```bash
source scl_source enable devtoolset-11
source scl_source enable rh-git218
```

如果需要在新的登录会话中继续构建，可将以上两行加入构建用户的 `~/.bash_profile`。

## 2. 环境准备

### 2.1 检查 CGroup 版本

CentOS 7 通常使用 CGroup v1。构建前先确认目标计算节点的实际挂载方式：

```bash
mount | grep cgroup
```

本文后续会显式设置 `-DCRANE_ENABLE_CGROUP_V2=OFF`。不要依赖项目默认值，因为默认值可能随版本变化。

### 2.2 配置 SELinux

先在测试环境验证 SELinux 策略。若暂时没有适用的策略，可在所有节点上禁用 SELinux：

```bash
setenforce 0
sed -i s#SELINUX=enforcing#SELINUX=disabled# /etc/selinux/config
```

### 2.3 同步系统时间

```bash
yum install -y ntp ntpdate
systemctl enable --now ntpd
timedatectl set-timezone Asia/Shanghai
```

### 2.4 配置防火墙

!!! tip
    多节点集群需要在**每个节点**上配置防火墙，否则节点间通信会失败。实际端口以 `/etc/crane/config.yaml` 为准。

可以禁用防火墙：

```bash
systemctl disable --now firewalld
```

如果需要保持防火墙启用，请开放鹤思配置使用的端口。默认配置通常需要：

```bash
firewall-cmd --add-port=10013/tcp --permanent --zone=public
firewall-cmd --add-port=10012/tcp --permanent --zone=public
firewall-cmd --add-port=10011/tcp --permanent --zone=public
firewall-cmd --add-port=10010/tcp --permanent --zone=public
firewall-cmd --add-port=873/tcp --permanent --zone=public
firewall-cmd --reload
```

## 3. 安装项目依赖

### 3.1 系统开发库

```bash
yum install -y \
    libstdc++-devel libstdc++-static \
    openssl-devel libcurl-devel pam-devel \
    zlib-devel zlib-static libaio-devel systemd-devel
```

Lua 支持默认开启，打包构建使用固定版本的内置 Lua。仅在
`CRANE_FULL_DYNAMIC=ON` 或 `-DCRANE_STATIC_LUA=OFF` 时安装 `lua-devel`；
只有不需要 Lua hook 时才使用 `-DCRANE_ENABLE_LUA=OFF`。

仅在启用 AddressSanitizer 或 ThreadSanitizer 时，才需要安装对应的 `devtoolset-11-libasan-devel` 或 `devtoolset-11-libtsan-devel`，生产构建不需要默认安装它们。

### 3.2 安装 libcgroup

CentOS 7 仓库中的 libcgroup 版本过旧。安装与项目当前内置版本一致的 libcgroup 3.1.0，并关闭该库中不需要的 systemd 集成：

```bash
wget https://github.com/libcgroup/libcgroup/releases/download/v3.1.0/libcgroup-3.1.0.tar.gz
echo "976ec4b1e03c0498308cfd28f1b256b40858f636abc8d1f9db24f0a7ea9e1258  libcgroup-3.1.0.tar.gz" | sha256sum -c -
tar -xzf libcgroup-3.1.0.tar.gz
cd libcgroup-3.1.0
./configure --prefix=/usr/local --disable-systemd
make -j"$(nproc)"
make install
cd ..
```

注册动态库和 pkg-config 路径并验证安装：

```bash
printf '/usr/local/lib\n/usr/local/lib64\n' > /etc/ld.so.conf.d/crane-local.conf
ldconfig
export PKG_CONFIG_PATH="/usr/local/lib/pkgconfig:/usr/local/lib64/pkgconfig:${PKG_CONFIG_PATH}"
pkg-config --modversion libcgroup
```

### 3.3 安装 Subid

当前版本在 CMake 配置阶段会查找 `libsubid`，即使集群暂时不使用容器功能也必须提供该开发库。CentOS 7 通常没有足够新的 `shadow-utils-subid-devel`，可从 Shadow 4.9 源码安装：

```bash
wget https://github.com/shadow-maint/shadow/releases/download/v4.9/shadow-4.9.tar.gz
tar -xzf shadow-4.9.tar.gz
cd shadow-4.9
./configure --prefix=/usr/local \
    --without-libcrack \
    --without-tcb \
    --without-nscd \
    --without-group-name-max-length \
    LIBS="-lpam_misc -lpam"
make -j"$(nproc)"
# 只安装 libsubid，避免覆盖 CentOS 自带的 passwd、useradd 等账号工具
make -C libsubid install
ldconfig
cd ..
```

确认头文件和动态库存在：

```bash
test -f /usr/local/include/shadow/subid.h
test -f /usr/local/lib/libsubid.so || test -f /usr/local/lib64/libsubid.so
```

## 4. 安装工具链

鹤思需要以下工具链版本：

* CMake >= 3.24
* g++ >= 14，或 clang++ >= 19

CentOS 7 自带的 GCC 4.8 无法构建当前版本。

### 4.1 安装 CMake

```bash
wget https://github.com/Kitware/CMake/releases/download/v3.26.4/cmake-3.26.4-linux-x86_64.sh
bash cmake-3.26.4-linux-x86_64.sh --prefix=/usr/local --skip-license
/usr/local/bin/cmake --version
```

### 4.2 安装 GCC 14

使用 devtoolset-11 作为引导编译器，将 GCC 14.2 安装到通用前缀 `/usr/local`：

```bash
source scl_source enable devtoolset-11

wget https://ftp.gnu.org/gnu/gcc/gcc-14.2.0/gcc-14.2.0.tar.gz
tar -xzf gcc-14.2.0.tar.gz
cd gcc-14.2.0
./contrib/download_prerequisites
mkdir build && cd build
../configure --prefix=/usr/local \
    --enable-checking=release \
    --enable-languages=c,c++ \
    --disable-multilib
make -j"$(nproc)"
make install
cd ../..
```

激活并验证实际使用的编译器。不要仅检查 `gcc` 命令是否存在：

```bash
export PATH="/usr/local/bin:${PATH}"
export LD_LIBRARY_PATH="/usr/local/lib64:/usr/local/lib:${LD_LIBRARY_PATH}"
hash -r

command -v gcc g++
gcc -dumpfullversion
g++ -dumpfullversion
```

两条版本命令都应输出 `14.x`，且 `command -v` 应指向 `/usr/local/bin`。

!!! tip
    CentOS 7 自带的 Binutils 较旧。普通构建通常可以继续，但 GCC 官方要求 Binutils 2.35+ 才能可靠使用 LTO。如果 CMake 报告 IPO/LTO 或链接器错误，可将 Binutils 2.41 等较新版本安装到独立前缀后重新配置；无需为普通 GCC 构建额外安装 LLDB。

## 5. 可选：安装 PMIx 和 UCX

只有需要通过 `crun --mpi=pmix` 启动 MPI 作业时才需要 PMIx；UCX 仅用于 InfiniBand、RoCE 等高速网络上的可选直连优化。

CentOS 7 上构建 OpenPMIx 前需要：

```bash
yum install -y libevent-devel hwloc-devel
```

具体版本、Open MPI 构建方式和 UCX 配置请参阅 [PMIx 使用指南](../configuration/pmix.md)。安装时还应遵循以下原则：

1. PMIx、MPI 和 UCX 应安装到所有运行节点均可访问的固定路径，或分别部署到每个节点。
2. 构建前设置 UCX 的 `PKG_CONFIG_PATH`，并用 `pkg-config --libs ucx` 验证；否则 CraneSched 不会编译 UCX 支持。
3. 使用自定义共享库路径时，在 CMake 中添加 `-DCMAKE_INSTALL_RPATH_USE_LINK_PATH=ON`，并在每个运行节点用 `ldd` 验证库解析结果。
4. 安装 OpenPMIx 并不能替代 MPI 实现；运行 MPI 程序仍需 Open MPI 4.1+ 等支持 PMIx 的 MPI。

## 6. 构建和安装

```bash
git clone https://github.com/PKUHPC/CraneSched.git
cd CraneSched

cmake -S . -B build -G Ninja \
    -DCMAKE_C_COMPILER=/usr/local/bin/gcc \
    -DCMAKE_CXX_COMPILER=/usr/local/bin/g++ \
    -DCMAKE_BUILD_TYPE=Release \
    -DCRANE_USE_SYSTEM_LIBCGROUP=ON \
    -DCRANE_ENABLE_CGROUP_V2=OFF \
    -DCRANE_NATIVE_ARCH_OPT=OFF \
    -DSUBID_INCLUDE_DIR=/usr/local/include/shadow \
    -DSUBID_LIBRARY=/usr/local/lib/libsubid.so
cmake --build build -j"$(nproc)"
```

如果 `libsubid.so` 实际安装在 `/usr/local/lib64`，相应调整 `SUBID_LIBRARY`。如果启用了 PMIx，再添加：

```bash
-DWITH_PMIX=/path/to/pmix \
-DCMAKE_INSTALL_RPATH_USE_LINK_PATH=ON
```

!!! note
    `CRANE_NATIVE_ARCH_OPT` 默认开启。本文将其关闭，以便同一构建产物安全分发到不同 CPU 型号的节点。如果所有节点 CPU 完全一致并且更重视本机优化，可以显式改为 `ON`。

安装前检查关键配置和动态库：

```bash
cmake -LA -N build | grep -E 'CMAKE_(C|CXX)_COMPILER|CRANE_ENABLE_CGROUP_V2|CRANE_NATIVE_ARCH_OPT|WITH_PMIX'
ldd build/src/CraneCtld/cranectld | grep 'not found' && exit 1 || true
ldd build/src/Craned/craned | grep 'not found' && exit 1 || true
```

源码安装：

```bash
cmake --install build
```

## 7. 构建和安装 RPM

CentOS 7 自带的 RPM 不支持 zstd 压缩。生成需要在 CentOS 7 上安装的软件包时，覆盖项目的默认压缩方式：

```bash
yum install -y rpm-build
cpack --config build/CPackConfig.cmake \
    -G RPM \
    -D CPACK_RPM_COMPRESSION_TYPE=gzip
```

前端项目通过 GoReleaser 生成 RPM 时，也要将 `.goreleaser.yaml` 中各 RPM 包的 `compression` 设置为 `gzip`。该修改只影响软件包压缩格式，不需要降低 TLS 版本或修改业务代码。

安装前检查包内容和依赖：

```bash
rpm -qpl build/*.rpm
rpm -qpR build/*.rpm
```

不要使用 `rpm --nodeps` 绕过缺失依赖。自定义路径中的 PMIx、UCX 或 Subid 不一定会被系统包管理器正确解析；必须确保目标节点存在对应动态库，并在安装后检查：

```bash
# 在控制节点
if ldd /usr/bin/cranectld | grep 'not found'; then exit 1; fi

# 在计算节点
if ldd /usr/bin/craned | grep 'not found'; then exit 1; fi
```

更完整的打包流程请参阅[打包指南](../packaging.md)。多节点部署请参阅[多节点部署指南](../configuration/multi-node.md)。

## 8. 安装和配置数据库

外部 MongoDB 仅在**控制节点**侧需要。CentOS 7 支持的最新 MongoDB 版本为 7.0，具体安装和嵌入式数据库选项请参阅[数据库配置指南](../configuration/database.md)。

## 9. PAM 模块设置

PAM 模块配置是可选的，但建议用于生产集群。请在集群服务验证正常后，再按照 [PAM 模块配置指南](../configuration/pam.md)启用访问控制，以免配置错误导致 SSH 登录被锁定。

## 10. 配置和启动服务

先按照[集群配置指南](../configuration/config.md)准备配置文件。RPM 安装脚本会自动创建 `crane` 系统用户；仅在源码安装并使用 systemd 时需要手动创建：

```bash
groupadd --system crane 2>/dev/null || true
useradd --system --gid crane --shell /sbin/nologin --create-home crane 2>/dev/null || true
```

对于需要锁定大量内存的 MPI、RDMA 或加速器作业，建议为所有计算节点上的 `craned.service` 配置 memlock：

```bash
mkdir -p /etc/systemd/system/craned.service.d
cat > /etc/systemd/system/craned.service.d/override.conf <<'EOF'
[Service]
LimitMEMLOCK=infinity
EOF
```

启动服务并检查日志：

```bash
systemctl daemon-reload
systemctl enable --now cranectld  # 控制节点
systemctl enable --now craned     # 计算节点

# 在对应类型的节点上检查相应服务
systemctl --no-pager --full status cranectld
systemctl --no-pager --full status craned
```

直接运行二进制文件仅建议用于调试：

```bash
build/src/CraneCtld/cranectld  # 控制节点
build/src/Craned/craned        # 计算节点
```
