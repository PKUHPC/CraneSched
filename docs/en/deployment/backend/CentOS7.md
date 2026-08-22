# Deployment Guide for CentOS 7

!!! warning
    CentOS 7 has reached **End of Life (EOL)**. CraneSched depends on modern compilers, so this guide is intended only for existing clusters that must continue to run CentOS 7 and is not guaranteed to receive long-term maintenance. Prefer a supported distribution such as Rocky Linux 9.

This guide assumes x86-64, CGroup v1, and a dedicated build node. Unless stated otherwise, run all commands on the **build node** as **root**. If artifacts will be distributed to other nodes, build on the oldest CPU model in the cluster or disable `-march=native` as shown below.

## 1. Configure the Build Environment

The public CentOS 7 repositories may no longer be available. Configure Yum to use a working Vault or internal mirror before installing the additional repositories and complete build toolchain:

```bash
yum install -y epel-release centos-release-scl-rh
yum install -y \
    ca-certificates wget curl tar gzip bzip2 xz unzip make which \
    patch autoconf automake libtool flex bison pkgconfig ninja-build \
    devtoolset-11 rh-git218
```

Enable the newer bootstrap compiler and Git:

```bash
source scl_source enable devtoolset-11
source scl_source enable rh-git218
```

Add these two lines to the build user's `~/.bash_profile` if the build must continue in a new login session.

## 2. Prepare the Environment

### 2.1 Check the CGroup Version

CentOS 7 normally uses CGroup v1. Check the actual mount layout on the target compute nodes before building:

```bash
mount | grep cgroup
```

The build command below explicitly sets `-DCRANE_ENABLE_CGROUP_V2=OFF`. Do not rely on the project default because it may change between releases.

### 2.2 Configure SELinux

Validate the SELinux policy in a test environment first. If no suitable policy is available, disable SELinux on every node:

```bash
setenforce 0
sed -i s#SELINUX=enforcing#SELINUX=disabled# /etc/selinux/config
```

### 2.3 Synchronize System Time

```bash
yum install -y ntp ntpdate
systemctl enable --now ntpd
timedatectl set-timezone Asia/Shanghai
```

### 2.4 Configure the Firewall

!!! tip
    Configure the firewall on **every node** in a multi-node cluster. Otherwise, inter-node communication will fail. The ports in `/etc/crane/config.yaml` are authoritative.

The firewall can be disabled:

```bash
systemctl disable --now firewalld
```

If the firewall must remain enabled, open the ports used by the CraneSched configuration. The default configuration normally requires:

```bash
firewall-cmd --add-port=10013/tcp --permanent --zone=public
firewall-cmd --add-port=10012/tcp --permanent --zone=public
firewall-cmd --add-port=10011/tcp --permanent --zone=public
firewall-cmd --add-port=10010/tcp --permanent --zone=public
firewall-cmd --add-port=873/tcp --permanent --zone=public
firewall-cmd --reload
```

## 3. Install Project Dependencies

### 3.1 System Development Libraries

```bash
yum install -y \
    libstdc++-devel libstdc++-static \
    openssl-devel libcurl-devel pam-devel \
    zlib-devel zlib-static libaio-devel systemd-devel lua-devel
```

Lua support is enabled by default. If `lua-devel` is unavailable from the Vault or internal mirror, configure with `-DCRANE_ENABLE_LUA=OFF`.

Install `devtoolset-11-libasan-devel` or `devtoolset-11-libtsan-devel` only when enabling AddressSanitizer or ThreadSanitizer. Production builds do not need them by default.

### 3.2 Install libcgroup

The libcgroup package in the CentOS 7 repositories is too old. Install libcgroup 3.1.0, matching the version currently bundled by the project, and disable its unnecessary systemd integration:

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

Register the library and pkg-config paths, then verify the installation:

```bash
printf '/usr/local/lib\n/usr/local/lib64\n' > /etc/ld.so.conf.d/crane-local.conf
ldconfig
export PKG_CONFIG_PATH="/usr/local/lib/pkgconfig:/usr/local/lib64/pkgconfig:${PKG_CONFIG_PATH}"
pkg-config --modversion libcgroup
```

### 3.3 Install Subid

The current release looks for `libsubid` during CMake configuration, even if the cluster does not currently use containers. CentOS 7 normally does not provide a sufficiently new `shadow-utils-subid-devel`, so install it from Shadow 4.9 source:

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
# Install only libsubid to avoid replacing CentOS account tools such as passwd and useradd
make -C libsubid install
ldconfig
cd ..
```

Confirm that the header and shared library exist:

```bash
test -f /usr/local/include/shadow/subid.h
test -f /usr/local/lib/libsubid.so || test -f /usr/local/lib64/libsubid.so
```

## 4. Install the Toolchain

CraneSched requires:

* CMake >= 3.24
* g++ >= 14, or clang++ >= 19

The GCC 4.8 supplied by CentOS 7 cannot build the current release.

### 4.1 Install CMake

```bash
wget https://github.com/Kitware/CMake/releases/download/v3.26.4/cmake-3.26.4-linux-x86_64.sh
bash cmake-3.26.4-linux-x86_64.sh --prefix=/usr/local --skip-license
/usr/local/bin/cmake --version
```

### 4.2 Install GCC 14

Use devtoolset-11 as the bootstrap compiler and install GCC 14.2 under the conventional `/usr/local` prefix:

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

Activate and verify the compiler that will actually be used. Do not merely check that a `gcc` command exists:

```bash
export PATH="/usr/local/bin:${PATH}"
export LD_LIBRARY_PATH="/usr/local/lib64:/usr/local/lib:${LD_LIBRARY_PATH}"
hash -r

command -v gcc g++
gcc -dumpfullversion
g++ -dumpfullversion
```

Both version commands must report `14.x`, and `command -v` must point to `/usr/local/bin`.

!!! tip
    The Binutils version supplied by CentOS 7 is old. A normal build can usually proceed, but GCC requires Binutils 2.35+ for reliable LTO support. If CMake reports IPO/LTO or linker errors, install a newer release such as Binutils 2.41 under a separate prefix and reconfigure. LLDB is not required for a normal GCC build.

## 5. Optional: Install PMIx and UCX

PMIx is needed only to launch MPI jobs with `crun --mpi=pmix`. UCX is an optional direct-connection optimization for high-speed networks such as InfiniBand or RoCE.

Install the OpenPMIx build dependencies on CentOS 7:

```bash
yum install -y libevent-devel hwloc-devel
```

See the [PMIx Guide](../configuration/pmix.md) for versions, the Open MPI build, and UCX configuration. Also follow these rules:

1. Install PMIx, MPI, and UCX at a stable path visible to every runtime node, or install them separately on every node.
2. Set UCX's `PKG_CONFIG_PATH` before configuring CraneSched and verify it with `pkg-config --libs ucx`; otherwise UCX support will not be compiled.
3. For custom shared-library paths, add `-DCMAKE_INSTALL_RPATH_USE_LINK_PATH=ON` and verify library resolution with `ldd` on every runtime node.
4. OpenPMIx does not replace an MPI implementation. MPI programs still require a PMIx-capable implementation such as Open MPI 4.1+.

## 6. Build and Install

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

Adjust `SUBID_LIBRARY` if `libsubid.so` was installed under `/usr/local/lib64`. If PMIx is enabled, also add:

```bash
-DWITH_PMIX=/path/to/pmix \
-DCMAKE_INSTALL_RPATH_USE_LINK_PATH=ON
```

!!! note
    `CRANE_NATIVE_ARCH_OPT` is enabled by default. This guide disables it so that one build can be safely distributed across nodes with different CPU models. It may be set to `ON` when every node has an identical CPU and native optimization is preferred.

Check the important configuration values and shared libraries before installing:

```bash
cmake -LA -N build | grep -E 'CMAKE_(C|CXX)_COMPILER|CRANE_ENABLE_CGROUP_V2|CRANE_NATIVE_ARCH_OPT|WITH_PMIX'
ldd build/src/CraneCtld/cranectld | grep 'not found' && exit 1 || true
ldd build/src/Craned/craned | grep 'not found' && exit 1 || true
```

For a source installation:

```bash
cmake --install build
```

## 7. Build and Install RPM Packages

The RPM version supplied by CentOS 7 does not support zstd compression. Override the project's default compression when generating packages that must be installed on CentOS 7:

```bash
yum install -y rpm-build
cpack --config build/CPackConfig.cmake \
    -G RPM \
    -D CPACK_RPM_COMPRESSION_TYPE=gzip
```

When the frontend project generates RPM packages through GoReleaser, also set `compression: gzip` for every RPM entry in `.goreleaser.yaml`. This changes only package compression; lowering the TLS version or modifying application code is not required.

Inspect package contents and dependencies before installing:

```bash
rpm -qpl build/*.rpm
rpm -qpR build/*.rpm
```

Do not use `rpm --nodeps` to bypass missing dependencies. PMIx, UCX, or Subid installed at custom paths may not be resolved correctly by the package manager. Ensure that every target node has the required shared libraries and check after installation:

```bash
# On the control node
if ldd /usr/bin/cranectld | grep 'not found'; then exit 1; fi

# On a compute node
if ldd /usr/bin/craned | grep 'not found'; then exit 1; fi
```

See the [Packaging Guide](../packaging.md) for the complete packaging workflow and the [Multi-node Deployment Guide](../configuration/multi-node.md) for cluster deployment.

## 8. Install and Configure the Database

An external MongoDB deployment is needed only on the **control-node** side. MongoDB 7.0 is the latest release supported on CentOS 7. See the [Database Configuration Guide](../configuration/database.md) for installation and embedded database alternatives.

## 9. PAM Module Setup

The PAM module is optional but recommended for production clusters. Enable it only after the cluster services have been validated, following the [PAM Module Guide](../configuration/pam.md), so that an incorrect configuration does not lock out SSH access.

## 10. Configure and Start Services

Prepare the configuration files using the [Cluster Configuration Guide](../configuration/config.md). The RPM post-installation script creates the `crane` system user automatically. Create it manually only for a source installation that uses systemd:

```bash
groupadd --system crane 2>/dev/null || true
useradd --system --gid crane --shell /sbin/nologin --create-home crane 2>/dev/null || true
```

For MPI, RDMA, or accelerator workloads that lock substantial memory, configure memlock for `craned.service` on every compute node:

```bash
mkdir -p /etc/systemd/system/craned.service.d
cat > /etc/systemd/system/craned.service.d/override.conf <<'EOF'
[Service]
LimitMEMLOCK=infinity
EOF
```

Start the services and inspect their status:

```bash
systemctl daemon-reload
systemctl enable --now cranectld  # Control node
systemctl enable --now craned     # Compute node

# Check the relevant service on each node type
systemctl --no-pager --full status cranectld
systemctl --no-pager --full status craned
```

Run binaries directly only for debugging:

```bash
build/src/CraneCtld/cranectld  # Control node
build/src/Craned/craned        # Compute node
```
