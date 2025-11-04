# 在 cgroup v2 上使用 eBPF 支持 GRES

在 cgroup v2 上使用 GRES（通用资源，例如 GPU）时，鹤思依赖 eBPF 来强制执行设备级 GRES 限制。

本指南将指导您在计算节点上设置 eBPF。

## 1. 安装 Clang 19+

!!! tip
    如果您的系统已经安装了 clang >= 19 或您的发行版提供 clang 19 软件包，则可以跳过此部分。

    有关更多详细信息，请参阅 LLVM 的[官方文档](https://llvm.org/docs/GettingStarted.html)。

安装先决条件：
```bash
dnf install \
    bpftool \
    bcc \
    bcc-tools \
    elfutils-libelf-devel \
    zlib-devel
```

从源码构建并安装 Clang 19：
```bash
# 克隆 LLVM 19.1.0
git clone --depth=1 --branch llvmorg-19.1.0 https://github.com/llvm/llvm-project.git \
    llvm-project-19.1.0
cd llvm-project-19.1.0/

# 构建依赖项
dnf install -y libedit-devel ncurses-devel libxml2-devel python3-devel swig

# 构建 LLVM/Clang
mkdir build && cd build
cmake -DCMAKE_INSTALL_PREFIX='/usr/local' \
    -DCMAKE_BUILD_TYPE='Release' -G Ninja \
    -DLLVM_ENABLE_PROJECTS='clang;clang-tools-extra;lld;lldb' -DLLVM_ENABLE_RUNTIMES=all \
    -DLLVM_TARGETS_TO_BUILD='X86;BPF' ../llvm
ninja && ninja install

# 构建并安装 libc++/libc++abi/libunwind
cd ../
mkdir build-libcxx && cd build-libcxx
cmake -G Ninja -DCMAKE_INSTALL_PREFIX='/usr/local' -DCMAKE_C_COMPILER=clang \
    -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Release -S ../runtimes \
    -DLLVM_ENABLE_RUNTIMES="libcxx;libcxxabi;libunwind"
ninja cxx cxxabi unwind
#ninja check-cxx check-cxxabi check-unwind
ninja install-cxx install-cxxabi install-unwind

# 为开发构建安装 ASan 和 TSan 头文件和库（compiler-rt）
cd ../
mkdir build-compiler-rt && cd build-compiler-rt
cmake ../compiler-rt -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ \
    -DCMAKE_INSTALL_PREFIX='/usr/local' -DCMAKE_BUILD_TYPE='Release' -G Ninja \
    -DLLVM_CMAKE_DIR=../cmake/modules
ninja install
```

下载并构建/安装 libbpf：
```bash
# 下载并解压 libbpf
wget https://github.com/libbpf/libbpf/archive/refs/tags/v1.4.6.zip
unzip v1.4.6.zip
cd libbpf-1.4.6/src

# 构建并安装
make
make install
```

## 2. 使用 eBPF 支持构建

鹤思可以使用 GCC 或 Clang 构建。但是，要使用 eBPF 功能，您必须使用 Clang 19 或更高版本。

构建鹤思时，请确保 Clang 19 已正确安装并且**在您的 PATH 中可用**，并使用以下 CMake 选项构建：

```
-DCRANE_ENABLE_CGROUP_V2=ON
-DCRANE_ENABLE_BPF=ON
```

构建后，您应该在构建输出的 `src/Misc/BPF/` 目录中看到 `cgroup_dev_bpf.o`。

## 3. eBPF 配置

在项目构建目录中：
```bash
cp ./src/Misc/BPF/cgroup_dev_bpf.o /etc/crane/cgroup_dev_bpf.o
```

检查子 cgroup 是否启用了相关控制器（例如 cpu、io、memory 等）：
```bash
cat /sys/fs/cgroup/cgroup.subtree_control
```

为子 cgroup 启用控制器：
```bash
echo '+cpuset +cpu +io +memory +pids' > /sys/fs/cgroup/cgroup.subtree_control
```

## 4. 挂载 BPF 文件系统

如果您看到如下错误：

```text
libbpf: specified path /sys/fs/bpf/dev_map is not on BPF FS
libbpf: map 'dev_map': failed to auto-pin at '/sys/fs/bpf/dev_map': -22
libbpf: map 'dev_map': failed to create: Invalid argument(-22)
libbpf: failed to load object 'cgroup_dev_bpf.o'
Failed to load BPF object
```

检查 BPF 文件系统是否已挂载：

```bash
mount | grep bpf
```

如果未挂载，请执行以下操作：

1. 挂载 BPF 文件系统：
```bash
mount -t bpf bpf /sys/fs/bpf
```

2. 挂载 BPF 调试文件系统：
```bash
mount -t debugfs none /sys/kernel/debug
```

3. 要查看设备访问日志，请运行：
```bash
cat /sys/kernel/debug/tracing/trace_pipe
```
