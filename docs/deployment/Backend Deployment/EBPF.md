# eBPF for GRES on cgroup v2
Purpose: When using cgroup v2 with GRES (Generic RESources), CraneSched relies on an eBPF program to enforce device-level GRES limits. Therefore, eBPF must be configured and loaded on systems where GRES is managed under cgroup v2.

CraneSched supports cgroup v2; if you need GRES enforcement, eBPF must be configured.
The eBPF program is compiled with clang; please ensure the system has clang 17 or newer (clang 19 instructions below).
## 1. Clang 19 Installation Guide

Install prerequisites:
```bash
dnf install \
    bpftool \
    bcc \
    bcc-tools \
    elfutils-libelf-devel \
    zlib-devel
```

Build and install Clang 19 from source:
```bash
# Clone LLVM 19.1.0
git clone --depth=1 --branch llvmorg-19.1.0 https://github.com/llvm/llvm-project.git \
    llvm-project-19.1.0
cd llvm-project-19.1.0/

# Build dependencies
dnf install -y libedit-devel ncurses-devel libxml2-devel python3-devel swig

# Build LLVM/Clang
mkdir build && cd build
cmake -DCMAKE_INSTALL_PREFIX='/usr/local' \
    -DCMAKE_BUILD_TYPE='Release' -G Ninja \
    -DLLVM_ENABLE_PROJECTS='clang;clang-tools-extra;lld;lldb' -DLLVM_ENABLE_RUNTIMES=all \
    -DLLVM_TARGETS_TO_BUILD='X86;BPF' ../llvm
ninja && ninja install

# Build and install libc++/libc++abi/libunwind
cd ../
mkdir build-libcxx && cd build-libcxx
cmake -G Ninja -DCMAKE_INSTALL_PREFIX='/usr/local' -DCMAKE_C_COMPILER=clang \
    -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Release -S ../runtimes \
    -DLLVM_ENABLE_RUNTIMES="libcxx;libcxxabi;libunwind"
ninja cxx cxxabi unwind
#ninja check-cxx check-cxxabi check-unwind
ninja install-cxx install-cxxabi install-unwind

# Install ASan and TSan headers and libraries for development builds (compiler-rt)
cd ../
mkdir build-compiler-rt && cd build-compiler-rt
cmake ../compiler-rt -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ \
    -DCMAKE_INSTALL_PREFIX='/usr/local' -DCMAKE_BUILD_TYPE='Release' -G Ninja \
    -DLLVM_CMAKE_DIR=../cmake/modules
ninja install
```

Download and build/install libbpf:
```bash
# Download and extract libbpf
wget https://github.com/libbpf/libbpf/archive/refs/tags/v1.4.6.zip
unzip v1.4.6.zip
cd libbpf-1.4.6/src

# Build and install
make
make install
```

## 2. eBPF System Configuration

In the project build directory:
```bash
cp ./src/Misc/BPF/cgroup_dev_bpf.o /etc/crane/cgroup_dev_bpf.o
```

Check whether child cgroups have the relevant controllers enabled (e.g., cpu, io, memory, etc.):
```bash
cat /sys/fs/cgroup/cgroup.subtree_control
```

Enable controllers for child cgroups:
```bash
echo '+cpuset +cpu +io +memory +pids' > /sys/fs/cgroup/cgroup.subtree_control
```

If you encounter a BPF load failure:

Mount the BPF filesystem:
```bash
mount -t bpf bpf /sys/fs/bpf
```

Mount the BPF debug filesystem:
```bash
mount -t debugfs none /sys/kernel/debug
```

To view device access logs, run:
```bash
cat /sys/kernel/debug/tracing/trace_pipe
```

## 3. Mounting the BPF Filesystem

If you see errors like the following:

```text
libbpf: specified path /sys/fs/bpf/dev_map is not on BPF FS
libbpf: map 'dev_map': failed to auto-pin at '/sys/fs/bpf/dev_map': -22
libbpf: map 'dev_map': failed to create: Invalid argument(-22)
libbpf: failed to load object 'cgroup_dev_bpf.o'
Failed to load BPF object
```

Check whether the BPF filesystem is mounted:
```bash
mount | grep bpf
```

Mount the BPF filesystem:
```bash
mkdir -p /sys/fs/bpf
mount -t bpf bpf /sys/fs/bpf
```