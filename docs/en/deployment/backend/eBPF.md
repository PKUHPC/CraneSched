# Using eBPF for GRES on cgroup v2

When GRES (generic resources such as GPUs) are used with cgroup v2, CraneSched relies on eBPF to enforce device-level limits.

This guide explains how to prepare eBPF support on your compute nodes.

## Install Clang

=== "RHEL/Fedora/CentOS"

    The official repositories for RHEL 9 and newer already provide Clang 19. Systems older than RHEL 9 (for example Rocky Linux 8) need to build Clang 19+ from source.

    ```bash
    dnf install clang
    ```

=== "Debian/Ubuntu"

    Install the latest Clang from the official LLVM repository at [https://apt.llvm.org/](https://apt.llvm.org/).

    After installation, set Clang (19 in this example) as the default using `update-alternatives`:

    ```bash
    update-alternatives --install /usr/bin/clang clang /usr/bin/clang-19 120 \
    --slave /usr/bin/clang++ clang++ /usr/bin/clang++-19
    ```

=== "Build from source"

    Refer to LLVM's [official documentation](https://llvm.org/docs/GettingStarted.html) for installation details. The following example targets Rocky Linux 8.

    **Install dependencies:**
    ```bash
    dnf install \
        bpftool \
        bcc \
        bcc-tools \
        elfutils-libelf-devel \
        zlib-devel
    ```

    **Build and install from source:**
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

## Install libbpf <small>(Optional)</small>

!!! tip
    By default CraneSched builds and uses a bundled libbpf when you configure CMake with `-DCRANE_ENABLE_BPF=ON`.

    To use the system-provided libbpf instead, follow the instructions below and add `-DCRANE_USE_SYSTEM_LIBBPF=ON` to your CMake configuration.

CraneSched requires libbpf version ≥ 1.4.6.

=== "RHEL/CentOS/Fedora"

    For RHEL 9 and later, the official repositories already include a compatible libbpf. Install it through the package manager:

    ```bash
    dnf install libbpf-devel
    ```

=== "Debian/Ubuntu"

    Ubuntu 25.04 and newer provide a compatible libbpf in the official repositories:

    ```bash
    apt install libbpf-dev
    ```

    Other Debian/Ubuntu versions must build libbpf from source.

=== "Build from source"

    **Install libbpf dependencies:**

    ```bash
    # RHEL/CentOS/Fedora
    dnf install zlib-devel elfutils-libelf-devel pkgconf

    # Debian/Ubuntu
    apt install zlib1g-dev libelf-dev pkg-config
    ```

    **Build and install libbpf:**

    !!! warning
        The commands differ slightly between Debian/Ubuntu and RHEL/CentOS/Fedora. Run the ones matching your distribution.

    ```bash
    # Download and extract
    wget https://github.com/libbpf/libbpf/archive/refs/tags/v1.6.2.tar.gz
    tar -xzf v1.6.2.tar.gz
    cd libbpf-1.6.2/

    # Debian/Ubuntu
    ARCH=$(dpkg-architecture -q DEB_HOST_MULTIARCH)
    make -j$(nproc) install PREFIX=/usr LIBDIR=/usr/lib/$ARCH

    # RHEL/CentOS/Fedora
    make -j$(nproc) install

    # Verify availability
    ldconfig
    pkg-config --cflags --libs libbpf
    ```

## Build the eBPF program

CraneSched can be built with GCC or Clang, but the eBPF program must be compiled with Clang 19 or newer.

When building CraneSched, make sure Clang is correctly installed and **available in your PATH**, and set the CMake option `-DCRANE_ENABLE_BPF=ON`.

For example:

```bash
cmake -G Ninja -DCRANE_ENABLE_BPF=ON -S . -B build
cmake --build build
```

After a successful build, you should find `cgroup_dev_bpf.o` under `src/Misc/BPF/` in the build output.

## Install the eBPF program

From the project build directory, copy the compiled BPF object:
```bash
cp ./src/Misc/BPF/cgroup_dev_bpf.o /usr/local/lib64/bpf/
```

Verify that the child cgroups have the relevant controllers enabled (e.g., cpu, io, memory):
```bash
cat /sys/fs/cgroup/cgroup.subtree_control
```

Enable the controllers if necessary:
```bash
echo '+cpuset +cpu +io +memory +pids' > /sys/fs/cgroup/cgroup.subtree_control
```

## Mount the BPF filesystem

If you encounter errors similar to:

```text
libbpf: specified path /sys/fs/bpf/dev_map is not on BPF FS
libbpf: map 'dev_map': failed to auto-pin at '/sys/fs/bpf/dev_map': -22
libbpf: map 'dev_map': failed to create: Invalid argument(-22)
libbpf: failed to load object 'cgroup_dev_bpf.o'
Failed to load BPF object
```

First check whether the BPF filesystem is mounted:

```bash
mount | grep bpf
```

If not, mount it manually:

1. Mount the BPF filesystem:
```bash
mount -t bpf bpf /sys/fs/bpf
```

2. Mount the BPF debug filesystem:
```bash
mount -t debugfs none /sys/kernel/debug
```

3. View device access logs when needed:
```bash
cat /sys/kernel/debug/tracing/trace_pipe
```
