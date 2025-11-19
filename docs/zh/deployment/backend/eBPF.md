# 在 cgroup v2 上使用 eBPF 支持 GRES

在 cgroup v2 上使用 GRES（通用资源，例如 GPU）时，鹤思依赖 eBPF 来强制执行设备级 GRES 限制。

本指南将指导您在计算节点上设置 eBPF。

## 安装 Clang

=== "RHEL/Fedora/CentOS"

    RHEL 9 及以上系统的官方软件源中包含 Clang 19。低于 RHEL 9 的系统（例如 Rocky Linux 8）需要从源码构建 Clang 19+。

    ```bash
    dnf install clang
    ```

=== "Debian/Ubuntu"

    请使用 [https://apt.llvm.org/](https://apt.llvm.org/) 提供的官方 LLVM 软件源安装最新版 Clang。

    安装后，可使用以下命令将 Clang（以 19 为例）设置为默认版本：

    ```bash
    update-alternatives --install /usr/bin/clang clang /usr/bin/clang-19 120 \
    --slave /usr/bin/clang++ clang++ /usr/bin/clang++-19 
    ```

=== "从源码构建"

    有关具体的安装信息，请参阅 LLVM 的[官方文档](https://llvm.org/docs/GettingStarted.html)。以下为 Rocky Linux 8 的示例：

    **安装依赖：**
    ```bash
    dnf install \
        bpftool \
        bcc \
        bcc-tools \
        elfutils-libelf-devel \
        zlib-devel
    ```

    **从源码构建并安装：**
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

## 安装 libbpf<small>（可选）</small>

!!! tip
    默认情况下 CraneSched 在启用 `-DCRANE_ENABLE_BPF=ON` 选项时会构建并使用内置的 libbpf。
    
    如果您希望使用系统提供的 libbpf，请按照下文操作，并在 CMake 配置时设置 `-DCRANE_USE_SYSTEM_LIBBPF=ON`。

CraneSched 需要 libbpf 版本 ≥ 1.4.6。

=== "RHEL/CentOS/Fedora"

    RHEL 9 及以上系统的官方软件源中包含满足条件的 libbpf，可通过包管理器安装：

    ```bash
    dnf install libbpf-devel
    ```

=== "Debian/Ubuntu"

    Ubuntu 25.04 及以上系统的官方软件源中包含满足条件的 libbpf，可通过包管理器安装：

    ```bash
    apt install libbpf-dev
    ```

    其他版本的 Debian/Ubuntu 系统需要从源码构建 libbpf。

=== "从源码构建"

    **安装 libbpf 的依赖：**

    ```bash
    # RHEL/CentOS/Fedora
    dnf install zlib-devel elfutils-libelf-devel pkgconf

    # Debian/Ubuntu
    apt install zlib1g-dev libelf-dev pkg-config
    ```

    **构建并安装 libbpf：**

    !!! warning
        在 Debian/Ubuntu 和 RHEL/CentOS/Fedora 系统上，构建和安装 libbpf 的命令略有不同，请根据您的系统执行相应的命令。

    ```bash
    # 下载并解压
    wget https://github.com/libbpf/libbpf/archive/refs/tags/v1.6.2.tar.gz
    tar -xzf v1.6.2.tar.gz
    cd libbpf-1.6.2/src

    # Debian/Ubuntu
    ARCH=$(dpkg-architecture -q DEB_HOST_MULTIARCH)
    make -j$(nproc) install PREFIX=/usr LIBDIR=/usr/lib/$ARCH

    # RHEL/CentOS/Fedora
    make -j$(nproc) install

    # 测试使用
    ldconfig
    pkg-config --cflags --libs libbpf
    ```

## 构建 eBPF 程序

鹤思可以使用 GCC 或 Clang 构建，但编译 eBPF 程序必须使用 Clang 19 或更高版本。

构建鹤思时，请确保 Clang 已正确安装并且**在您的 PATH 中可用**，并设置 CMake 选项 `-DCRANE_ENABLE_BPF=ON`。

例如：

```bash
cmake -G Ninja -DCRANE_ENABLE_BPF=ON -S . -B build
cmake --build build
```

构建后，您应该可在构建输出的 `src/Misc/BPF/` 目录中看到 `cgroup_dev_bpf.o`。

## 安装 eBPF 程序

在项目构建目录中：
```bash
cp ./src/Misc/BPF/cgroup_dev_bpf.o /usr/local/lib64/bpf/
```

检查子 cgroup 是否启用了相关 controller（例如 cpu、io、memory 等）：
```bash
cat /sys/fs/cgroup/cgroup.subtree_control
```

为子 cgroup 启用 controller：
```bash
echo '+cpuset +cpu +io +memory +pids' > /sys/fs/cgroup/cgroup.subtree_control
```

## 挂载 BPF 文件系统

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
