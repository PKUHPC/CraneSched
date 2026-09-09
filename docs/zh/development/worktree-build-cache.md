# 多 worktree 构建缓存 SOP

本文适用于在同一台开发机上为多个 CraneSched worktree 编译后端。目标是让每个 worktree 保持独立的 CMake 构建目录，同时共享编译器缓存，避免重复编译 protobuf、gRPC 以及其他大型依赖。

## 结论

- 每个 worktree 必须使用独立的 build 目录。不要复制或软链接另一个 worktree 的整个 CMake build 目录。
- 多个 worktree 可以安全共享同一个 `ccache` 目录。当前开发环境的默认目录是 `/root/.cache/ccache`，以 `ccache --get-config cache_dir` 的实际输出为准。
- `CPM_SOURCE_CACHE` 只缓存 CPM 的下载脚本。在当前 CraneSched 的 CMake 结构中，它不会共享已经编译好的依赖库。
- 不要把另一个 worktree 的 `_deps` 目录直接作为当前 worktree 的 `FETCHCONTENT_BASE_DIR`。其中包含源码路径、构建路径和 CMake cache，容易造成路径污染，也不适合并发配置。
- 当前工程通过 `add_subdirectory` 和 `FetchContent` 构建依赖，尚没有可以稳定跨 worktree 复用的依赖安装前缀。真正共享 `.a/.so` 需要后续将依赖改为独立安装前缀并通过 `find_package` 接入。

## 标准流程

### 1. 创建独立 worktree

```bash
cd /work/CraneSched
git fetch origin master
git worktree add /work/CraneSched-<topic> -b codex/<topic> origin/master
```

源码目录和构建目录使用不同路径，便于一眼区分：

```text
/work/CraneSched-<topic>       # 源码 worktree
/work/CraneSched-<topic>-build # 独立 CMake build tree
```

### 2. 配置共享 ccache

先确认使用的是开发机已有的 ccache，而不是为每个 worktree 新建缓存：

```bash
ccache --get-config cache_dir
ccache --show-stats
```

CraneSched 的顶层 `CMakeLists.txt` 会自动查找 `ccache`。也可以在配置时显式指定：

```bash
cmake -S /work/CraneSched-<topic> \
  -B /work/CraneSched-<topic>-build \
  -G Ninja \
  -DCMAKE_C_COMPILER_LAUNCHER=ccache \
  -DCMAKE_CXX_COMPILER_LAUNCHER=ccache
```

如果不同 worktree 的绝对路径导致命中率较低，可以在同一开发机上把 `/work` 设为 ccache 的公共基准目录，并关闭当前工作目录参与哈希：

```bash
export CCACHE_BASEDIR=/work
export CCACHE_NOHASHDIR=1
```

Debug 构建还应使用统一的调试路径映射，避免缓存命中的目标文件把调试信息指向另一个 worktree：

```bash
cmake -S /work/CraneSched-<topic> \
  -B /work/CraneSched-<topic>-build \
  -G Ninja \
  -DCMAKE_C_FLAGS=-fdebug-prefix-map=/work=. \
  -DCMAKE_CXX_FLAGS=-fdebug-prefix-map=/work=.
```

只在本机的 worktree 之间共享时使用上述设置；如果需要保留每个 worktree 的绝对调试路径，省略 `CCACHE_NOHASHDIR`，接受较低的跨 worktree 命中率。

### 3. 配置并构建

推荐把选项固定在每个 worktree 自己的 build 目录中：

```bash
cmake -S /work/CraneSched-<topic> \
  -B /work/CraneSched-<topic>-build \
  -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCRANE_ENABLE_TESTS=ON \
  -DCRANE_ENABLE_BPF=OFF \
  -DCRANE_ENABLE_TRACING=OFF \
  -DCMAKE_C_COMPILER_LAUNCHER=ccache \
  -DCMAKE_CXX_COMPILER_LAUNCHER=ccache

cmake --build /work/CraneSched-<topic>-build --parallel
ccache --show-stats
```

如果主机内存有限，使用项目约定的 systemd scope：

```bash
systemd-run --scope -p MemoryMax=15G -- \
  cmake --build /work/CraneSched-<topic>-build --parallel
```

### 4. 修改后增量构建

修改源码后只重新构建自己的 build 目录：

```bash
cmake --build /work/CraneSched-<topic>-build --parallel --target cranectld
```

切换到另一个 worktree 时，重复相同命令即可。未修改的编译单元会从共享 ccache 返回，依赖的链接步骤仍在当前 worktree 的 build 目录中完成。

## 常见错误

### 复用旧 build 目录

不要执行以下操作：

```bash
ln -s /work/CraneSched/cmake-build-debug /work/CraneSched-<topic>/cmake-build-debug
cp -a /work/CraneSched/cmake-build-debug /work/CraneSched-<topic>-build
```

CMake cache 会记录 `CMAKE_HOME_DIRECTORY`、源码绝对路径、生成器和依赖目标路径。跨 worktree 复用后，典型表现是 `No rule to make target ...`、目标文件仍指向旧源码，或两个 worktree 并发构建互相覆盖。

### 把 `_deps` 当作已安装库共享

`_deps` 同时包含依赖源码、依赖自己的 build tree 和 FetchContent 的 stamp 文件，不是可移植的库安装目录。即使一次构建成功，也不能据此保证另一个 worktree 的 CMake 配置和链接安全。

### 误以为 `CPM_SOURCE_CACHE` 会复用所有依赖

当前项目中的 `CPM_SOURCE_CACHE` 只影响 `CPM.cmake` 本身的下载位置；gRPC、protobuf 等依赖仍由 `FetchContent` 在当前 build tree 中配置和构建。它可以减少少量下载/配置开销，但不能替代 ccache，也不能替代独立依赖前缀。

## 验证缓存是否生效

```bash
ccache --show-stats
cmake --build /work/CraneSched-<topic>-build --clean-first --parallel
ccache --show-stats
```

观察 `cache hit` 和 `cache miss`。如果不同 worktree 之间始终 miss，先检查：

1. 编译器、编译器版本、构建类型和 CMake 选项是否一致。
2. `ccache --get-config cache_dir` 是否指向同一个目录。
3. 是否设置了 `CCACHE_BASEDIR=/work`，以及 Debug 构建是否使用了统一的 `-fdebug-prefix-map`。
4. 是否误用了另一个 worktree 的 build 目录或 `_deps` 目录。
