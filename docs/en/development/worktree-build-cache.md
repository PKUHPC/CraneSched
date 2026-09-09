# Build Cache SOP for Multiple Worktrees

This SOP applies when several CraneSched worktrees are built on the same development host. Each worktree keeps an isolated CMake build tree, while compiler results are shared through ccache.

## Rules

- Use one build directory per worktree. Do not copy or symlink another worktree's complete CMake build tree.
- Multiple worktrees may safely share the same `ccache` directory. On the current development host it is `/root/.cache/ccache`; use `ccache --get-config cache_dir` as the source of truth.
- `CPM_SOURCE_CACHE` caches the CPM bootstrap script in the current project. It does not share already-built dependency libraries.
- Do not point `FETCHCONTENT_BASE_DIR` at another worktree's `_deps` directory. `_deps` contains path-bound source, build, and CMake state and is unsafe for concurrent configuration.
- The current project builds dependencies through `add_subdirectory` and `FetchContent`; it does not yet provide a stable cross-worktree dependency install prefix. Reusable `.a`/`.so` files require a future `find_package`-based dependency installation flow.

## Standard Procedure

### 1. Create an isolated worktree

```bash
cd /work/CraneSched
git fetch origin master
git worktree add /work/CraneSched-<topic> -b codex/<topic> origin/master
```

Keep source and build paths separate:

```text
/work/CraneSched-<topic>       # source worktree
/work/CraneSched-<topic>-build # isolated CMake build tree
```

### 2. Use the shared ccache

Verify that the normal host cache is used rather than creating one per worktree:

```bash
ccache --get-config cache_dir
ccache --show-stats
```

The top-level `CMakeLists.txt` auto-detects ccache. It may also be selected explicitly:

```bash
cmake -S /work/CraneSched-<topic> \
  -B /work/CraneSched-<topic>-build \
  -G Ninja \
  -DCMAKE_C_COMPILER_LAUNCHER=ccache \
  -DCMAKE_CXX_COMPILER_LAUNCHER=ccache
```

If absolute worktree paths reduce hit rates, use a common ccache base on the same host:

```bash
export CCACHE_BASEDIR=/work
export CCACHE_NOHASHDIR=1
```

Debug builds should also use a common debug-path mapping so a cache hit does not return an object whose debug info points to a different worktree:

```bash
cmake -S /work/CraneSched-<topic> \
  -B /work/CraneSched-<topic>-build \
  -G Ninja \
  -DCMAKE_C_FLAGS=-fdebug-prefix-map=/work=. \
  -DCMAKE_CXX_FLAGS=-fdebug-prefix-map=/work=.
```

Use these options only when worktrees share the same development host. If each worktree must retain absolute debug paths, omit `CCACHE_NOHASHDIR` and accept a lower cross-worktree hit rate.

### 3. Configure and build

Keep build options in the worktree's own build directory:

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

For hosts with a strict memory limit, use the project convention:

```bash
systemd-run --scope -p MemoryMax=15G -- \
  cmake --build /work/CraneSched-<topic>-build --parallel
```

### 4. Incremental builds

After a source change, build only the current worktree:

```bash
cmake --build /work/CraneSched-<topic>-build --parallel --target cranectld
```

Switching to another worktree only requires the equivalent command there. Unchanged translation units are served from the shared ccache; linking still happens in the current worktree's build tree.

## Common Mistakes

Do not reuse an existing build tree:

```bash
ln -s /work/CraneSched/cmake-build-debug /work/CraneSched-<topic>/cmake-build-debug
cp -a /work/CraneSched/cmake-build-debug /work/CraneSched-<topic>-build
```

CMake records `CMAKE_HOME_DIRECTORY`, absolute source paths, the generator, and dependency target paths. Reusing the tree can produce `No rule to make target ...`, stale source references, or races between concurrent builds.

Do not treat `_deps` as an installed-library cache. It contains dependency source trees, dependency build trees, and FetchContent stamp files, so it is not portable between worktrees.

In this project, `CPM_SOURCE_CACHE` only changes where `CPM.cmake` is downloaded. gRPC, protobuf, and other dependencies are still configured and built by `FetchContent` inside the current build tree. This saves a small download/configuration cost but does not replace ccache or an installed dependency prefix.

## Check Cache Effectiveness

```bash
ccache --show-stats
cmake --build /work/CraneSched-<topic>-build --clean-first --parallel
ccache --show-stats
```

If different worktrees continue to miss the cache, check that the compiler and version, build type, CMake options, and `ccache --get-config cache_dir` are identical. Also check that `CCACHE_BASEDIR=/work` and the debug prefix map are applied, and that no other worktree's build or `_deps` directory is being reused.
