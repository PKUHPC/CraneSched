#!/usr/bin/env bash
set -euo pipefail
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy

[[ "$(id -u)" == 1001 && "$(id -g)" == 1001 ]]
[[ "$CI_DEV_IMAGE" =~ @sha256:[a-f0-9]{64}$ ]]
[[ "$CI_POD_UID" =~ ^[a-f0-9-]{36}$ ]]
product_ref=${CI_PRODUCT_REF:-master}
[[ "$(git branch --show-current)" == "$product_ref" ]]

workspace=/workspace
generation="$CI_POD_UID"
generation_root="$CI_GOLDEN_ROOT/trees/$generation"
[[ -d "$workspace" && -z "$(find "$workspace" -mindepth 1 -print -quit)" ]]
[[ -d "$generation_root/workspace" && ! -e "$generation_root/source.sha" ]]

export CCACHE_DIR=/ci/cache/ccache
export CCACHE_TEMPDIR=/tmp/ccache
export CCACHE_COMPILERCHECK=content
export CCACHE_NOHARDLINK=true
export CCACHE_NOFILECLONE=true
export CCACHE_NOINODECACHE=true
export CPM_SOURCE_CACHE=/ci/cache/cpm
export SOURCE_DATE_EPOCH="$(git log -1 --format=%ct)"

mkdir -p "$generation_root" "$CCACHE_DIR" "$CCACHE_TEMPDIR" "$CPM_SOURCE_CACHE"

git -C "$workspace" init
git -C "$workspace" remote add origin "$CI_PRODUCT_REPOSITORY"
git -C "$workspace" fetch --depth=1 origin "refs/heads/$product_ref"
source_sha=$(git -C "$workspace" rev-parse FETCH_HEAD)
git -C "$workspace" checkout --force --detach "$source_sha"
[[ "$(git -C "$workspace" rev-parse HEAD)" == "$source_sha" ]]

cd "$workspace"
cmake --preset ci-debug
cmake --build --preset ci-debug --target crane_unit_tests --parallel "${BUILD_JOBS:-6}"
ctest --test-dir cmake-build-ci-debug --output-on-failure
cmake --build --preset ci-debug --parallel "${BUILD_JOBS:-6}"

printf '%s\n' "$source_sha" > "$generation_root/source.sha"
ln -s "trees/$generation/workspace" "$CI_GOLDEN_ROOT/latest-$generation"
mv -Tf "$CI_GOLDEN_ROOT/latest-$generation" "$CI_GOLDEN_ROOT/latest"
