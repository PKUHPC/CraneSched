#!/usr/bin/env bash
set -euo pipefail
proxy_environment=()
for name in HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy; do
  value=${!name:-}
  if [[ -n "$value" ]]; then
    proxy_environment+=("$name=$value")
  fi
done
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy

run_with_proxy() {
  if ((${#proxy_environment[@]})); then
    env "${proxy_environment[@]}" "$@"
  else
    "$@"
  fi
}

[[ "$(id -u)" == 1001 && "$(id -g)" == 1001 ]]
[[ "$CI_DEV_IMAGE" =~ @sha256:[a-f0-9]{64}$ ]]
[[ "$CI_POD_UID" =~ ^[a-f0-9-]{36}$ ]]
product_ref=${CI_PRODUCT_REF:-master}
[[ "$(git branch --show-current)" == "$product_ref" ]]

workspace=/workspace
generation="$CI_POD_UID"
generation_root="$CI_GOLDEN_ROOT/trees/$generation"
product_checkout="$PWD"
source_sha=$(git rev-parse HEAD)
[[ -d "$workspace" && -z "$(find "$workspace" -mindepth 1 -print -quit)" ]]
[[ -d "$generation_root/workspace" ]]

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
git -C "$workspace" remote add origin "$(git -C "$product_checkout" remote get-url origin)"
git -C "$workspace" fetch --no-tags "$product_checkout" "$source_sha"
git -C "$workspace" checkout --force --detach "$source_sha"
[[ "$(git -C "$workspace" rev-parse HEAD)" == "$source_sha" ]]

cd "$workspace"
run_with_proxy cmake --preset ci-debug
run_with_proxy cmake --build --preset ci-debug --target crane_unit_tests --parallel "${BUILD_JOBS:-6}"
ctest --test-dir cmake-build-ci-debug --output-on-failure
run_with_proxy cmake --build --preset ci-debug --parallel "${BUILD_JOBS:-6}"

touch "$generation_root"
previous=$(readlink -e "$CI_GOLDEN_ROOT/latest" || true)
ln -s "trees/$generation/workspace" "$CI_GOLDEN_ROOT/latest-$generation"
mv -Tf "$CI_GOLDEN_ROOT/latest-$generation" "$CI_GOLDEN_ROOT/latest"

latest=$(readlink -e "$CI_GOLDEN_ROOT/latest")
for old_path in "$CI_GOLDEN_ROOT"/trees/*; do
  [[ -d "$old_path" ]] || continue
  old_workspace="$old_path/workspace"
  if [[ "$old_workspace" != "$latest" && "$old_workspace" != "$previous" ]]; then
    rm -rf -- "$old_path"
  fi
done
