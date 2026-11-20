#!/usr/bin/env bash
set -euo pipefail
download_proxy=${HTTPS_PROXY:-}
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy

[[ "$(id -u)" == 1001 && "$(id -g)" == 1001 ]]
[[ "$CI_DEV_IMAGE" =~ @sha256:[a-f0-9]{64}$ ]]
[[ "$CI_POD_UID" =~ ^[a-f0-9-]{36}$ ]]

workspace=/workspace
generation="$CI_POD_UID"
golden_root="/ci/golden/native-rhel10-amd64/${CI_DEV_IMAGE##*@sha256:}/ci-debug"
generation_root="$golden_root/trees/$generation"
product_checkout="$PWD"
source_sha=$(git rev-parse HEAD)
[[ -d "$workspace" && -z "$(find "$workspace" -mindepth 1 -print -quit)" ]]
[[ -d "$generation_root/workspace" ]]

export SOURCE_DATE_EPOCH="$(git log -1 --format=%ct)"

mkdir -p "$generation_root" "$CCACHE_DIR" "$CCACHE_TEMPDIR" "$CPM_SOURCE_CACHE"

git -C "$workspace" init
git -C "$workspace" remote add origin "$(git -C "$product_checkout" remote get-url origin)"
git -C "$workspace" fetch --update-shallow --depth=1 --no-tags "$product_checkout" "$source_sha"
git -C "$workspace" checkout --force --detach "$source_sha"
[[ "$(git -C "$workspace" rev-parse HEAD)" == "$source_sha" ]]

cd "$workspace"
HTTPS_PROXY="$download_proxy" cmake --preset ci-debug
cmake --build --preset ci-debug --target crane_unit_tests --parallel "${BUILD_JOBS:-6}"
ctest --test-dir cmake-build-ci-debug --output-on-failure
cmake --build --preset ci-debug --parallel "${BUILD_JOBS:-6}"

[[ "$(git rev-parse HEAD)" == "$source_sha" ]]
[[ -z "$(git status --porcelain --untracked-files=no)" ]]
previous=$(readlink -e "$golden_root/latest" || true)
for old_path in "$golden_root"/trees/*; do
  [[ -d "$old_path" ]] || continue
  old_workspace="$old_path/workspace"
  if [[ "$old_workspace" != "$generation_root/workspace" && "$old_workspace" != "$previous" ]]; then
    rm -rf -- "$old_path"
  fi
done

ln -s "trees/$generation/workspace" "$golden_root/latest-$generation"
mv -Tf "$golden_root/latest-$generation" "$golden_root/latest"
