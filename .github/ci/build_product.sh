#!/usr/bin/env bash
set -euo pipefail
download_proxy=${HTTPS_PROXY:-}
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy

[[ "$CI_DEV_IMAGE" =~ @sha256:[a-f0-9]{64}$ ]]
getent passwd "$(id -u)" >/dev/null
getent group "$(id -g)" >/dev/null

source_sha="$CI_SOURCE_SHA"
source_checkout="$PWD"
golden_root="/ci/golden/native-rhel10-amd64/${CI_DEV_IMAGE##*@sha256:}/ci-debug"
artifact_dir="/ci/attempts/${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}/build"
overlay_root=/run/ci-overlay
package_dir=

mkdir -p "$(dirname "$artifact_dir")" "$overlay_root/upper" "$overlay_root/work" /workspace
mkdir "$artifact_dir"
git config --global --add safe.directory /workspace

overlay_mounted() {
  findmnt -rn -t fuse.fuse-overlayfs --mountpoint /workspace >/dev/null
}

cleanup_overlay() {
  local status=$?
  trap - EXIT
  cd /
  if overlay_mounted; then
    fusermount3 -u /workspace || status=1
  fi
  if [[ -n "$package_dir" ]]; then
    rm -rf -- "$package_dir"
  fi
  exit "$status"
}
trap cleanup_overlay EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if lower=$(readlink -e "$golden_root/latest") && [[ -d "$lower/.git" ]]; then
  fuse-overlayfs -o "lowerdir=$lower,upperdir=$overlay_root/upper,workdir=$overlay_root/work" /workspace ||
    echo '::warning::fuse-overlayfs failed to mount the golden tree.'
fi

if overlay_mounted; then
  echo "Using golden tree: $lower"
else
  echo 'Using a private cold build.'
  git init /workspace
fi

git -C /workspace config remote.origin.url "$(git -C "$source_checkout" remote get-url origin)"
git -C /workspace fetch --update-shallow --depth=1 --no-tags "$source_checkout" "$source_sha"
if git -C /workspace rev-parse --verify HEAD >/dev/null 2>&1; then
  git -C /workspace update-index --refresh
fi
git -C /workspace checkout --force --detach "$source_sha"
cd -P /workspace
[[ "$(git rev-parse HEAD)" == "$source_sha" ]]
[[ -z "$(git status --porcelain --untracked-files=no)" ]]

export SOURCE_DATE_EPOCH="$(git log -1 --format=%ct)"
export CPM_SOURCE_CACHE=/ci/cache/cpm
mkdir -p "$CPM_SOURCE_CACHE" "$CCACHE_DIR" "$CCACHE_TEMPDIR"

unformatted="$(gofmt -l go)"
if [[ -n "$unformatted" ]]; then
  printf '%s\n' "$unformatted"
  echo '::error::Run gofmt on the listed files.'
  exit 1
fi

make -C go protos
(cd go && go vet -mod=readonly ./...)
make -C go test

HTTPS_PROXY="$download_proxy" cmake --preset ci-debug
cmake --build --preset ci-debug --target crane_unit_tests --parallel "${BUILD_JOBS:-6}"
ctest --test-dir cmake-build-ci-debug --output-on-failure
# Build the opt-in kernel fixture for the isolated integration environment.
cmake --preset ci-debug -DCRANE_ENABLE_KERNEL_TESTS=ON
cmake --build --preset ci-debug --target oom_cgroup_integration_test --parallel "${BUILD_JOBS:-6}"
mkdir -p "$artifact_dir/kernel"
cp cmake-build-ci-debug/test/Craned/oom_cgroup_integration_test "$artifact_dir/kernel/"
cmake --build --preset ci-debug --parallel "${BUILD_JOBS:-6}"

make -C go clean
make -C go all tool PREFIX=/usr STRIP=true CHECK_GPU=0

package_dir=$(mktemp -d /tmp/crane-packages.XXXXXXXX)

(cd cmake-build-ci-debug && cpack -G RPM -B "$package_dir")

GITHUB_TOKEN="$CI_PACKAGING_TOKEN" make -C go package SKIP_REBUILD=true CHECK_GPU=0 \
  GORELEASER_SNAPSHOT=false GORELEASER_RELEASE_ARGS=--skip=publish,validate
shopt -s nullglob
packages=("$package_dir"/*.rpm go/build/dist/*.rpm)

expected_version="$(< VERSION)"
expected_names=(cranesched-cranectld cranesched-craned cranesched-client cranesched-plugin cranesched-slurm-compat)
[[ "${#packages[@]}" == "${#expected_names[@]}" ]]
declare -A actual_names=()
for package in "${packages[@]}"; do
  metadata="$(rpm -qp --queryformat '%{NAME} %{VERSION} %{ARCH}' "$package")"
  read -r name version architecture <<< "$metadata"
  [[ "$version" == "$expected_version" && "$architecture" == x86_64 ]]
  actual_names["$name"]=1
done

for name in "${expected_names[@]}"; do
  [[ -n "${actual_names[$name]:-}" ]]
done

mkdir -p "$artifact_dir/packages/rpm"
cp "${packages[@]}" "$artifact_dir/packages/rpm/"
printf 'rpm_dir=%s\n' "$artifact_dir/packages/rpm" >> "$GITHUB_OUTPUT"
