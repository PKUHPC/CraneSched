#!/usr/bin/env bash
set -euo pipefail
download_proxy=${HTTPS_PROXY:-}
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy

[[ "$CI_DEV_IMAGE" =~ @sha256:[a-f0-9]{64}$ ]]
[[ "$(id -u)" == 1001 && "$(id -g)" == 1001 ]]
getent passwd 1001 >/dev/null
getent group 1001 >/dev/null

source_sha="$CI_SOURCE_SHA"
source_checkout="$PWD"
golden_root="/ci/golden/native-rhel10-amd64/${CI_DEV_IMAGE##*@sha256:}/ci-debug"
artifact_dir="/ci/attempts/${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}/build"
overlay_root=/run/ci-overlay
overlay_pid=

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
  if [[ -n "$overlay_pid" ]]; then
    kill "$overlay_pid" 2>/dev/null || true
    wait "$overlay_pid" 2>/dev/null || true
  fi
  exit "$status"
}
trap cleanup_overlay EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ -e "$golden_root/latest" ]]; then
  lower=$(readlink -e "$golden_root/latest")
  [[ -d "$lower/.git" ]]
  echo "Using golden tree: $lower"
  fuse-overlayfs -f -o "lowerdir=$lower,upperdir=$overlay_root/upper,workdir=$overlay_root/work" /workspace &
  overlay_pid=$!
  mounted=false
  for attempt in {1..100}; do
    if overlay_mounted; then
      mounted=true
      break
    fi
    if ! kill -0 "$overlay_pid" 2>/dev/null; then
      wait "$overlay_pid" || true
      overlay_pid=
      break
    fi
    sleep 0.1
  done
  if [[ "$mounted" != true ]]; then
    if overlay_mounted; then
      fusermount3 -u /workspace
    fi
    if [[ -n "$overlay_pid" ]]; then
      kill "$overlay_pid" 2>/dev/null || true
      wait "$overlay_pid" 2>/dev/null || true
      overlay_pid=
    fi
    echo '::warning::fuse-overlayfs did not mount; using a private cold build.'
    git init /workspace
  fi
else
  echo 'No compatible golden tree; using a private cold build.'
  git init /workspace
fi

git -C /workspace config remote.origin.url "$(git -C "$source_checkout" remote get-url origin)"
git -C /workspace fetch --update-shallow --depth=1 --no-tags "$source_checkout" "$source_sha"
if previous_sha=$(git -C /workspace rev-parse --verify HEAD 2>/dev/null); then
  git -C /workspace update-index --refresh
  cmake_changes=$(git -C /workspace diff --name-only "$previous_sha^{tree}" "$source_sha^{tree}" -- CMakeLists.txt CMakePresets.json ':(glob)**/CMakeLists.txt' ':(glob)**/*.cmake')
  if [[ -n "$cmake_changes" ]]; then
    rm -rf /workspace/cmake-build-ci-debug /workspace/generated
  fi
fi
git -C /workspace checkout --force --detach "$source_sha"
cd -P /workspace
[[ "$(git rev-parse HEAD)" == "$source_sha" ]]
[[ -z "$(git status --porcelain --untracked-files=no)" ]]

export ARTIFACT_DIR="$artifact_dir"
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
cmake --build --preset ci-debug --parallel "${BUILD_JOBS:-6}"

make -C go clean
make -C go all tool PREFIX=/usr STRIP=true CHECK_GPU=0

mkdir -p "$artifact_dir/packages/rpm" "$artifact_dir/cpack-backend"
(cd cmake-build-ci-debug && cpack -G RPM -B "$artifact_dir/cpack-backend")
mapfile -t backend_rpms < <(find "$artifact_dir/cpack-backend" -maxdepth 1 -type f -name '*.rpm' -print)
[[ "${#backend_rpms[@]}" == 2 ]]
cp "${backend_rpms[@]}" "$artifact_dir/packages/rpm/"

GITHUB_TOKEN="$CI_PACKAGING_TOKEN" make -C go package SKIP_REBUILD=true CHECK_GPU=0 \
  GORELEASER_SNAPSHOT=false GORELEASER_RELEASE_ARGS=--skip=publish,validate
cp go/build/dist/*.rpm "$artifact_dir/packages/rpm/"

expected_version="$(< VERSION)"
expected_names=(cranesched-cranectld cranesched-craned cranesched-client cranesched-plugin cranesched-slurm-compat)
mapfile -t packages < <(find "$artifact_dir/packages/rpm" -maxdepth 1 -type f -name '*.rpm' -print | sort)
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

printf 'rpm_dir=%s\n' "$artifact_dir/packages/rpm" >> "$GITHUB_OUTPUT"
