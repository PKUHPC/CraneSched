#!/usr/bin/env bash
set -euo pipefail
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy
[[ "$CI_DEV_IMAGE" =~ @sha256:[a-f0-9]{64}$ ]]
[[ "$(id -u)" == 1001 && "$(id -g)" == 1001 ]]
getent passwd 1001 >/dev/null
getent group 1001 >/dev/null
source_sha="$CI_SOURCE_SHA"
source_checkout="$PWD"
ci_scripts="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
state="/ci/workspaces/native-rhel10-amd64/${CI_DEV_IMAGE##*@sha256:}/ci-debug"
artifact_dir="/ci/attempts/${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}/build"
mkdir -p "$state" "$(dirname "$artifact_dir")"
mkdir "$artifact_dir"
exec 9>"$state/build.lock"
flock 9
if [[ ! -d "$state/source/.git" ]]; then
  git init "$state/source"
fi
git -C "$state/source" config remote.origin.url "$(git -C "$source_checkout" remote get-url origin)"
git -C "$state/source" fetch --no-tags --update-shallow "$source_checkout" "$source_sha"
git -C "$state/source" checkout --force --detach "$source_sha"
cd -P "$state/source"
[[ "$(git rev-parse HEAD)" == "$source_sha" ]]
[[ -z "$(git status --porcelain --untracked-files=no)" ]]
export ARTIFACT_DIR="$artifact_dir"
export SOURCE_DATE_EPOCH="$(git log -1 --format=%ct)"
export CPM_SOURCE_CACHE=/ci/cache/cpm
mkdir -p "$CPM_SOURCE_CACHE" "$CCACHE_DIR" "$CCACHE_TEMPDIR"
# Check Go sources before tests or packaging.
unformatted="$(gofmt -l go)"
if [[ -n "$unformatted" ]]; then
  printf '%s\n' "$unformatted"
  echo '::error::Run gofmt on the listed files.'
  exit 1
fi
make -C go protos
(cd go && go vet -mod=readonly ./...)
cmake --preset ci-debug
cmake --build --preset ci-debug --target crane_unit_tests --parallel "${BUILD_JOBS:-6}"
ctest --test-dir cmake-build-ci-debug --output-on-failure
make -C go test
cmake --build --preset ci-debug --parallel "${BUILD_JOBS:-6}"
# Package inputs must all come from this source revision.
bash "$ci_scripts/prepare-go-package-inputs.sh"
make -C go all tool PREFIX=/usr STRIP=true CHECK_GPU=0
mkdir -p "$artifact_dir/packages/rpm" "$artifact_dir/cpack-backend"
(cd cmake-build-ci-debug && cpack -G RPM -B "$artifact_dir/cpack-backend")
ARTIFACT_DIR="$artifact_dir" bash "$ci_scripts/collect-rpm-packages.sh"
GITHUB_TOKEN="$CI_PACKAGING_TOKEN" make -C go package SKIP_REBUILD=true CHECK_GPU=0 \
  GORELEASER_SNAPSHOT=false GORELEASER_RELEASE_ARGS=--skip=publish,validate
cp go/build/dist/*.rpm "$artifact_dir/packages/rpm/"
python3 - <<'PYRPM'
import os
import subprocess
from pathlib import Path
root = Path(os.environ['ARTIFACT_DIR'])
expected = {'cranesched-cranectld', 'cranesched-craned', 'cranesched-client',
            'cranesched-plugin', 'cranesched-slurm-compat'}
packages = []
for path in sorted((root / 'packages/rpm').glob('*.rpm')):
    packages.append(subprocess.check_output(
        ['rpm', '-qp', '--qf', '%{NAME}\t%{VERSION}\t%{ARCH}', str(path)], text=True).split('\t'))
if len(packages) != 5 or {item[0] for item in packages} != expected:
    raise SystemExit('The expected five RPMs were not produced')
if {item[1] for item in packages} != {Path('VERSION').read_text().strip()} or {item[2] for item in packages} != {'x86_64'}:
    raise SystemExit('RPM version or architecture does not match this build')
with Path(os.environ['GITHUB_OUTPUT']).open('a') as output:
    output.write(f'rpm_dir={root / "packages/rpm"}\n')
PYRPM
