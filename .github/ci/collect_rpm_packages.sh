#!/usr/bin/env bash
set -euo pipefail

artifact_dir=${ARTIFACT_DIR:?ARTIFACT_DIR is required}
source_dir="$artifact_dir/cpack-backend"
target_dir="$artifact_dir/packages/rpm"
mkdir -p "$target_dir"
mapfile -t backend_rpms < <(
  find "$source_dir" -maxdepth 1 -type f -name '*.rpm' -print
)
[[ "${#backend_rpms[@]}" == 2 ]]
cp "${backend_rpms[@]}" "$target_dir/"
