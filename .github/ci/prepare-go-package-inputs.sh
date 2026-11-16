#!/usr/bin/env bash
set -euo pipefail

cd "${1:-.}"
rm -rf go/build/bin go/build/plugin go/build/tool
