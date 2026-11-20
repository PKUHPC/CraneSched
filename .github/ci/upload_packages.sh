#!/usr/bin/env bash
set -euo pipefail
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy

: "${NEXUS_URL:?NEXUS_URL is required}"
: "${NEXUS_USERNAME:?NEXUS_USERNAME is required}"
: "${NEXUS_PASSWORD:?NEXUS_PASSWORD is required}"
: "${PACKAGE_DIR:?PACKAGE_DIR is required}"
: "${VERSION:?VERSION is required}"
: "${RPM_REPO_NAME:?RPM_REPO_NAME is required}"
: "${RPM_DISTRO:?RPM_DISTRO is required}"
: "${RPM_ARCH:?RPM_ARCH is required}"

[[ -d "$PACKAGE_DIR" ]]

max_retries=3
retry_delay=5

mapfile -t packages < <(find "$PACKAGE_DIR" -maxdepth 1 -type f -name '*.rpm' -print | sort)
[[ ${#packages[@]} -gt 0 ]]

upload_failed=0
for package in "${packages[@]}"; do
  relative_path=${package#"$PACKAGE_DIR"/}
  url="${NEXUS_URL%/}/repository/$RPM_REPO_NAME/$VERSION/$RPM_DISTRO/$RPM_ARCH/RPMS/$relative_path"

  for ((attempt = 1; attempt <= max_retries; attempt++)); do
    if curl --connect-timeout 15 --max-time 300 --silent --show-error --fail \
      --user "$NEXUS_USERNAME:$NEXUS_PASSWORD" --upload-file "$package" "$url"; then
      printf 'Uploaded %s\n' "${package##*/}"
      break
    fi

    if (( attempt == max_retries )); then
      printf 'Failed to upload %s after %d attempts\n' "${package##*/}" "$attempt" >&2
      upload_failed=1
      break
    fi

    printf 'Upload failed for %s; retrying (%d/%d)\n' "${package##*/}" "$((attempt + 1))" "$max_retries" >&2
    sleep "$retry_delay"
  done
done

[[ "$upload_failed" == 0 ]]
