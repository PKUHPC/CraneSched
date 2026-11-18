#!/bin/bash

# =============================================================================
# Nexus Repository Upload Script
# Restored from CraneSched-AutoTest 2c1157a9 (formerly github-runner/upload.sh).
# -----------------------------------------------------------------------------
# Uploads DEB and RPM packages to Nexus Repository Manager with robust
# retry logic and clear logging.
# =============================================================================

# ------------------------------ Configuration --------------------------------

NEXUS_URL="${NEXUS_URL:-https://nexus.example.com}"
NEXUS_USERNAME="${NEXUS_USERNAME:-admin}"
NEXUS_PASSWORD="${NEXUS_PASSWORD:-}"

DEB_REPO_NAME="${DEB_REPO_NAME:-debian-hosted}"
RPM_REPO_NAME="${RPM_REPO_NAME:-rpm-hosted}"

PACKAGE_DIR="${PACKAGE_DIR:-./packages}"
DEB_PATTERN="${DEB_PATTERN:-*.deb}"
RPM_PATTERN="${RPM_PATTERN:-*.rpm}"

RPM_UPLOAD_PATH="${RPM_UPLOAD_PATH:-}"
if [[ -z "$RPM_UPLOAD_PATH" ]]; then
    RPM_UPLOAD_PATH='{version}/{distro}/{arch}/RPMS/{relpath}'
fi

UPLOAD_MODE="${UPLOAD_MODE:-}"

VERSION="${VERSION:-master}"
DEB_ARCH="${DEB_ARCH:-amd64}"
RPM_ARCH="${RPM_ARCH:-x86_64}"
DEB_DISTRO="${DEB_DISTRO:-}"
RPM_DISTRO="${RPM_DISTRO:-9}"

DRY_RUN="${DRY_RUN:-false}"
VERBOSE="${VERBOSE:-false}"
CONTINUE_ON_ERROR="${CONTINUE_ON_ERROR:-false}"
FAIL_FAST="${FAIL_FAST:-true}"
MASK_SENSITIVE="${MASK_SENSITIVE:-true}"

MAX_RETRIES="${MAX_RETRIES:-3}"
RETRY_DELAY="${RETRY_DELAY:-5}"


# ------------------------------ Color Output ---------------------------------

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# ------------------------------- Statistics ----------------------------------

TOTAL_PACKAGES=0
SUCCESSFUL_UPLOADS=0
FAILED_UPLOADS=0

# ------------------------------ Helper Methods -------------------------------

trim_trailing_slash() {
    local value="$1"
    [[ "$value" == "/" ]] && { echo "/"; return; }
    value="${value%/}"
    [[ -z "$value" ]] && value="/"
    echo "$value"
}

log_info()    { echo -e "${BLUE}[INFO]${NC} $*"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $*"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $*"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $*"; }
log_verbose() { [[ "$VERBOSE" == "true" ]] && echo -e "${BLUE}[DEBUG]${NC} $*"; }

mask_url() {
    local url="$1"
    [[ "$MASK_SENSITIVE" != "true" ]] && { echo "$url"; return; }

    if [[ "$url" =~ ^(https?://)([^@]+@)?([^/]+)(/.*)?$ ]]; then
        local protocol="${BASH_REMATCH[1]}"
        local auth="${BASH_REMATCH[2]}"
        local host="${BASH_REMATCH[3]}"
        local path="${BASH_REMATCH[4]}"

        [[ -n "$auth" ]] && auth="***:***@"
        echo "${protocol}${auth}***${path}"
    else
        echo "$url"
    fi
}

relative_path() {
    local package_file="$1"
    local base="${PACKAGE_DIR%/}"
    local rel="${package_file#${base}/}"
    rel="${rel#./}"
    echo "$rel"
}

collect_packages() {
    local pattern="$1"
    local -n result_ref="$2"
    result_ref=()

    while IFS= read -r -d '' file; do
        result_ref+=("$file")
    done < <(find "$PACKAGE_DIR" -type f -name "$pattern" -print0 2>/dev/null)
}

check_dependencies() {
    local deps=(curl find)
    local missing=()
    for dep in "${deps[@]}"; do
        command -v "$dep" &>/dev/null || missing+=("$dep")
    done

    if [[ ${#missing[@]} -gt 0 ]]; then
        log_error "Missing required dependencies: ${missing[*]}"
        exit 1
    fi
}

validate_config() {
    local errors=0

    if [[ -z "$NEXUS_PASSWORD" ]]; then
        log_error "Nexus password is required (set the NEXUS_PASSWORD environment variable)"
        ((errors++))
    fi

    if [[ ! -d "$PACKAGE_DIR" ]]; then
        log_error "Package directory does not exist: $PACKAGE_DIR"
        ((errors++))
    fi

    if [[ -n "$UPLOAD_MODE" ]]; then
        local normalized_mode="${UPLOAD_MODE^^}"
        if [[ "$normalized_mode" != "RPM" && "$normalized_mode" != "DEB" ]]; then
            log_error "UPLOAD_MODE must be either 'RPM' or 'DEB' when set"
            ((errors++))
        fi
    fi

    if [[ $errors -gt 0 ]]; then
        log_error "Configuration validation failed"
        exit 1
    fi
}

build_repo_url() {
    local repo_name="$1"
    echo "${NEXUS_URL}/repository/${repo_name}"
}

build_deb_target_url() {
    local repo_root="$(build_repo_url "$DEB_REPO_NAME")/"
    local distro_value="$DEB_DISTRO"
    local query_params=()

    [[ -n "$distro_value" ]] && query_params+=("dist=${distro_value}")

    if [[ ${#query_params[@]} -gt 0 ]]; then
        local IFS='&'
        echo "${repo_root}?${query_params[*]}"
    else
        echo "$repo_root"
    fi
}

generate_upload_url() {
    local repo_url="$1"
    local path_template="$2"
    local package_file="$3"
    local package_type="$4"
    local distro="$5"

    local relpath="$(relative_path "$package_file")"
    local filename="$(basename "$package_file")"

    local arch_for_url="$RPM_ARCH"
    if [[ "$package_type" == "DEB" ]]; then
        arch_for_url="$DEB_ARCH"
    fi

    local upload_path="$path_template"
    upload_path="${upload_path//\{version\}/$VERSION}"
    upload_path="${upload_path//\{arch\}/$arch_for_url}"
    upload_path="${upload_path//\{distro\}/$distro}"
    upload_path="${upload_path//\{filename\}/$filename}"
    upload_path="${upload_path//\{relpath\}/$relpath}"

    upload_path="${upload_path#/}"
    echo "${repo_url%/}/${upload_path}"
}

execute_upload() {
    local package_type="$1"
    local relpath="$2"
    local target_url="$3"
    local http_method="$4"
    shift 4
    local -a transport_opts=("$@")

    log_info "Uploading ${package_type} package: $relpath"
    log_verbose "Target URL: $(mask_url "$target_url")"

    if [[ "$DRY_RUN" == "true" ]]; then
        log_warning "DRY RUN: Would upload $relpath to $(mask_url "$target_url")"
        return 0
    fi

    local -a base_curl=("curl" "--connect-timeout" "15" "--max-time" "300" "-sS" "-w" "\n%{http_code}" "-u" "${NEXUS_USERNAME}:${NEXUS_PASSWORD}")
    [[ "$FAIL_FAST" == "true" ]] && base_curl+=("-f")
    [[ -n "$http_method" ]] && base_curl+=("-X" "$http_method")

    local attempt=1
    while (( attempt <= MAX_RETRIES )); do
        local -a cmd=("${base_curl[@]}" "${transport_opts[@]}" "$target_url")
        local response
        response=$("${cmd[@]}" 2>&1)
        local exit_code=$?

        if [[ $exit_code -eq 0 ]]; then
            local http_code="$(echo "$response" | tail -n1)"
            local body="$(echo "$response" | sed '$d')"
            if [[ "$http_code" =~ ^20[0-4]$ ]]; then
                log_success "Successfully uploaded: $relpath"
                log_verbose "HTTP $http_code: $body"
                return 0
            fi

            log_warning "Upload returned HTTP $http_code: $relpath"
            log_verbose "Response: $body"
        else
            log_warning "Upload failed with exit code $exit_code: $relpath"
            log_verbose "curl output: $response"
        fi

        if (( attempt < MAX_RETRIES )); then
            log_info "Retrying in ${RETRY_DELAY}s..."
            sleep "$RETRY_DELAY"
        fi
        ((attempt++))
    done

    log_error "Failed to upload after $MAX_RETRIES attempts: $relpath"
    return 1
}

upload_deb_package() {
    local package_file="$1"
    local _repo_name="$2"
    local _distro="$3"
    local _unused_path="$4"
    local package_type="$5"

    local relpath="$(relative_path "$package_file")"
    local target_url="$(build_deb_target_url)"
    local -a transport_opts=("-H" "Content-Type: multipart/form-data" "--data-binary" "@${package_file}")

    execute_upload "$package_type" "$relpath" "$target_url" "POST" "${transport_opts[@]}"
}

upload_rpm_package() {
    local package_file="$1"
    local repo_name="$2"
    local distro="$3"
    local path_template="$4"
    local package_type="$5"

    local repo_url="$(build_repo_url "$repo_name")"
    local upload_url="$(generate_upload_url "$repo_url" "$path_template" "$package_file" "$package_type" "$distro")"
    local relpath="$(relative_path "$package_file")"
    local -a transport_opts=("--upload-file" "$package_file")

    execute_upload "$package_type" "$relpath" "$upload_url" "" "${transport_opts[@]}"
}

process_package_group() {
    local package_type="$1"
    local pattern="$2"
    local repo_name="$3"
    local distro="$4"
    local path_template="$5"
    local uploader_fn="$6"

    log_info "Searching for ${package_type} packages..."

    local packages=()
    collect_packages "$pattern" packages

    if [[ ${#packages[@]} -eq 0 ]]; then
        log_warning "No ${package_type} packages found in $PACKAGE_DIR"
        return
    fi

    log_info "Found ${#packages[@]} ${package_type} package(s)"

    for package in "${packages[@]}"; do
        ((TOTAL_PACKAGES++))
        if "$uploader_fn" "$package" "$repo_name" "$distro" "$path_template" "$package_type"; then
            ((SUCCESSFUL_UPLOADS++))
        else
            ((FAILED_UPLOADS++))
            if [[ "$CONTINUE_ON_ERROR" != "true" ]]; then
                log_error "Upload failed, exiting (set CONTINUE_ON_ERROR=true to continue on error)"
                exit 1
            fi
        fi
    done

}

print_usage() {
    cat << EOF
Usage: $0 [-h|--help]

Options:
    -h, --help              Show this help message

All other configuration is handled via environment variables:
    NEXUS_URL               Nexus server URL
    NEXUS_USERNAME          Nexus username
    NEXUS_PASSWORD          Nexus password
    DEB_REPO_NAME           DEB repository name
    RPM_REPO_NAME           RPM repository name
    PACKAGE_DIR             Package directory path
    VERSION                 Package version for upload path expansion
    DEB_ARCH                Architecture token for DEB uploads (default: amd64)
    RPM_ARCH                Architecture token for RPM uploads (default: x86_64)
    DEB_DISTRO              Distribution name for APT uploads (optional query param)
    RPM_DISTRO              Distribution label for RPM upload paths
    RPM_UPLOAD_PATH         RPM upload path template
    UPLOAD_MODE             Limit uploads to 'RPM' or 'DEB' (default: both)
    DRY_RUN                 Enable dry run mode (true/false)
    VERBOSE                 Enable verbose output (true/false)
    CONTINUE_ON_ERROR       Continue on upload error (true/false)
    FAIL_FAST               Add curl -f for uploads (true/false)
    MASK_SENSITIVE          Mask URLs in logs (true/false)
EOF
}

parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                print_usage; exit 0 ;;
            *)
                log_error "Unknown option: $1 (use environment variables for configuration)"
                print_usage
                exit 1
                ;;
        esac
    done
}

main() {
    parse_arguments "$@"

    NEXUS_URL="$(trim_trailing_slash "$NEXUS_URL")"
    PACKAGE_DIR="$(trim_trailing_slash "$PACKAGE_DIR")"

    log_info "Starting Nexus Repository Upload"
    log_info "=================================="

    check_dependencies
    validate_config

    local normalized_mode="${UPLOAD_MODE^^}"

    log_info "Configuration:"
    log_info "  Nexus URL: $(mask_url "$NEXUS_URL")"
    log_info "  DEB Repository: $DEB_REPO_NAME"
    log_info "  RPM Repository: $RPM_REPO_NAME"
    log_info "  Package Directory: $PACKAGE_DIR"
    log_info "  Version: $VERSION"
    log_info "  DEB Architecture: $DEB_ARCH"
    log_info "  RPM Architecture: $RPM_ARCH"
    log_info "  DEB Distribution: ${DEB_DISTRO:-<unset>}"
    log_info "  RPM Distribution: $RPM_DISTRO"
    log_info "  Upload Mode: ${normalized_mode:-Both}"
    log_info "  Dry Run: $DRY_RUN"
    echo ""

    case "$normalized_mode" in
        "DEB")
            process_package_group "DEB" "$DEB_PATTERN" "$DEB_REPO_NAME" "$DEB_DISTRO" "" "upload_deb_package"
            ;;
        "RPM")
            process_package_group "RPM" "$RPM_PATTERN" "$RPM_REPO_NAME" "$RPM_DISTRO" "$RPM_UPLOAD_PATH" "upload_rpm_package"
            ;;
        *)
            process_package_group "DEB" "$DEB_PATTERN" "$DEB_REPO_NAME" "$DEB_DISTRO" "" "upload_deb_package"
            echo ""
            process_package_group "RPM" "$RPM_PATTERN" "$RPM_REPO_NAME" "$RPM_DISTRO" "$RPM_UPLOAD_PATH" "upload_rpm_package"
            ;;
    esac

    echo ""
    log_info "Upload Summary"
    log_info "=============="
    log_info "Total packages: $TOTAL_PACKAGES"
    log_success "Successful uploads: $SUCCESSFUL_UPLOADS"

    if [[ $FAILED_UPLOADS -gt 0 ]]; then
        log_error "Failed uploads: $FAILED_UPLOADS"
        exit 1
    fi

    log_success "All packages processed successfully"
}

main "$@"
