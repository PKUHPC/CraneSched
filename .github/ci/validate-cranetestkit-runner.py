#!/usr/bin/env python3
"""Fail closed when the privileged CraneTestKit runner drifts from its release contract."""

from __future__ import annotations

import argparse
import base64
import binascii
import copy
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
from pathlib import Path
from typing import Callable, Iterable, Sequence


SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
GIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
BUNDLE_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$")
PINNED_IMAGE_RE = re.compile(r"^.+@sha256:[0-9a-f]{64}$")
PAT_RE = re.compile(r"(?:github_pat_|gh[pousr]_)[A-Za-z0-9_]+")
ADMISSION_POLICY_NAME = "crane-testkit-ci-jobs"
FINDMNT = Path("/usr/bin/findmnt")
OPENSSL = Path("/usr/bin/openssl")
PROBE_RENDERER = (
    Path(__file__).resolve().with_name("render-cranetestkit-admission-probe.py")
)
EXPECTED_CLIENT_CERT_CN = "system:serviceaccount:crane-testkit:crane-testkit-ci"
MIN_CLIENT_CERT_VALIDITY_SECONDS = 4 * 60 * 60


class ValidationError(RuntimeError):
    pass


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _run(
    command: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    label: str,
) -> str:
    result = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        check=False,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        raise ValidationError(f"{label} failed with exit code {result.returncode}")
    return result.stdout.strip()


def _require_absolute(path: Path, label: str) -> Path:
    if not path.is_absolute():
        raise ValidationError(f"{label} must be an absolute path")
    return path


def _require_regular(
    path: Path,
    label: str,
    *,
    root_owned: bool = False,
    private: bool = False,
    read_only: bool = False,
) -> os.stat_result:
    _require_absolute(path, label)
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ValidationError(f"cannot stat {label}: {exc.strerror}") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise ValidationError(f"{label} must be a regular non-symlink file")
    if root_owned and metadata.st_uid != 0:
        raise ValidationError(f"{label} must be owned by root")
    if metadata.st_mode & 0o022:
        raise ValidationError(f"{label} must not be group- or world-writable")
    if private and metadata.st_mode & 0o077:
        raise ValidationError(f"{label} must not grant group or other permissions")
    if read_only and metadata.st_mode & 0o222:
        raise ValidationError(f"{label} must be read-only")
    return metadata


def _require_digest(value: str, label: str) -> None:
    if not SHA256_RE.fullmatch(value):
        raise ValidationError(f"{label} must be a lowercase SHA-256 digest")


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValidationError(f"{label} must be a YAML mapping")
    return value


def _string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValidationError(f"{label} must be a non-empty string")
    return value


def _load_yaml(path: Path, label: str) -> dict[str, object]:
    try:
        import yaml
    except ImportError as exc:
        raise ValidationError("trusted system Python must provide PyYAML") from exc
    try:
        value = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ValidationError(f"cannot safely parse {label}") from exc
    return _mapping(value, label)


def _resolve_profile_reference(profile: Path, value: str) -> Path:
    candidate = Path(value)
    if candidate.is_absolute():
        return candidate
    direct = profile.parent / candidate
    if direct.exists():
        return direct.resolve()
    return (profile.parent.parent / candidate).resolve()


def _git(release: Path, arguments: Iterable[str], label: str) -> str:
    environment = os.environ.copy()
    environment["GIT_OPTIONAL_LOCKS"] = "0"
    return _run(
        ["git", "-c", f"safe.directory={release}", "-C", str(release), *arguments],
        env=environment,
        label=label,
    )


def _is_generated_python_bytecode(release: Path, relative: str) -> bool:
    path = Path(relative)
    if path.suffix != ".pyc" or "__pycache__" not in path.parts:
        return False
    try:
        return stat.S_ISREG((release / path).lstat().st_mode)
    except OSError:
        return False


def _validate_release(
    release: Path,
    expected_sha: str,
    profile: Path,
    expected_executable_sha256: str,
) -> Path:
    _require_absolute(release, "AutoTest release")
    try:
        metadata = release.lstat()
    except OSError as exc:
        raise ValidationError(f"cannot stat AutoTest release: {exc.strerror}") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise ValidationError("AutoTest release must be a non-symlink directory")
    if metadata.st_uid != 0 or metadata.st_mode & 0o222:
        raise ValidationError(
            "AutoTest release must be root-owned and read-only to the runner"
        )
    if not GIT_SHA_RE.fullmatch(expected_sha):
        raise ValidationError(
            "AutoTest release SHA must be 40 lowercase hexadecimal characters"
        )

    actual_sha = _git(release, ["rev-parse", "HEAD"], "read AutoTest release SHA")
    if actual_sha != expected_sha:
        raise ValidationError("AutoTest release SHA does not match the configured SHA")
    if _git(
        release,
        ["status", "--porcelain=v1", "--untracked-files=all"],
        "inspect AutoTest release worktree",
    ):
        raise ValidationError("AutoTest release worktree is dirty")

    ignored = _git(
        release,
        ["ls-files", "--others", "--ignored", "--exclude-standard", "-z"],
        "inspect ignored AutoTest release files",
    ).split("\0")
    unexpected_ignored = [
        relative
        for relative in filter(None, ignored)
        if relative != ".venv"
        and not relative.startswith(".venv/")
        and not _is_generated_python_bytecode(release, relative)
    ]
    if unexpected_ignored:
        raise ValidationError(
            "AutoTest release contains ignored files outside .venv: "
            + ", ".join(sorted(unexpected_ignored)[:5])
        )

    git_dir = release / ".git"
    if not git_dir.is_dir() or git_dir.is_symlink():
        raise ValidationError(
            "AutoTest release must contain a local non-symlink .git directory"
        )
    if git_dir.stat().st_uid != 0 or git_dir.stat().st_mode & 0o222:
        raise ValidationError(
            "AutoTest release .git directory must be root-owned and read-only"
        )

    for directory, directory_names, file_names in os.walk(release, followlinks=False):
        for name in [*directory_names, *file_names]:
            path = Path(directory) / name
            item_metadata = path.lstat()
            relative = path.relative_to(release).as_posix()
            if item_metadata.st_uid != 0:
                raise ValidationError(
                    f"AutoTest release entry is not root-owned: {path}"
                )
            if (
                not stat.S_ISLNK(item_metadata.st_mode)
                and item_metadata.st_mode & 0o222
                and not (
                    stat.S_ISDIR(item_metadata.st_mode) and path.name == "__pycache__"
                )
                and not _is_generated_python_bytecode(release, relative)
            ):
                raise ValidationError(
                    f"AutoTest release entry is writable by the runner: {path}"
                )

    tracked = _git(release, ["ls-files", "-z"], "list AutoTest release files").split(
        "\0"
    )
    directories: set[Path] = {release}
    for relative in filter(None, tracked):
        candidate = release / relative
        try:
            candidate.relative_to(release)
        except ValueError as exc:
            raise ValidationError(
                "AutoTest release contains a path outside its root"
            ) from exc
        file_metadata = candidate.lstat()
        if stat.S_ISLNK(file_metadata.st_mode):
            raise ValidationError(
                f"tracked AutoTest release file is a symlink: {relative}"
            )
        if file_metadata.st_uid != 0 or file_metadata.st_mode & 0o222:
            raise ValidationError(
                f"tracked AutoTest release file is writable by the runner: {relative}"
            )
        parent = candidate.parent
        while parent != release:
            directories.add(parent)
            parent = parent.parent
    for directory in directories:
        directory_metadata = directory.lstat()
        if directory_metadata.st_uid != 0 or directory_metadata.st_mode & 0o222:
            raise ValidationError(
                f"AutoTest release directory is writable by the runner: {directory}"
            )

    resolved_profile = profile.resolve(strict=True)
    try:
        profile_relative = resolved_profile.relative_to(release.resolve(strict=True))
    except ValueError as exc:
        raise ValidationError(
            "CraneTestKit profile must be inside the AutoTest release"
        ) from exc
    _require_regular(
        resolved_profile,
        "CraneTestKit profile",
        root_owned=True,
        read_only=True,
    )
    _git(
        release,
        ["ls-files", "--error-unmatch", str(profile_relative)],
        "verify tracked CraneTestKit profile",
    )

    executable = release / ".venv/bin/crane_testkit"
    _require_digest(expected_executable_sha256, "CraneTestKit executable checksum")
    _require_regular(
        executable,
        "CraneTestKit executable",
        root_owned=True,
        read_only=True,
    )
    if _sha256(executable) != expected_executable_sha256:
        raise ValidationError("CraneTestKit executable checksum does not match")
    if not os.access(executable, os.X_OK):
        raise ValidationError("CraneTestKit executable is not executable")

    for remote in filter(
        None, _git(release, ["remote"], "list Git remotes").splitlines()
    ):
        remote_urls = _git(
            release,
            ["remote", "get-url", "--all", remote],
            "inspect Git remote",
        )
        if PAT_RE.search(remote_urls) or re.search(
            r"^https?://[^/@]+@", remote_urls, re.MULTILINE
        ):
            raise ValidationError("AutoTest release Git remote embeds a credential")
    return executable


def _validate_frozen_environment(release: Path, executable: Path) -> Path:
    try:
        with executable.open("rb") as stream:
            first_line = stream.readline(4096).decode("utf-8").rstrip("\r\n")
    except (OSError, UnicodeDecodeError) as exc:
        raise ValidationError("cannot read CraneTestKit executable shebang") from exc
    if not first_line.startswith("#!"):
        raise ValidationError("CraneTestKit executable has no absolute venv shebang")
    interpreter = Path(first_line[2:])
    expected_bin = release / ".venv/bin"
    if (
        not interpreter.is_absolute()
        or interpreter.parent != expected_bin
        or interpreter.name not in {"python", "python3"}
    ):
        raise ValidationError(
            "CraneTestKit executable must use the validated release interpreter"
        )
    try:
        resolved_interpreter = interpreter.resolve(strict=True)
    except OSError as exc:
        raise ValidationError(
            "CraneTestKit release interpreter is unavailable"
        ) from exc
    _require_regular(
        resolved_interpreter,
        "CraneTestKit release interpreter",
        root_owned=True,
    )
    if not os.access(resolved_interpreter, os.X_OK):
        raise ValidationError("CraneTestKit release interpreter is not executable")

    pyvenv = release / ".venv/pyvenv.cfg"
    _require_regular(
        pyvenv,
        "CraneTestKit pyvenv.cfg",
        root_owned=True,
        read_only=True,
    )
    settings = {}
    try:
        for raw_line in pyvenv.read_text(encoding="utf-8").splitlines():
            if not raw_line.strip():
                continue
            key, separator, value = raw_line.partition("=")
            if not separator:
                raise ValidationError(
                    "CraneTestKit pyvenv.cfg contains an invalid line"
                )
            settings[key.strip().lower()] = value.strip()
    except OSError as exc:
        raise ValidationError("cannot read CraneTestKit pyvenv.cfg") from exc
    if settings.get("include-system-site-packages", "").lower() != "false":
        raise ValidationError("CraneTestKit venv must disable system site packages")

    site_packages = sorted((release / ".venv/lib").glob("python*/site-packages"))
    if len(site_packages) != 1:
        raise ValidationError(
            "CraneTestKit venv must contain exactly one site-packages tree"
        )
    site_root = site_packages[0]
    if site_root.is_symlink() or not site_root.is_dir():
        raise ValidationError(
            "CraneTestKit site-packages must be a non-symlink directory"
        )
    pth_files = {path.name: path for path in site_root.glob("*.pth")}
    expected_pth = {
        "_editable_impl_cranesched_autotest.pth",
        "_virtualenv.pth",
    }
    if set(pth_files) != expected_pth:
        raise ValidationError(
            "CraneTestKit venv contains an unexpected .pth startup file"
        )
    editable = pth_files["_editable_impl_cranesched_autotest.pth"]
    virtualenv = pth_files["_virtualenv.pth"]
    for path in (editable, virtualenv):
        _require_regular(
            path,
            f"CraneTestKit startup file {path.name}",
            root_owned=True,
            read_only=True,
        )
    if editable.read_text(encoding="utf-8").splitlines() != [str(release / "src")]:
        raise ValidationError(
            "CraneTestKit editable install points outside the release"
        )
    if virtualenv.read_text(encoding="utf-8").splitlines() != ["import _virtualenv"]:
        raise ValidationError("CraneTestKit venv contains executable .pth drift")
    _require_regular(
        site_root / "_virtualenv.py",
        "CraneTestKit virtualenv bootstrap",
        root_owned=True,
        read_only=True,
    )
    for customizer in ("sitecustomize.py", "usercustomize.py"):
        if (site_root / customizer).exists():
            raise ValidationError("CraneTestKit venv contains a startup customizer")
    return interpreter


def _validate_runtime(
    path: Path,
    expected_digest: str,
    expected_autotest_sha: str,
    expected_lock_digest: str,
    expected_repository: str,
) -> str:
    _require_digest(expected_digest, "image runtime manifest checksum")
    _require_regular(path, "image runtime manifest")
    if _sha256(path) != expected_digest:
        raise ValidationError("image runtime manifest checksum does not match")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValidationError("image runtime manifest is not valid JSON") from exc
    if value.get("apiVersion") != "cranesched.io/v1alpha1" or value.get("kind") != (
        "CraneTestKitImageRuntime"
    ):
        raise ValidationError("unsupported image runtime manifest")
    if value.get("sourceShas", {}).get("autotest") != expected_autotest_sha:
        raise ValidationError("image runtime manifest is for a different AutoTest SHA")
    if value.get("lockSha256") != expected_lock_digest:
        raise ValidationError("image runtime manifest is for a different image lock")
    image_data = value.get("autotest")
    image = image_data.get("image") if isinstance(image_data, dict) else None
    digest = image_data.get("digest") if isinstance(image_data, dict) else None
    if not isinstance(image, str) or not isinstance(digest, str):
        raise ValidationError("image runtime manifest has no immutable AutoTest image")
    pinned = image if "@" in image else f"{image}@{digest}"
    if not PINNED_IMAGE_RE.fullmatch(pinned):
        raise ValidationError("AutoTest image is not digest-qualified")
    unpinned = pinned.rsplit("@", 1)[0]
    last_slash = unpinned.rfind("/")
    last_colon = unpinned.rfind(":")
    repository = unpinned[:last_colon] if last_colon > last_slash else unpinned
    if repository != expected_repository:
        raise ValidationError(
            "image runtime manifest uses an unexpected AutoTest repository"
        )
    if pinned.rsplit("@", 1)[1] != digest:
        raise ValidationError("AutoTest image and digest fields disagree")
    return f"{repository}@{digest}"


def _validate_nfs_mount_identity(
    root: Path,
    *,
    expected_server: str,
    expected_export: Path,
    expected_relative: Path,
    findmnt_output: str,
) -> None:
    try:
        value = json.loads(findmnt_output)
        filesystems = value["filesystems"]
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        raise ValidationError(
            "findmnt returned an invalid NFS mount description"
        ) from exc
    if not isinstance(filesystems, list) or len(filesystems) != 1:
        raise ValidationError("NFS root must resolve to exactly one mounted filesystem")
    filesystem = filesystems[0]
    if not isinstance(filesystem, dict):
        raise ValidationError("findmnt returned an invalid filesystem entry")
    source = filesystem.get("source")
    fstype = filesystem.get("fstype")
    target_value = filesystem.get("target")
    if fstype not in {"nfs", "nfs4"}:
        raise ValidationError("configured NFS root is not mounted from NFS")
    expected_source = f"{expected_server}:{expected_export}"
    if source != expected_source:
        raise ValidationError(
            "NFS mount source does not match the profile server/export"
        )
    if not isinstance(target_value, str) or not target_value:
        raise ValidationError("NFS mount has no absolute target")
    target = Path(target_value)
    if not target.is_absolute() or target.is_symlink():
        raise ValidationError("NFS mount target must be an absolute non-symlink path")
    try:
        target = target.resolve(strict=True)
        relative = root.resolve(strict=True).relative_to(target)
    except (OSError, ValueError) as exc:
        raise ValidationError("NFS root is outside the mounted export") from exc
    if relative != expected_relative:
        raise ValidationError("NFS root does not match the profile rootRelative path")


def _render_approved_jobs(
    *,
    interpreter: Path,
    release: Path,
    profile: Path,
    image: str,
    namespace: str,
) -> list[dict[str, object]]:
    _require_regular(
        PROBE_RENDERER, "trusted admission probe renderer", root_owned=True
    )
    environment = {
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "PATH": "/usr/bin:/bin",
    }
    raw = _run(
        [
            str(interpreter),
            "-I",
            "-B",
            str(PROBE_RENDERER),
            "--release",
            str(release),
            "--profile",
            str(profile),
            "--image",
            image,
            "--namespace",
            namespace,
        ],
        cwd=release,
        env=environment,
        label="render canonical CraneTestKit admission probes",
    )
    try:
        value = json.loads(raw)
        jobs = value["jobs"]
        module_paths = value["module_paths"]
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        raise ValidationError(
            "CraneTestKit admission renderer returned invalid JSON"
        ) from exc
    if not isinstance(jobs, list) or not jobs:
        raise ValidationError("CraneTestKit admission renderer returned no Jobs")
    if not isinstance(module_paths, dict) or set(module_paths) != {
        "kubernetes",
        "profile",
    }:
        raise ValidationError(
            "CraneTestKit admission renderer omitted source provenance"
        )
    source_root = (release / "src").resolve(strict=True)
    for path_value in module_paths.values():
        if not isinstance(path_value, str):
            raise ValidationError(
                "CraneTestKit admission renderer returned an invalid source path"
            )
        try:
            Path(path_value).resolve(strict=True).relative_to(source_root)
        except (OSError, ValueError) as exc:
            raise ValidationError(
                "CraneTestKit admission renderer loaded code outside the release"
            ) from exc
    normalized = []
    for job in jobs:
        if not isinstance(job, dict):
            raise ValidationError(
                "CraneTestKit admission renderer returned a non-object Job"
            )
        metadata = job.get("metadata")
        if (
            job.get("apiVersion") != "batch/v1"
            or job.get("kind") != "Job"
            or not isinstance(metadata, dict)
            or metadata.get("namespace") != namespace
        ):
            raise ValidationError(
                "CraneTestKit admission renderer returned an invalid Job"
            )
        normalized.append(job)
    return normalized


def _admission_mutations(
    job: dict[str, object],
) -> tuple[tuple[str, dict[str, object], str], ...]:
    try:
        pod = job["spec"]["template"]["spec"]  # type: ignore[index]
        containers = pod["containers"]  # type: ignore[index]
        volumes = pod["volumes"]  # type: ignore[index]
    except (KeyError, TypeError) as exc:
        raise ValidationError(
            "canonical CraneTestKit Job has no mutable Pod contract"
        ) from exc
    if not isinstance(pod, dict) or not isinstance(containers, list) or not containers:
        raise ValidationError("canonical CraneTestKit Job has an invalid Pod contract")
    if not isinstance(volumes, list):
        raise ValidationError("canonical CraneTestKit Job has no volume contract")

    probes = []
    node_name = copy.deepcopy(job)
    node_name["spec"]["template"]["spec"]["nodeName"] = "wrl-forbidden"  # type: ignore[index]
    probes.append(
        (
            "nodeName",
            node_name,
            "AutoTest Pods must isolate host namespaces and disable API token mounts",
        )
    )

    lifecycle = copy.deepcopy(job)
    lifecycle["spec"]["template"]["spec"]["containers"][0]["lifecycle"] = {  # type: ignore[index]
        "postStart": {"exec": {"command": ["/bin/true"]}}
    }
    probes.append(
        (
            "lifecycle",
            lifecycle,
            "AutoTest main container lifecycle and probe hooks are forbidden",
        )
    )

    host_namespace = copy.deepcopy(job)
    host_namespace["spec"]["template"]["spec"]["hostNetwork"] = True  # type: ignore[index]
    probes.append(
        (
            "hostNetwork",
            host_namespace,
            "AutoTest Pods must isolate host namespaces and disable API token mounts",
        )
    )

    image = copy.deepcopy(job)
    image["spec"]["template"]["spec"]["containers"][0]["image"] = (  # type: ignore[index]
        "registry.invalid/crane-testkit@sha256:" + "0" * 64
    )
    probes.append(
        (
            "image",
            image,
            "Every workload image must use an administrator-approved immutable digest",
        )
    )

    nfs = copy.deepcopy(job)
    nfs_volumes = nfs["spec"]["template"]["spec"]["volumes"]  # type: ignore[index]
    changed = False
    for volume in nfs_volumes:
        if isinstance(volume, dict) and volume.get("name") == "run-input":
            nfs_data = volume.get("nfs")
            if isinstance(nfs_data, dict):
                nfs_data["path"] = "/forbidden/crane-testkit-input"
                changed = True
                break
    if not changed:
        raise ValidationError("canonical CraneTestKit Job has no run-input NFS volume")
    probes.append(
        (
            "NFS path",
            nfs,
            "Volumes must use only the run-scoped NFS prefix, Secret, and emptyDir",
        )
    )
    return tuple(probes)


AdmissionSubmit = Callable[[dict[str, object]], subprocess.CompletedProcess[str]]


def _validate_admission_boundary(
    jobs: Sequence[dict[str, object]], submit: AdmissionSubmit
) -> None:
    for job in jobs:
        result = submit(job)
        if result.returncode != 0:
            raise ValidationError(
                "administrator-approved CraneTestKit Job failed server-side dry-run"
            )
    for label, mutated, expected_message in _admission_mutations(jobs[0]):
        result = submit(mutated)
        if result.returncode == 0:
            raise ValidationError(
                f"ValidatingAdmissionPolicy accepted forbidden {label} mutation"
            )
        response = f"{result.stdout}\n{result.stderr}"
        if ADMISSION_POLICY_NAME not in response and expected_message not in response:
            raise ValidationError(
                f"forbidden {label} mutation was not rejected by the expected admission policy"
            )


def _server_dry_run(
    k3s: Path,
    env: dict[str, str],
    namespace: str,
    job: dict[str, object],
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            str(k3s),
            "kubectl",
            "create",
            "--filename=-",
            "--dry-run=server",
            "--validate=strict",
            "--output=name",
            "--request-timeout=30s",
            "--namespace",
            namespace,
        ],
        env=env,
        input=json.dumps(job, separators=(",", ":")),
        check=False,
        text=True,
        capture_output=True,
    )


def _can_i_command(k3s: Path, verb: str, resource: str, namespace: str) -> list[str]:
    base_resource, separator, subresource = resource.partition("/")
    command = [str(k3s), "kubectl", "auth", "can-i", verb, base_resource]
    if separator:
        if not subresource or "/" in subresource:
            raise ValidationError(f"invalid Kubernetes subresource: {resource}")
        command.append(f"--subresource={subresource}")
    if namespace:
        command.extend(["--namespace", namespace])
    return command


def _can_i(
    k3s: Path, env: dict[str, str], verb: str, resource: str, namespace: str
) -> bool:
    command = _can_i_command(k3s, verb, resource, namespace)
    result = subprocess.run(
        command,
        env=env,
        check=False,
        text=True,
        capture_output=True,
    )
    answer = result.stdout.strip()
    expected_return_code = {"yes": 0, "no": 1}.get(answer)
    if expected_return_code is None or result.returncode != expected_return_code:
        raise ValidationError(
            f"unexpected Kubernetes authorization answer for {verb} {resource}"
        )
    return answer == "yes"


def _kubeconfig_credential_material(
    user: dict[str, object],
    *,
    data_name: str,
    path_name: str,
    label: str,
    private: bool,
) -> bytes:
    data_value = user.get(data_name)
    path_value = user.get(path_name)
    if (data_value is None) == (path_value is None):
        raise ValidationError(
            f"CI kubeconfig must configure exactly one {label} source"
        )
    if data_value is not None:
        if not isinstance(data_value, str) or not data_value:
            raise ValidationError(f"CI kubeconfig {label} data is invalid")
        try:
            material = base64.b64decode(data_value, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ValidationError(
                f"CI kubeconfig {label} data is not valid base64"
            ) from exc
    else:
        if not isinstance(path_value, str) or not path_value:
            raise ValidationError(f"CI kubeconfig {label} path is invalid")
        path = Path(path_value)
        metadata = _require_regular(
            path,
            f"CI kubeconfig {label}",
            root_owned=True,
            private=private,
        )
        current = path.parent
        while current != current.parent:
            try:
                current_metadata = current.lstat()
            except OSError as exc:
                raise ValidationError(
                    f"cannot stat CI kubeconfig {label} path"
                ) from exc
            if (
                stat.S_ISLNK(current_metadata.st_mode)
                or not stat.S_ISDIR(current_metadata.st_mode)
                or current_metadata.st_uid != 0
                or current_metadata.st_mode & 0o022
            ):
                raise ValidationError(
                    f"CI kubeconfig {label} path has an unsafe parent directory"
                )
            current = current.parent
        if metadata.st_size > 128 * 1024:
            raise ValidationError(f"CI kubeconfig {label} file is unexpectedly large")
        try:
            material = path.read_bytes()
        except OSError as exc:
            raise ValidationError(f"cannot read CI kubeconfig {label}") from exc
    if not material or len(material) > 128 * 1024:
        raise ValidationError(f"CI kubeconfig {label} material has an invalid size")
    return material


def _openssl(arguments: Sequence[str], material: bytes, label: str) -> bytes:
    result = subprocess.run(
        [str(OPENSSL), *arguments],
        env={"HOME": "/nonexistent", "PATH": "/usr/bin:/bin"},
        input=material,
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        raise ValidationError(f"{label} failed with exit code {result.returncode}")
    return result.stdout


def _validate_client_certificate(certificate: bytes, private_key: bytes) -> None:
    _require_regular(OPENSSL, "trusted OpenSSL executable", root_owned=True)
    if not os.access(OPENSSL, os.X_OK):
        raise ValidationError("trusted OpenSSL executable is not executable")
    if b"-----BEGIN CERTIFICATE-----" not in certificate:
        raise ValidationError("CI kubeconfig client certificate is not PEM encoded")
    if not re.search(rb"-----BEGIN [A-Z ]*PRIVATE KEY-----", private_key):
        raise ValidationError("CI kubeconfig client private key is not PEM encoded")
    subject = (
        _openssl(
            ("x509", "-noout", "-subject", "-nameopt", "RFC2253"),
            certificate,
            "inspect CI client certificate subject",
        )
        .decode("utf-8", errors="replace")
        .strip()
    )
    match = re.search(r"(?:^|,)CN=([^,]+)(?:,|$)", subject.removeprefix("subject="))
    if match is None or match.group(1) != EXPECTED_CLIENT_CERT_CN:
        raise ValidationError("CI client certificate has an unexpected identity")
    _openssl(
        (
            "x509",
            "-noout",
            "-checkend",
            str(MIN_CLIENT_CERT_VALIDITY_SECONDS),
        ),
        certificate,
        "check CI client certificate validity",
    )
    certificate_public_key = _openssl(
        ("x509", "-pubkey", "-noout"),
        certificate,
        "read CI client certificate public key",
    )
    private_public_key = _openssl(
        ("pkey", "-pubout", "-passin", "pass:"),
        private_key,
        "derive CI client private-key public key",
    )
    if certificate_public_key != private_public_key:
        raise ValidationError("CI client certificate and private key do not match")


def _validate_kubeconfig_contract(
    config: dict[str, object], *, expected_server: str, namespace: str
) -> tuple[bytes, bytes]:
    try:
        clusters = config["clusters"]
        contexts = config["contexts"]
        users = config["users"]
        if (
            not isinstance(clusters, list)
            or len(clusters) != 1
            or not isinstance(contexts, list)
            or len(contexts) != 1
            or not isinstance(users, list)
            or len(users) != 1
        ):
            raise KeyError("minified kubeconfig cardinality")
        cluster = clusters[0]["cluster"]
        context = contexts[0]["context"]
        user = users[0]["user"]
    except (IndexError, KeyError, TypeError) as exc:
        raise ValidationError("CI kubeconfig has no usable current context") from exc
    if (
        not isinstance(cluster, dict)
        or not isinstance(context, dict)
        or not isinstance(user, dict)
    ):
        raise ValidationError("CI kubeconfig current context is malformed")
    if cluster.get("server") != expected_server:
        raise ValidationError(
            "CI kubeconfig targets an unexpected Kubernetes API server"
        )
    if cluster.get("insecure-skip-tls-verify") is True or "proxy-url" in cluster:
        raise ValidationError("CI kubeconfig weakens the Kubernetes transport boundary")
    ca_data = cluster.get("certificate-authority-data")
    if not isinstance(ca_data, str) or not ca_data:
        raise ValidationError("CI kubeconfig must embed its Kubernetes CA certificate")
    if context.get("namespace", "default") != namespace:
        raise ValidationError(
            "CI kubeconfig current context uses an unexpected namespace"
        )
    allowed_user_keys = {
        "client-certificate",
        "client-certificate-data",
        "client-key",
        "client-key-data",
    }
    if not set(user) <= allowed_user_keys:
        raise ValidationError(
            "CI kubeconfig must not use token, exec, auth-provider, or password credentials"
        )
    certificate = _kubeconfig_credential_material(
        user,
        data_name="client-certificate-data",
        path_name="client-certificate",
        label="client certificate",
        private=False,
    )
    private_key = _kubeconfig_credential_material(
        user,
        data_name="client-key-data",
        path_name="client-key",
        label="client private key",
        private=True,
    )
    return certificate, private_key


def _validate_kubernetes(
    k3s: Path,
    kubeconfig: Path,
    *,
    expected_k3s_sha256: str,
    expected_k3s_version: str,
    expected_server: str,
    namespace: str,
    approved_jobs: Sequence[dict[str, object]],
) -> None:
    _require_regular(k3s, "K3s client", root_owned=True)
    if not os.access(k3s, os.X_OK):
        raise ValidationError("K3s client is not executable")
    if _sha256(k3s) != expected_k3s_sha256:
        raise ValidationError(
            "K3s client checksum does not match the profile image lock"
        )
    version = _run(
        [str(k3s), "--version"], label="read K3s client version"
    ).splitlines()[0]
    if expected_k3s_version not in version:
        raise ValidationError("K3s client version does not match the profile")

    kubeconfig_metadata = _require_regular(kubeconfig, "CI kubeconfig", private=True)
    if kubeconfig_metadata.st_uid not in {0, os.getuid()} or not os.access(
        kubeconfig, os.R_OK
    ):
        raise ValidationError(
            "CI kubeconfig must be readable only by root or the runner user"
        )
    env = os.environ.copy()
    env["KUBECONFIG"] = str(kubeconfig)
    raw_config = _run(
        [
            str(k3s),
            "kubectl",
            "config",
            "view",
            "--minify",
            "--raw",
            "--output=json",
        ],
        env=env,
        label="inspect restricted kubeconfig",
    )
    try:
        config = json.loads(raw_config)
    except json.JSONDecodeError as exc:
        raise ValidationError("CI kubeconfig is not valid JSON") from exc
    if not isinstance(config, dict):
        raise ValidationError("CI kubeconfig is not a JSON object")
    certificate, private_key = _validate_kubeconfig_contract(
        config,
        expected_server=expected_server,
        namespace=namespace,
    )
    _validate_client_certificate(certificate, private_key)

    allowed = (
        ("create", "jobs.batch"),
        ("get", "jobs.batch"),
        ("list", "jobs.batch"),
        ("watch", "jobs.batch"),
        ("delete", "jobs.batch"),
        ("get", "pods"),
        ("list", "pods"),
        ("watch", "pods"),
        ("delete", "pods"),
        ("create", "secrets"),
        ("delete", "secrets"),
        ("create", "leases.coordination.k8s.io"),
        ("get", "leases.coordination.k8s.io"),
        ("update", "leases.coordination.k8s.io"),
        ("patch", "leases.coordination.k8s.io"),
        ("delete", "leases.coordination.k8s.io"),
    )
    denied = [
        (verb, "secrets", namespace)
        for verb in ("get", "list", "watch", "update", "patch")
    ]
    denied.extend((verb, "pods", namespace) for verb in ("create", "update", "patch"))
    denied.extend(
        (verb, subresource, namespace)
        for subresource, verbs in (
            ("pods/exec", ("create", "get")),
            ("pods/attach", ("create", "get")),
            ("pods/portforward", ("create", "get")),
            ("pods/log", ("get",)),
        )
        for verb in verbs
    )
    denied.extend(
        (verb, resource, namespace)
        for resource in (
            "serviceaccounts",
            "roles.rbac.authorization.k8s.io",
            "rolebindings.rbac.authorization.k8s.io",
        )
        for verb in ("get", "list", "watch", "create", "update", "patch", "delete")
    )
    denied.extend(
        (verb, resource, "")
        for resource in (
            "namespaces",
            "nodes",
            "clusterroles.rbac.authorization.k8s.io",
            "clusterrolebindings.rbac.authorization.k8s.io",
        )
        for verb in ("get", "list", "watch", "create", "update", "patch", "delete")
    )
    denied.extend(
        (
            ("update", "jobs.batch", namespace),
            ("patch", "jobs.batch", namespace),
            ("list", "leases.coordination.k8s.io", namespace),
            ("watch", "leases.coordination.k8s.io", namespace),
        )
    )
    for verb, resource in allowed:
        if not _can_i(k3s, env, verb, resource, namespace):
            raise ValidationError(
                f"CI identity is missing required permission: {verb} {resource}"
            )
    for verb, resource, resource_namespace in denied:
        if _can_i(k3s, env, verb, resource, resource_namespace):
            raise ValidationError(
                f"CI identity has forbidden permission: {verb} {resource}"
            )
    _validate_admission_boundary(
        approved_jobs,
        lambda job: _server_dry_run(k3s, env, namespace, job),
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--autotest-sha", required=True)
    parser.add_argument("--executable-sha256", required=True)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--profile-sha256", required=True)
    parser.add_argument("--bundle-id", required=True)
    parser.add_argument("--runtime-manifest", type=Path, required=True)
    parser.add_argument("--runtime-sha256", required=True)
    parser.add_argument("--k3s", type=Path, required=True)
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--nfs-root", type=Path, required=True)
    parser.add_argument("--namespace", default="crane-testkit")
    return parser


def main() -> int:
    args = _parser().parse_args()
    executable = _validate_release(
        args.release,
        args.autotest_sha,
        args.profile,
        args.executable_sha256,
    )
    interpreter = _validate_frozen_environment(args.release, executable)
    _require_digest(args.profile_sha256, "profile checksum")
    if _sha256(args.profile) != args.profile_sha256:
        raise ValidationError("profile checksum does not match")
    if not BUNDLE_RE.fullmatch(args.bundle_id):
        raise ValidationError("bundle ID must be a Kubernetes DNS label")

    profile_data = _load_yaml(args.profile, "CraneTestKit profile")
    if (
        profile_data.get("apiVersion") != "cranesched.io/v1alpha1"
        or profile_data.get("kind") != "CraneTestKitProfile"
    ):
        raise ValidationError("unsupported CraneTestKit profile")
    cluster = _mapping(profile_data.get("cluster"), "profile.cluster")
    storage = _mapping(profile_data.get("storage"), "profile.storage")
    nfs = _mapping(storage.get("nfs"), "profile.storage.nfs")
    images = _mapping(profile_data.get("images"), "profile.images")
    nfs_export = Path(_string(nfs.get("export"), "profile.storage.nfs.export"))
    nfs_server = _string(nfs.get("server"), "profile.storage.nfs.server")
    nfs_relative = Path(
        _string(nfs.get("rootRelative"), "profile.storage.nfs.rootRelative")
    )
    if (
        not nfs_export.is_absolute()
        or ".." in nfs_export.parts
        or nfs_relative.is_absolute()
        or str(nfs_relative) in {"", "."}
        or ".." in nfs_relative.parts
    ):
        raise ValidationError(
            "profile NFS path is not a safe absolute export plus relative root"
        )
    image_lock_path = _resolve_profile_reference(
        args.profile, _string(images.get("lockFile"), "profile.images.lockFile")
    )
    _require_regular(
        image_lock_path,
        "CraneTestKit image lock",
        root_owned=True,
        read_only=True,
    )
    try:
        image_lock_relative = image_lock_path.relative_to(
            args.release.resolve(strict=True)
        )
    except ValueError as exc:
        raise ValidationError(
            "CraneTestKit image lock must be inside the AutoTest release"
        ) from exc
    _git(
        args.release,
        ["ls-files", "--error-unmatch", str(image_lock_relative)],
        "verify tracked CraneTestKit image lock",
    )
    image_lock = _load_yaml(image_lock_path, "CraneTestKit image lock")
    if (
        image_lock.get("apiVersion") != "cranesched.io/v1alpha1"
        or image_lock.get("kind") != "CraneTestKitImageLock"
    ):
        raise ValidationError("unsupported CraneTestKit image lock")
    k3s_lock = _mapping(image_lock.get("k3s"), "image lock k3s")
    k3s_binary = _mapping(k3s_lock.get("binary"), "image lock k3s.binary")
    autotest_lock = _mapping(image_lock.get("autotest"), "image lock autotest")

    expected_nfs_root = (nfs_export / nfs_relative).resolve()
    _require_absolute(args.nfs_root, "NFS root")
    try:
        nfs_metadata = args.nfs_root.lstat()
    except OSError as exc:
        raise ValidationError("NFS root is unavailable") from exc
    if stat.S_ISLNK(nfs_metadata.st_mode) or not stat.S_ISDIR(nfs_metadata.st_mode):
        raise ValidationError("NFS root must be an existing non-symlink directory")
    supplied_nfs_root = args.nfs_root.resolve()
    if supplied_nfs_root != expected_nfs_root:
        raise ValidationError("configured NFS root does not match the profile")
    if not os.access(supplied_nfs_root, os.W_OK):
        raise ValidationError("NFS root is not writable by the runner")
    _require_regular(FINDMNT, "trusted findmnt executable", root_owned=True)
    if not os.access(FINDMNT, os.X_OK):
        raise ValidationError("trusted findmnt executable is not executable")
    findmnt_output = _run(
        [
            str(FINDMNT),
            "--json",
            "--target",
            str(supplied_nfs_root),
            "--output",
            "TARGET,SOURCE,FSTYPE",
        ],
        label="inspect NFS mount identity",
    )
    _validate_nfs_mount_identity(
        supplied_nfs_root,
        expected_server=nfs_server,
        expected_export=nfs_export,
        expected_relative=nfs_relative,
        findmnt_output=findmnt_output,
    )

    expected_runtime = supplied_nfs_root / "images" / args.bundle_id / "runtime.json"
    if args.runtime_manifest.absolute() != expected_runtime:
        raise ValidationError(
            "image runtime manifest path does not match the configured bundle"
        )
    current = supplied_nfs_root
    for component in ("images", args.bundle_id):
        current /= component
        try:
            current_metadata = current.lstat()
        except OSError as exc:
            raise ValidationError(
                "image runtime manifest directory is unavailable"
            ) from exc
        if stat.S_ISLNK(current_metadata.st_mode) or not stat.S_ISDIR(
            current_metadata.st_mode
        ):
            raise ValidationError(
                "image runtime manifest has a symlinked path component"
            )
    image = _validate_runtime(
        args.runtime_manifest,
        args.runtime_sha256,
        args.autotest_sha,
        _sha256(image_lock_path),
        _string(autotest_lock.get("repository"), "image lock autotest.repository"),
    )
    approved_jobs = _render_approved_jobs(
        interpreter=interpreter,
        release=args.release,
        profile=args.profile,
        image=image,
        namespace=args.namespace,
    )
    _validate_kubernetes(
        args.k3s,
        args.kubeconfig,
        expected_k3s_sha256=_string(
            k3s_binary.get("sha256"), "image lock k3s.binary.sha256"
        ),
        expected_k3s_version=_string(k3s_lock.get("version"), "image lock k3s.version"),
        expected_server=(
            f"https://{_string(cluster.get('apiAddress'), 'profile.cluster.apiAddress')}:6443"
        ),
        namespace=args.namespace,
        approved_jobs=approved_jobs,
    )
    print(
        json.dumps(
            {
                "autotest_sha": args.autotest_sha,
                "bundle_id": args.bundle_id,
                "crane_testkit": str(executable),
                "crane_testkit_sha256": args.executable_sha256,
                "image": image,
                "namespace": args.namespace,
                "profile_sha256": args.profile_sha256,
                "runtime_sha256": args.runtime_sha256,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValidationError as exc:
        print(f"runner validation failed: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
