#!/usr/bin/env python3
"""Copy only an allowlisted, redacted subset of a CraneTestKit NFS run."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import stat
from itertools import islice
from pathlib import Path


MAX_LOG_BYTES = 4 * 1024 * 1024
MAX_TOTAL_LOG_BYTES = 128 * 1024 * 1024
MAX_METADATA_BYTES = 8 * 1024 * 1024
MAX_METADATA_FILES = 512
MAX_LOG_FILES = 2048
MAX_LOG_SCAN = 8192
SAFE_LOG_NAME = re.compile(r"^[A-Za-z0-9_.-]+\.log$")
DENIED_NAME = re.compile(
    r"(?:env|context|secret|token|credential|kubeconfig)", re.IGNORECASE
)
LOG_RELATIVE = re.compile(
    r"^[A-Za-z0-9_.-]+/[0-9]+/(?:[A-Za-z0-9_.-]+/)*[A-Za-z0-9_.-]+\.log$"
)
REDACTIONS = (
    re.compile(r"github_pat_[A-Za-z0-9_]+"),
    re.compile(r"gh[pousr]_[A-Za-z0-9]+"),
    re.compile(r"\b(?:hvs\.[A-Za-z0-9_-]+|s\.[A-Za-z0-9]{16,})\b"),
    re.compile(r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b"),
    re.compile(r"(?i)(authorization\s*:\s*(?:bearer|basic)\s+)[^\s]+"),
    re.compile(
        r"(?i)((?:password|passwd|token|secret|cookie|private[_-]?key|client[_-]?secret)"
        r"\s*[:=]\s*)[^\s,;]+"
    ),
    re.compile(r"(?i)(https?://)[^/@\s:]+:[^/@\s]+@"),
    re.compile(
        r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----", re.S
    ),
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _safe_source(root: Path, relative: Path) -> Path | None:
    source = root / relative
    try:
        metadata = source.lstat()
    except OSError:
        return None
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        return None
    current = source.parent
    while current != root:
        try:
            if current.is_symlink():
                return None
        except OSError:
            return None
        current = current.parent
    try:
        source.resolve(strict=True).relative_to(root.resolve(strict=True))
    except (OSError, ValueError):
        return None
    return source


def _redact(value: str) -> tuple[str, int]:
    count = 0
    for pattern in REDACTIONS:
        value, replacements = pattern.subn(
            lambda match: match.group(1) + "[REDACTED]"
            if match.lastindex
            else "[REDACTED]",
            value,
        )
        count += replacements
    return value, count


def _copy_text(
    source: Path,
    destination: Path,
    *,
    tail_bytes: int | None = None,
) -> dict[str, object]:
    source_bytes = source.stat().st_size
    limit = tail_bytes if tail_bytes is not None else MAX_METADATA_BYTES
    truncated = source_bytes > limit
    with source.open("rb") as stream:
        if truncated and tail_bytes is not None:
            stream.seek(source_bytes - limit)
        raw = stream.read(limit)
    text = raw.decode("utf-8", errors="replace")
    redacted, count = _redact(text)
    encoded = redacted.encode("utf-8")
    if len(encoded) > limit:
        redacted = encoded[:limit].decode("utf-8", errors="ignore")
        truncated = True
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(redacted, encoding="utf-8")
    return {
        "path": str(destination),
        "redactions": count,
        "sha256": _sha256(destination),
        "source_bytes": source_bytes,
        "truncated": truncated,
    }


def _escape_markdown(value: object) -> str:
    text, _ = _redact(str(value)[:4096])
    return (
        text[:512]
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("`", "&#96;")
        .replace("|", "\\|")
        .replace("\r", " ")
        .replace("\n", " ")
    )


def _read_json(path: Path) -> dict[str, object]:
    try:
        if path.stat().st_size > MAX_METADATA_BYTES:
            return {}
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _write_summary(destination: Path, run_id: str, run_root: Path) -> None:
    result = _read_json(run_root / "result.json")
    state = _read_json(run_root / "state.json")
    plan = _read_json(run_root / "plan/manifest.json")
    sources = result.get("sources") or plan.get("sources") or {}
    if not isinstance(sources, dict):
        sources = {}
    rows = (
        ("Run ID", run_id),
        ("Phase", state.get("phase", "not-created")),
        ("Exit code", result.get("exit_code", state.get("exit_code", "unavailable"))),
        ("Total", result.get("total", "unavailable")),
        ("Passed", result.get("passed", "unavailable")),
        ("Failed", result.get("failed", "unavailable")),
        ("Error", result.get("error", "unavailable")),
        ("Backend SHA", sources.get("backend", "unavailable")),
        ("Frontend SHA", sources.get("frontend", "unavailable")),
        ("AutoTest SHA", sources.get("autotest", "unavailable")),
        ("Build ID", result.get("build_id", plan.get("build_id", "unavailable"))),
        ("Started", result.get("started_at", "unavailable")),
        ("Finished", result.get("finished_at", "unavailable")),
        ("Phase durations", result.get("phase_durations_seconds", "unavailable")),
        (
            "Plan digest",
            result.get("plan_digest", plan.get("plan_digest", "unavailable")),
        ),
        ("Image", result.get("image_digest", plan.get("image_digest", "unavailable"))),
    )
    lines = [
        "## CraneTestKit K3s system test",
        "",
        "| Field | Value |",
        "| --- | --- |",
    ]
    lines.extend(
        f"| {_escape_markdown(key)} | `{_escape_markdown(value)}` |"
        for key, value in rows
    )
    destination.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--include-logs", action="store_true")
    args = parser.parse_args()

    destination = args.destination.resolve()
    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir(parents=True)
    entries: list[dict[str, object]] = []

    run_root = args.run_root
    if run_root.exists() and not run_root.is_symlink() and run_root.is_dir():
        allowlisted = [
            Path("plan/manifest.json"),
            Path("plan/allocation.json"),
            Path("plan/jobs.yaml"),
            Path("state.json"),
            Path("result.json"),
            Path("input/build-manifest.json"),
            Path("input/packages.json"),
            Path("input/images-runtime.json"),
        ]
        plan_dir = run_root / "plan"
        if plan_dir.is_dir() and not plan_dir.is_symlink():
            allowlisted.extend(
                path.relative_to(run_root)
                for path in islice(plan_dir.glob("shard-*.json"), MAX_METADATA_FILES)
            )
        results_dir = run_root / "results"
        if results_dir.is_dir() and not results_dir.is_symlink():
            allowlisted.extend(
                path.relative_to(run_root)
                for path in islice(
                    results_dir.rglob("shard-*.json"), MAX_METADATA_FILES
                )
            )
        for relative in sorted(set(allowlisted), key=str)[:MAX_METADATA_FILES]:
            source = _safe_source(run_root, relative)
            if source is None:
                continue
            entry = _copy_text(source, destination / relative)
            entry["path"] = str(relative)
            entries.append(entry)

        if args.include_logs:
            total = 0
            log_files = 0
            logs_root = run_root / "logs"
            if logs_root.is_dir() and not logs_root.is_symlink():
                for source in sorted(islice(logs_root.rglob("*.log"), MAX_LOG_SCAN)):
                    relative_under_logs = source.relative_to(logs_root)
                    if (
                        total >= MAX_TOTAL_LOG_BYTES
                        or log_files >= MAX_LOG_FILES
                        or not SAFE_LOG_NAME.fullmatch(source.name)
                        or DENIED_NAME.search(str(relative_under_logs))
                        or not LOG_RELATIVE.fullmatch(str(relative_under_logs))
                    ):
                        continue
                    safe = _safe_source(run_root, Path("logs") / relative_under_logs)
                    if safe is None:
                        continue
                    entry = _copy_text(
                        safe,
                        destination / "logs" / relative_under_logs,
                        tail_bytes=MAX_LOG_BYTES,
                    )
                    entry["path"] = str(Path("logs") / relative_under_logs)
                    entries.append(entry)
                    total += min(safe.stat().st_size, MAX_LOG_BYTES)
                    log_files += 1

    _write_summary(destination / "summary.md", args.run_id, run_root)
    manifest = {
        "apiVersion": "cranesched.io/v1alpha1",
        "kind": "CraneTestKitCiArtifactSet",
        "run_id": args.run_id,
        "run_present": run_root.is_dir() and not run_root.is_symlink(),
        "failure_logs_included": args.include_logs,
        "files": entries,
    }
    (destination / "artifact-manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
