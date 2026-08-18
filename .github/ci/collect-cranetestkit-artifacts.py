#!/usr/bin/env python3
"""Copy only an allowlisted, redacted subset of a CraneTestKit NFS run."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import shutil
import stat
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path


MAX_LOG_BYTES = 4 * 1024 * 1024
MAX_TOTAL_LOG_BYTES = 128 * 1024 * 1024
MAX_METADATA_BYTES = 8 * 1024 * 1024
MAX_METADATA_FILES = 512
MAX_CHECKPOINT_BYTES = 2 * 1024 * 1024
MAX_TOTAL_CHECKPOINT_BYTES = 16 * 1024 * 1024
MAX_LOG_FILES = 2048
MAX_LOG_SCAN = 8192
MAX_FAILURE_ROWS = 50
MAX_INFRASTRUCTURE_ROWS = 20
MAX_SLOW_CASE_ROWS = 10
MAX_SUMMARY_BYTES = 512 * 1024
SAFE_LOG_NAME = re.compile(r"^[A-Za-z0-9_.-]+\.log$")
DENIED_NAME = re.compile(
    r"(?:env|context|secret|token|credential|kubeconfig)", re.IGNORECASE
)
LOG_RELATIVE = re.compile(
    r"^[A-Za-z0-9_.-]+/[0-9]+/(?:[A-Za-z0-9_.-]+/)*[A-Za-z0-9_.-]+\.log$"
)
CASE_LEVEL_DIAGNOSTIC = re.compile(
    r"^(?:case was not run|missing case result):\s*\S+\s*$", re.IGNORECASE
)
EXECUTION_STAGE_MESSAGE = re.compile(
    r"^(?:cancelled|run/wait|collect(?: cancelled)?|stop(?: cancelled| retry)?|"
    r"lease(?: release)?):",
    re.IGNORECASE,
)
REDACTIONS = (
    re.compile(
        r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?"
        r"(?:-----END [A-Z ]*PRIVATE KEY-----|\Z)",
        re.S,
    ),
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
)
ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
UNSAFE_CONTROLS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
BIDI_CONTROLS = re.compile(r"[\u202a-\u202e\u2066-\u2069]")
FULL_SHA = re.compile(r"^[0-9a-f]{40}$")
FRONTEND_REPOSITORY = "PKUHPC/CraneSched-FrontEnd"
FRONTEND_SOURCES = (
    "matching_branch",
    "master_fallback",
    "master_default",
    "manual_ref",
)


def _full_sha(value: str) -> str:
    if FULL_SHA.fullmatch(value) is None:
        raise argparse.ArgumentTypeError("revision must be a full lowercase commit SHA")
    return value


def _frontend_ref(value: str) -> str:
    if (
        not value
        or len(value) > 1024
        or any(character in value for character in "\r\n")
    ):
        raise argparse.ArgumentTypeError("FrontEnd ref is invalid")
    return value


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
            lambda match: (
                match.group(1) + "[REDACTED]" if match.lastindex else "[REDACTED]"
            ),
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
    text = BIDI_CONTROLS.sub("", ANSI_ESCAPE.sub("", text))
    text = UNSAFE_CONTROLS.sub(" ", text)
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


def _reject_json_constant(constant: str) -> object:
    raise ValueError(f"invalid JSON constant: {constant}")


def _read_json(path: Path) -> dict[str, object]:
    try:
        if path.stat().st_size > MAX_METADATA_BYTES:
            return {}
        value = json.loads(
            path.read_text(encoding="utf-8"),
            parse_constant=_reject_json_constant,
        )
    except (OSError, UnicodeError, ValueError, RecursionError):
        return {}
    return value if isinstance(value, dict) else {}


def _read_run_json(run_root: Path, relative: Path) -> dict[str, object]:
    if not run_root.is_dir() or run_root.is_symlink():
        return {}
    source = _safe_source(run_root, relative)
    return _read_json(source) if source is not None else {}


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        number = float(value)
    except (OverflowError, ValueError):
        return None
    return number if math.isfinite(number) and number >= 0 else None


def _integer(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _format_duration(value: object) -> str:
    seconds = _number(value)
    if seconds is None:
        return "unavailable"
    scaled = seconds * 100
    if not math.isfinite(scaled):
        return "unavailable"
    centiseconds = int(round(scaled))
    hours, remainder = divmod(centiseconds, 360_000)
    minutes, remainder = divmod(remainder, 6_000)
    whole_seconds = remainder / 100
    if hours:
        return f"{hours}h {minutes:02d}m {whole_seconds:05.2f}s"
    if minutes:
        return f"{minutes}m {whole_seconds:05.2f}s"
    return f"{whole_seconds:.2f}s"


def _format_case_duration(value: object) -> str:
    seconds = _number(value)
    return f"{seconds:.2f}s" if seconds is not None else "unavailable"


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed


def _duration_between(started_at: object, finished_at: object) -> float | None:
    started = _timestamp(started_at)
    finished = _timestamp(finished_at)
    if started is None or finished is None:
        return None
    duration = (finished - started).total_seconds()
    return duration if math.isfinite(duration) and duration >= 0 else None


def _case_counts(cases: object) -> dict[str, int]:
    counts = {"passed": 0, "failed": 0, "error": 0, "not_run": 0}
    if not isinstance(cases, list):
        return counts
    for case in cases:
        if not isinstance(case, dict):
            continue
        status = case.get("status")
        if isinstance(status, str) and status in counts:
            counts[status] += 1
    return counts


def _allocation_by_shard(allocation: dict[str, object]) -> dict[int, tuple[str, str]]:
    result: dict[int, tuple[str, str]] = {}
    workers = allocation.get("workers")
    if not isinstance(workers, dict):
        return result
    for worker, shard_indices in workers.items():
        if not isinstance(worker, str) or not isinstance(shard_indices, list):
            continue
        for slot, shard_index in enumerate(shard_indices):
            if _integer(shard_index) is not None:
                result[shard_index] = (worker, str(slot))
    return result


def _planned_shards(plan: dict[str, object]) -> dict[int, dict[str, object]]:
    result: dict[int, dict[str, object]] = {}
    sharding = plan.get("sharding")
    if not isinstance(sharding, dict) or not isinstance(sharding.get("shards"), list):
        return result
    for shard in sharding["shards"]:
        if not isinstance(shard, dict):
            continue
        shard_index = _integer(shard.get("index"))
        if shard_index is not None:
            result[shard_index] = shard
    return result


def _shard_results(run_root: Path) -> list[tuple[Path, dict[str, object]]]:
    results_root = run_root / "results"
    if (
        not results_root.is_dir()
        or results_root.is_symlink()
        or not run_root.is_dir()
        or run_root.is_symlink()
    ):
        return []
    records: list[tuple[Path, dict[str, object]]] = []
    total_bytes = 0
    for path in sorted(islice(results_root.rglob("shard-*.json"), MAX_METADATA_FILES)):
        try:
            relative = path.relative_to(run_root)
        except ValueError:
            continue
        source = _safe_source(run_root, relative)
        if source is None:
            continue
        try:
            source_bytes = source.stat().st_size
        except OSError:
            continue
        if source_bytes > MAX_CHECKPOINT_BYTES:
            continue
        if total_bytes + source_bytes > MAX_TOTAL_CHECKPOINT_BYTES:
            break
        total_bytes += source_bytes
        value = _read_json(source)
        if value:
            records.append((relative, value))
    return records


def _shard_context(
    run_root: Path,
    plan: dict[str, object],
    allocation: dict[str, object],
) -> tuple[list[dict[str, object]], dict[str, tuple[int, str, str]], list[str]]:
    planned = _planned_shards(plan)
    allocated = _allocation_by_shard(allocation)
    records_by_index: dict[int, list[tuple[Path, dict[str, object]]]] = {}
    case_locations: dict[str, tuple[int, str, str]] = {}
    fatals: list[str] = []

    for relative, record in _shard_results(run_root):
        shard_index = _integer(record.get("shard_index"))
        if shard_index is None:
            continue
        records_by_index.setdefault(shard_index, []).append((relative, record))
        worker, slot = allocated.get(shard_index, ("unavailable", "unavailable"))
        if len(relative.parts) >= 4 and relative.parts[0] == "results":
            worker, slot = relative.parts[1], relative.parts[2]
        cases = record.get("cases")
        if isinstance(cases, list):
            for case in cases:
                if isinstance(case, dict) and isinstance(case.get("id"), str):
                    case_locations.setdefault(case["id"], (shard_index, worker, slot))
        fatal = record.get("fatal")
        if isinstance(fatal, dict):
            phase = fatal.get("phase", "unknown")
            message = fatal.get("message", "unknown fatal error")
            phase = phase if isinstance(phase, str) else "unknown"
            message = message if isinstance(message, str) else "unknown fatal error"
            fatals.append(f"shard {shard_index} fatal during {phase}: {message}")

    rows: list[dict[str, object]] = []
    for shard_index in sorted(set(planned) | set(records_by_index)):
        expected = planned.get(shard_index, {})
        expected_cases = expected.get("cases")
        planned_count = len(expected_cases) if isinstance(expected_cases, list) else 0
        estimate = expected.get("estimated_duration_seconds")
        records = records_by_index.get(shard_index, [])
        worker, slot = allocated.get(shard_index, ("unavailable", "unavailable"))
        if len(records) != 1:
            rows.append(
                {
                    "shard": shard_index,
                    "worker": worker,
                    "slot": slot,
                    "cases": planned_count,
                    "counts": _case_counts([]),
                    "wall": None,
                    "case_time": None,
                    "estimate": estimate,
                    "result": "MISSING"
                    if not records
                    else f"DUPLICATE ({len(records)})",
                    "terminal": False,
                }
            )
            continue

        relative, record = records[0]
        if len(relative.parts) >= 4 and relative.parts[0] == "results":
            worker, slot = relative.parts[1], relative.parts[2]
        cases = record.get("cases")
        counts = _case_counts(cases)
        case_time = None
        if isinstance(cases, list):
            case_time = sum(
                duration
                for case in cases
                if isinstance(case, dict)
                and (duration := _number(case.get("duration_seconds"))) is not None
            )
        fatal = record.get("fatal")
        exit_code = record.get("worker_exit_code")
        if not isinstance(exit_code, int) or isinstance(exit_code, bool):
            exit_code = None
        if isinstance(fatal, dict) or exit_code == 2:
            outcome = "INFRASTRUCTURE ERROR"
        elif counts["not_run"]:
            outcome = "INCOMPLETE"
        elif counts["failed"] or counts["error"] or exit_code == 1:
            outcome = "TEST FAILURE"
        elif exit_code == 0 and record.get("finished_at"):
            outcome = "PASSED"
        else:
            outcome = "INCOMPLETE"
        rows.append(
            {
                "shard": shard_index,
                "worker": worker,
                "slot": slot,
                "cases": len(cases) if isinstance(cases, list) else planned_count,
                "counts": counts,
                "wall": _duration_between(
                    record.get("started_at"), record.get("finished_at")
                ),
                "case_time": case_time,
                "estimate": estimate,
                "result": outcome,
                "terminal": exit_code in {0, 1, 2} and bool(record.get("finished_at")),
            }
        )
    return rows, case_locations, fatals


def _append_table(
    lines: list[str], headers: tuple[str, ...], rows: list[tuple[object, ...]]
) -> None:
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join("---" for _ in headers) + " |")
    lines.extend(
        "| " + " | ".join(f"`{_escape_markdown(value)}`" for value in row) + " |"
        for row in rows
    )


def _valid_exit_code(value: object) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool) and value in {0, 1, 2}:
        return value
    return None


def _resolve_exit_code(
    result: dict[str, object],
    state: dict[str, object],
    execute_exit_code: int | None,
) -> tuple[int | None, list[str]]:
    result_code = _valid_exit_code(result.get("exit_code"))
    state_code = _valid_exit_code(state.get("exit_code"))

    if execute_exit_code is not None:
        diagnostics: list[str] = []
        if result_code is None:
            diagnostics.append(
                f"Workflow exit code {execute_exit_code} was captured, but the aggregate "
                "result is missing or has no valid exit code"
            )
        mismatches: list[str] = []
        if result_code is not None and result_code != execute_exit_code:
            mismatches.append(f"aggregate result exit code {result_code}")
        if state_code is not None and state_code != execute_exit_code:
            mismatches.append(f"run state exit code {state_code}")
        if mismatches:
            diagnostics.append(
                f"Workflow exit code {execute_exit_code} disagrees with "
                + " and ".join(mismatches)
            )
        return (2, diagnostics) if diagnostics else (execute_exit_code, [])

    if result_code is not None and state_code is not None and result_code != state_code:
        return 2, [
            f"Aggregate result exit code {result_code} disagrees with run state exit code "
            f"{state_code}"
        ]
    if result_code is not None:
        return result_code, []

    infrastructure = result.get("infrastructure_errors")
    not_run = result.get("not_run_case_ids")
    if (isinstance(infrastructure, list) and infrastructure) or (
        isinstance(not_run, list) and not_run
    ):
        return 2, []
    if (_integer(result.get("failed")) or 0) or (_integer(result.get("error")) or 0):
        return 1, []
    if state_code is not None:
        if not result:
            return 2, [
                f"Run state exit code {state_code} exists, but the aggregate result is missing"
            ]
        return state_code, []
    return None, []


def _frontend_revision_diagnostics(
    run_root: Path,
    result: dict[str, object],
    plan: dict[str, object],
    authorized_sha: str,
) -> list[str]:
    if not run_root.is_dir() or run_root.is_symlink():
        return []
    diagnostics: list[str] = []
    for label, relative, document in (
        ("Aggregate result", Path("result.json"), result),
        ("Plan", Path("plan/manifest.json"), plan),
    ):
        if _safe_source(run_root, relative) is None:
            continue
        sources = document.get("sources")
        actual = sources.get("frontend") if isinstance(sources, dict) else None
        if actual is None:
            diagnostics.append(f"{label} FrontEnd SHA is missing")
        elif actual != authorized_sha:
            diagnostics.append(
                f"{label} FrontEnd SHA {actual} does not match authorized SHA "
                f"{authorized_sha}"
            )
    return diagnostics


def _suite_digest(result: dict[str, object], plan: dict[str, object]) -> str:
    candidates = [result.get("suite_digest")]
    suite = plan.get("suite")
    if isinstance(suite, dict):
        candidates.append(suite.get("digest"))
    sharding = plan.get("sharding")
    if isinstance(sharding, dict):
        candidates.append(sharding.get("suite_digest"))
    for candidate in candidates:
        if isinstance(candidate, str) and candidate:
            return candidate
    return "unavailable"


def _unique_messages(messages: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for message in messages:
        key = " ".join(message.split()).casefold()
        if key not in seen:
            seen.add(key)
            result.append(message)
    return result


def _infrastructure_priority(message: str) -> int:
    normalized = message.casefold()
    causal_markers = (
        "fatal",
        "kubernetes job",
        "pod ",
        "exit code",
        "mismatch",
        "timed out",
        "timeout",
        "failed",
    )
    if EXECUTION_STAGE_MESSAGE.match(message) or any(
        marker in normalized for marker in causal_markers
    ):
        return 0
    if "checkpoint" in normalized or normalized.startswith("missing shard result:"):
        return 1
    return 2


def _case_id_sort_key(case_id: str) -> tuple[int, tuple[int, ...], str]:
    parts = case_id.split(".")
    if parts and all(part.isdigit() for part in parts):
        return 0, tuple(int(part) for part in parts), case_id
    return 1, (), case_id


def _shard_evidence_diagnostics(
    plan: dict[str, object],
    shard_rows: list[dict[str, object]],
    aggregate_exit_code: int,
) -> list[str]:
    planned = _planned_shards(plan)
    if not planned:
        return ["Shard plan is missing; aggregate outcome cannot be verified"]

    diagnostics: list[str] = []
    for row in shard_rows:
        shard_index = row["shard"]
        if shard_index not in planned:
            diagnostics.append(f"shard {shard_index} is not present in the plan")
        if not row["terminal"]:
            diagnostics.append(f"shard {shard_index} is {str(row['result']).lower()}")
            continue
        shard_result = row["result"]
        if shard_result in {"INFRASTRUCTURE ERROR", "INCOMPLETE"}:
            conflict = (
                "an infrastructure error"
                if shard_result == "INFRASTRUCTURE ERROR"
                else "incomplete coverage"
            )
            diagnostics.append(
                f"shard {shard_index} reports {conflict} that conflicts "
                f"with aggregate exit code {aggregate_exit_code}"
            )
        elif aggregate_exit_code == 0 and shard_result != "PASSED":
            diagnostics.append(
                f"shard {shard_index} reports {str(shard_result).lower()} that conflicts "
                "with aggregate exit code 0"
            )
    if aggregate_exit_code == 1 and not any(
        row["result"] == "TEST FAILURE" for row in shard_rows
    ):
        diagnostics.append(
            "Aggregate exit code 1 has no matching test failure in terminal shard evidence"
        )
    return diagnostics


def _write_summary(
    destination: Path,
    run_id: str,
    run_root: Path,
    *,
    build_cache_hit: str | None = None,
    build_duration_seconds: float | None = None,
    execute_exit_code: int | None = None,
    routing_sha: str | None = None,
    pr_base_sha: str | None = None,
    pr_head_sha: str | None = None,
    frontend_ref: str,
    frontend_sha: str,
    frontend_source: str,
) -> int | None:
    result = _read_run_json(run_root, Path("result.json"))
    state = _read_run_json(run_root, Path("state.json"))
    plan = _read_run_json(run_root, Path("plan/manifest.json"))
    allocation = _read_run_json(run_root, Path("plan/allocation.json"))
    sources = result.get("sources") or plan.get("sources") or {}
    if not isinstance(sources, dict):
        sources = {}
    shard_rows, case_locations, shard_fatals = _shard_context(
        run_root, plan, allocation
    )
    exit_code, exit_diagnostics = _resolve_exit_code(result, state, execute_exit_code)
    frontend_diagnostics = _frontend_revision_diagnostics(
        run_root, result, plan, frontend_sha
    )
    if frontend_diagnostics:
        exit_code = 2
        exit_diagnostics.extend(frontend_diagnostics)
    if exit_code in {0, 1}:
        shard_diagnostics = _shard_evidence_diagnostics(plan, shard_rows, exit_code)
        if shard_diagnostics:
            exit_code = 2
            exit_diagnostics.extend(shard_diagnostics)
    outcome = {0: "PASSED", 1: "TEST FAILURE", 2: "INFRASTRUCTURE ERROR"}.get(
        exit_code, "INCOMPLETE"
    )
    aggregate_cases = result.get("cases")
    has_aggregate_cases = isinstance(aggregate_cases, list)
    cases = aggregate_cases
    if not has_aggregate_cases:
        cases = []
    not_run_case_ids = result.get("not_run_case_ids")
    infrastructure = result.get("infrastructure_errors")
    aggregate_infrastructure: list[str] = []
    folded_case_diagnostics = 0
    if isinstance(infrastructure, list):
        for message in infrastructure:
            if not isinstance(message, str):
                continue
            if CASE_LEVEL_DIAGNOSTIC.fullmatch(message):
                folded_case_diagnostics += 1
                continue
            aggregate_infrastructure.append(message)
    aggregate_infrastructure.sort(key=_infrastructure_priority)
    infrastructure_messages = exit_diagnostics + shard_fatals + aggregate_infrastructure
    for label, key in (
        ("Missing cases", "missing_case_ids"),
        ("Duplicate cases", "duplicate_case_ids"),
        ("Extra cases", "extra_case_ids"),
    ):
        identifiers = result.get(key)
        if isinstance(identifiers, list) and identifiers:
            visible = [str(identifier) for identifier in identifiers[:10]]
            suffix = (
                f" (+{len(identifiers) - 10} more)" if len(identifiers) > 10 else ""
            )
            infrastructure_messages.append(
                f"{label}: {len(identifiers)} ({', '.join(visible)}{suffix})"
            )
    infrastructure_messages = _unique_messages(infrastructure_messages)
    if exit_code == 2 and not infrastructure_messages:
        infrastructure_messages.append(
            "Infrastructure failure reported without aggregate error details"
        )

    aggregate_counts = _case_counts(cases)
    total = _integer(result.get("total"))
    passed = _integer(result.get("passed"))
    failed = _integer(result.get("failed"))
    error = _integer(result.get("error"))
    total = total if total is not None else "unavailable"
    passed = passed if passed is not None else "unavailable"
    failed = failed if failed is not None else "unavailable"
    error = error if error is not None else "unavailable"
    if isinstance(not_run_case_ids, list):
        not_run_count: object = len(not_run_case_ids)
    elif has_aggregate_cases:
        not_run_count = aggregate_counts["not_run"]
    elif shard_rows:
        checkpoint_not_run = sum(int(row["counts"]["not_run"]) for row in shard_rows)
        not_run_count = f"{checkpoint_not_run} (partial)"
    else:
        not_run_count = "unavailable"
    phase_durations = result.get("phase_durations_seconds")
    if not isinstance(phase_durations, dict):
        phase_durations = {}
    terminal_shards = sum(bool(row["terminal"]) for row in shard_rows)

    lines = [
        f"## CraneTestKit K3s system test: {outcome}",
        "",
        "### Result",
        "",
    ]
    _append_table(
        lines,
        ("Total", "Passed", "Failed", "Error", "Not run", "Infrastructure", "Shards"),
        [
            (
                total,
                passed,
                failed,
                error,
                not_run_count,
                len(infrastructure_messages),
                f"{terminal_shards}/{len(shard_rows)}",
            )
        ],
    )
    lines.append("")

    if infrastructure_messages:
        lines.extend(["### Infrastructure errors", ""])
        lines.extend(
            f"- `{_escape_markdown(message)}`"
            for message in infrastructure_messages[:MAX_INFRASTRUCTURE_ROWS]
        )
        if len(infrastructure_messages) > MAX_INFRASTRUCTURE_ROWS:
            lines.append(
                f"- Showing {MAX_INFRASTRUCTURE_ROWS} of {len(infrastructure_messages)} errors."
            )
        lines.append("")
    if folded_case_diagnostics:
        lines.extend(
            [
                f"_Folded {folded_case_diagnostics} case-level not-run or missing-result "
                "diagnostics into the aggregate case counts._",
                "",
            ]
        )

    interesting_cases: list[dict[str, object]] = []
    for case in cases:
        if not isinstance(case, dict) or case.get("status") not in {
            "failed",
            "error",
            "not_run",
        }:
            continue
        case_id_value = case.get("id")
        case_id = case_id_value if isinstance(case_id_value, str) else "unavailable"
        location = case_locations.get(case_id)
        shard = location[0] if location else "unavailable"
        worker = location[1] if location else "unavailable"
        slot = location[2] if location else "unavailable"
        interesting_cases.append(
            {
                "id": case_id,
                "name": (
                    case.get("name")
                    if isinstance(case.get("name"), str)
                    else "unavailable"
                ),
                "status": case.get("status", "unavailable"),
                "shard": shard,
                "worker": worker,
                "duration": _format_case_duration(case.get("duration_seconds")),
                "message": (
                    case.get("message") if isinstance(case.get("message"), str) else "-"
                ),
                "logs": f"logs/{worker}/{slot}/" if location else "unavailable",
            }
        )
    interesting_cases.sort(
        key=lambda case: (
            {"error": 0, "failed": 1, "not_run": 2}.get(str(case["status"]), 3),
            _case_id_sort_key(str(case["id"])),
        )
    )
    if interesting_cases:
        lines.extend(["### Failed, errored, or not-run cases", ""])
        _append_table(
            lines,
            (
                "Case",
                "Name",
                "Status",
                "Shard",
                "Worker",
                "Duration",
                "Message",
                "Logs",
            ),
            [
                (
                    case["id"],
                    case["name"],
                    case["status"],
                    case["shard"],
                    case["worker"],
                    case["duration"],
                    case["message"],
                    case["logs"],
                )
                for case in interesting_cases[:MAX_FAILURE_ROWS]
            ],
        )
        if len(interesting_cases) > MAX_FAILURE_ROWS:
            lines.extend(
                [
                    "",
                    f"Showing {MAX_FAILURE_ROWS} of {len(interesting_cases)} affected cases.",
                ]
            )
        lines.append("")

    if shard_rows:
        lines.extend(["### Shards", ""])
        _append_table(
            lines,
            (
                "Shard",
                "Worker",
                "Cases (pass/fail/error/not run)",
                "Wall",
                "Case time",
                "Estimate",
                "Result",
            ),
            [
                (
                    row["shard"],
                    f"{row['worker']} / {row['slot']}",
                    (
                        f"{row['cases']} ({row['counts']['passed']}/{row['counts']['failed']}/"
                        f"{row['counts']['error']}/{row['counts']['not_run']})"
                    ),
                    _format_duration(row["wall"]),
                    _format_duration(row["case_time"]),
                    _format_duration(row["estimate"]),
                    row["result"],
                )
                for row in shard_rows
            ],
        )
        lines.append("")

    timing_rows: list[tuple[object, ...]] = []
    if _number(build_duration_seconds) is not None:
        timing_rows.append(("Build", _format_duration(build_duration_seconds)))
    for key, label in (
        ("plan", "Plan"),
        ("lease_acquire", "Lease acquire"),
        ("run", "Submit Jobs"),
        ("wait", "Run and wait"),
        ("collect", "Collect"),
        ("stop", "Stop"),
        ("lease_release", "Lease release"),
        ("total", "System execute total"),
    ):
        if _number(phase_durations.get(key)) is not None:
            timing_rows.append((label, _format_duration(phase_durations[key])))
    if timing_rows:
        lines.extend(["### Timing", ""])
        _append_table(lines, ("Phase", "Duration"), timing_rows)
        if build_cache_hit in {"true", "false"}:
            lines.extend(
                [
                    "",
                    f"Build cache: **{'hit' if build_cache_hit == 'true' else 'miss'}**",
                ]
            )
        lines.append("")

    completed_cases = [
        case
        for case in cases
        if isinstance(case, dict)
        and case.get("status") != "not_run"
        and _number(case.get("duration_seconds")) is not None
    ]
    completed_cases.sort(
        key=lambda case: (-float(case["duration_seconds"]), str(case.get("id", "")))
    )
    if completed_cases:
        lines.extend(["### Slowest cases", ""])
        slow_rows: list[tuple[object, ...]] = []
        for case in completed_cases[:MAX_SLOW_CASE_ROWS]:
            case_id_value = case.get("id")
            case_id = case_id_value if isinstance(case_id_value, str) else "unavailable"
            location = case_locations.get(case_id)
            placement = (
                f"{location[1]} / shard {location[0]}" if location else "unavailable"
            )
            slow_rows.append(
                (
                    case_id,
                    (
                        case.get("name")
                        if isinstance(case.get("name"), str)
                        else "unavailable"
                    ),
                    case.get("status", "unavailable"),
                    _format_case_duration(case.get("duration_seconds")),
                    placement,
                )
            )
        _append_table(
            lines, ("Case", "Name", "Status", "Duration", "Placement"), slow_rows
        )
        lines.append("")

    provenance_rows = [
        ("Run ID", run_id),
        ("Phase", state.get("phase", "not-created")),
        ("Exit code", exit_code if exit_code is not None else "unavailable"),
        ("Authorized FrontEnd repository", FRONTEND_REPOSITORY),
        ("Authorized FrontEnd ref", frontend_ref),
        ("Authorized FrontEnd source", frontend_source),
        ("Authorized FrontEnd SHA", frontend_sha),
        ("Backend SHA", sources.get("backend", "unavailable")),
        ("Frontend SHA", sources.get("frontend", "unavailable")),
        ("AutoTest SHA", sources.get("autotest", "unavailable")),
        ("Build ID", result.get("build_id", plan.get("build_id", "unavailable"))),
        ("Suite digest", _suite_digest(result, plan)),
        (
            "Plan digest",
            result.get("plan_digest", plan.get("plan_digest", "unavailable")),
        ),
        ("Image", result.get("image_digest", plan.get("image_digest", "unavailable"))),
        ("Started", result.get("started_at", "unavailable")),
        ("Finished", result.get("finished_at", "unavailable")),
    ]
    if routing_sha is not None:
        provenance_rows.insert(4, ("Workflow routing SHA", routing_sha))
    if pr_head_sha is not None:
        provenance_rows[4:4] = [
            ("PR base SHA", pr_base_sha or "unavailable"),
            ("PR head SHA", pr_head_sha),
        ]
    lines.extend(["<details>", "<summary>Provenance</summary>", ""])
    _append_table(lines, ("Field", "Value"), provenance_rows)
    lines.extend(["", "</details>", ""])

    text = "\n".join(lines) + "\n"
    encoded = text.encode("utf-8")
    if len(encoded) > MAX_SUMMARY_BYTES:
        suffix = (
            "\n\n_Report truncated; download the JSON artifact for complete details._\n"
        )
        limit = MAX_SUMMARY_BYTES - len(suffix.encode("utf-8"))
        text = encoded[:limit].decode("utf-8", errors="ignore") + suffix
    destination.write_text(text, encoding="utf-8")
    return exit_code


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--include-logs", action="store_true")
    parser.add_argument("--build-cache-hit", choices=("true", "false"))
    parser.add_argument("--build-duration-seconds", type=float)
    parser.add_argument("--execute-exit-code", type=int, choices=(0, 1, 2))
    parser.add_argument("--routing-sha", type=_full_sha)
    parser.add_argument("--pr-base-sha", type=_full_sha)
    parser.add_argument("--pr-head-sha", type=_full_sha)
    parser.add_argument("--frontend-ref", type=_frontend_ref, required=True)
    parser.add_argument("--frontend-sha", type=_full_sha, required=True)
    parser.add_argument("--frontend-source", choices=FRONTEND_SOURCES, required=True)
    args = parser.parse_args()
    if (args.pr_base_sha is None) != (args.pr_head_sha is None):
        parser.error("PR base and head SHAs must be provided together")

    destination = args.destination.absolute()
    if destination.is_symlink():
        parser.error("destination must not be a symlink")
    if destination.exists() and not destination.is_dir():
        parser.error("destination must be a directory")
    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir(parents=True)
    entries: list[dict[str, object]] = []

    run_root = args.run_root
    summary_exit_code = _write_summary(
        destination / "summary.md",
        args.run_id,
        run_root,
        build_cache_hit=args.build_cache_hit,
        build_duration_seconds=args.build_duration_seconds,
        execute_exit_code=args.execute_exit_code,
        routing_sha=args.routing_sha,
        pr_base_sha=args.pr_base_sha,
        pr_head_sha=args.pr_head_sha,
        frontend_ref=args.frontend_ref,
        frontend_sha=args.frontend_sha,
        frontend_source=args.frontend_source,
    )
    include_failure_logs = args.include_logs or summary_exit_code != 0
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

        if include_failure_logs:
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

    manifest = {
        "apiVersion": "cranesched.io/v1alpha1",
        "kind": "CraneTestKitCiArtifactSet",
        "run_id": args.run_id,
        "run_present": run_root.is_dir() and not run_root.is_symlink(),
        "failure_logs_included": include_failure_logs,
        "check_exit_code": (summary_exit_code if summary_exit_code in {0, 1, 2} else 2),
        "revision_routing": {
            "routing_sha": args.routing_sha,
            "pr_base_sha": args.pr_base_sha,
            "pr_head_sha": args.pr_head_sha,
            "frontend": {
                "repository": FRONTEND_REPOSITORY,
                "ref": args.frontend_ref,
                "sha": args.frontend_sha,
                "source": args.frontend_source,
            },
        },
        "files": entries,
    }
    (destination / "artifact-manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
