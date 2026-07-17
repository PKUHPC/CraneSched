#!/usr/bin/env python3
"""Keep the privileged repository runner label in its trusted workflow."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


ALLOWED_WORKFLOW = "build.yaml"
ALLOWED_JOB = "test"
RUNNER_LABEL = "cranesystemtest"
_LABEL_CHARACTER = r"A-Za-z0-9_.-"
_JOB_KEY_RE = re.compile(r"^  ([A-Za-z0-9_-]+):\s*(?:#.*)?$")
_INLINE_RUNS_ON_RE = re.compile(
    r"^    runs-on:\s*\[([^\]]+)\]\s*(?:#.*)?$",
    re.IGNORECASE,
)


class RoutingPolicyError(RuntimeError):
    pass


def _label_pattern(label: str) -> re.Pattern[str]:
    if not label or label != label.strip():
        raise ValueError("runner label must be a non-empty trimmed string")
    return re.compile(
        rf"(?<![{_LABEL_CHARACTER}]){re.escape(label)}"
        rf"(?![{_LABEL_CHARACTER}])",
        re.IGNORECASE,
    )


def _validate_allowed_job_selector(path: Path, runner_label: str) -> None:
    lines = path.read_text(encoding="utf-8").splitlines()
    jobs_headers = [index for index, line in enumerate(lines) if line == "jobs:"]
    if len(jobs_headers) != 1:
        raise RoutingPolicyError(f"{ALLOWED_WORKFLOW} must have one canonical jobs section")
    jobs_start = jobs_headers[0] + 1
    jobs_end = len(lines)
    for index in range(jobs_start, len(lines)):
        line = lines[index]
        if line and not line[0].isspace() and not line.startswith("#"):
            jobs_end = index
            break

    job_start: int | None = None
    job_end = jobs_end
    for index in range(jobs_start, jobs_end):
        line = lines[index]
        match = _JOB_KEY_RE.fullmatch(line)
        if match is None:
            continue
        if job_start is None and match.group(1) == ALLOWED_JOB:
            job_start = index + 1
            continue
        if job_start is not None:
            job_end = index
            break
    if job_start is None:
        raise RoutingPolicyError(f"allowed job is missing: {ALLOWED_JOB}")

    selectors = [
        match.group(1)
        for line in lines[job_start:job_end]
        if (match := _INLINE_RUNS_ON_RE.fullmatch(line)) is not None
    ]
    if len(selectors) != 1:
        raise RoutingPolicyError(
            f"{ALLOWED_WORKFLOW}:{ALLOWED_JOB} must have one inline runs-on selector"
        )
    labels = [item.strip().strip("'\"").casefold() for item in selectors[0].split(",")]
    if labels != ["self-hosted", runner_label.casefold()]:
        raise RoutingPolicyError(
            f"{ALLOWED_WORKFLOW}:{ALLOWED_JOB} must route only to "
            f"[self-hosted, {runner_label}]"
        )


def validate_workflow_routing(
    workflows_dir: Path,
    *,
    allowed_workflow: str = ALLOWED_WORKFLOW,
    runner_label: str = RUNNER_LABEL,
) -> None:
    if not workflows_dir.is_dir() or workflows_dir.is_symlink():
        raise RoutingPolicyError("workflow directory must be a regular directory")
    if Path(allowed_workflow).name != allowed_workflow:
        raise RoutingPolicyError("allowed workflow must be a filename")

    pattern = _label_pattern(runner_label)
    workflow_paths = sorted(
        path
        for path in workflows_dir.iterdir()
        if path.suffix in {".yaml", ".yml"}
    )
    allowed_path = workflows_dir / allowed_workflow
    if allowed_path not in workflow_paths:
        raise RoutingPolicyError(f"allowed workflow is missing: {allowed_workflow}")

    violations: list[str] = []
    for path in workflow_paths:
        if path.is_symlink() or not path.is_file():
            raise RoutingPolicyError(f"workflow must be a regular file: {path.name}")
        text = path.read_text(encoding="utf-8")
        for line_number, line in enumerate(text.splitlines(), start=1):
            if pattern.search(line) is None:
                continue
            if path != allowed_path:
                violations.append(f"{path.name}:{line_number}")

    if violations:
        locations = ", ".join(violations)
        raise RoutingPolicyError(
            f"runner label {runner_label!r} is restricted to {allowed_workflow}; "
            f"found in {locations}"
        )
    _validate_allowed_job_selector(allowed_path, runner_label)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workflows-dir", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        validate_workflow_routing(args.workflows_dir)
    except (OSError, UnicodeError, RoutingPolicyError, ValueError) as exc:
        print(f"workflow routing validation failed: {exc}", file=sys.stderr)
        return 1
    print(
        f"workflow routing validated: {RUNNER_LABEL} is restricted to "
        f"{ALLOWED_WORKFLOW}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
