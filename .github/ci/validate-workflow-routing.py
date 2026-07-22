#!/usr/bin/env python3
"""Keep every workflow job on its approved runner boundary."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


ALLOWED_WORKFLOW = "build.yaml"
ALLOWED_JOB = "test"
RUNNER_LABEL = "cranesystemtest"
GITHUB_HOSTED_RUNNER = "ubuntu-latest"
_JOB_KEY_RE = re.compile(r"^  ([A-Za-z0-9_-]+):\s*(?:#.*)?$")
_DIRECT_PROPERTY_RE = re.compile(r"^    ([A-Za-z0-9_-]+):(?:\s*(.*?))?\s*$")


class RoutingPolicyError(RuntimeError):
    pass


def _split_jobs(path: Path) -> dict[str, list[str]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    jobs_headers = [index for index, line in enumerate(lines) if line == "jobs:"]
    if len(jobs_headers) != 1:
        raise RoutingPolicyError(f"{path.name} must have one canonical jobs section")
    jobs_start = jobs_headers[0] + 1
    jobs_end = len(lines)
    for index in range(jobs_start, len(lines)):
        line = lines[index]
        if line and not line[0].isspace() and not line.startswith("#"):
            raise RoutingPolicyError(
                f"{path.name} jobs must be the final top-level section"
            )

    starts: list[tuple[str, int]] = []
    for index in range(jobs_start, jobs_end):
        line = lines[index]
        match = _JOB_KEY_RE.fullmatch(line)
        if match is not None:
            starts.append((match.group(1), index))
            continue
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        indentation = len(line) - len(line.lstrip(" "))
        if indentation < 4:
            raise RoutingPolicyError(
                f"{path.name} has a non-canonical job declaration: {line.strip()}"
            )
    if not starts:
        raise RoutingPolicyError(f"{path.name} must declare at least one job")
    if len({name for name, _ in starts}) != len(starts):
        raise RoutingPolicyError(f"{path.name} contains a duplicate job declaration")

    jobs: dict[str, list[str]] = {}
    for offset, (name, start) in enumerate(starts):
        end = starts[offset + 1][1] if offset + 1 < len(starts) else jobs_end
        jobs[name] = lines[start + 1 : end]
    return jobs


def _direct_properties(lines: list[str]) -> dict[str, list[str]]:
    properties: dict[str, list[str]] = {}
    for line in lines:
        match = _DIRECT_PROPERTY_RE.fullmatch(line)
        if match is None:
            continue
        value = (match.group(2) or "").strip()
        if " #" in value:
            value = value.split(" #", 1)[0].rstrip()
        properties.setdefault(match.group(1).casefold(), []).append(value)
    return properties


def _validate_job_selector(
    path: Path,
    job_name: str,
    lines: list[str],
    *,
    privileged: bool,
    runner_label: str,
) -> None:
    for line in lines:
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        indentation = len(line) - len(line.lstrip(" "))
        if indentation == 4 and _DIRECT_PROPERTY_RE.fullmatch(line) is None:
            raise RoutingPolicyError(
                f"{path.name}:{job_name} has a non-canonical job property: "
                f"{line.strip()}"
            )
    properties = _direct_properties(lines)
    if "uses" in properties:
        raise RoutingPolicyError(
            f"{path.name}:{job_name} must not call a job-level reusable workflow"
        )
    selectors = properties.get("runs-on", [])
    if len(selectors) != 1:
        raise RoutingPolicyError(
            f"{path.name}:{job_name} must have one runs-on selector"
        )

    selector = selectors[0]
    if privileged:
        expected = f"[self-hosted, {runner_label}]"
        if selector.casefold() != expected.casefold():
            raise RoutingPolicyError(
                f"{path.name}:{job_name} must route only to {expected}"
            )
        return

    unquoted = selector
    if len(selector) >= 2 and selector[0] == selector[-1] and selector[0] in "'\"":
        unquoted = selector[1:-1]
    if unquoted != GITHUB_HOSTED_RUNNER:
        raise RoutingPolicyError(
            f"{path.name}:{job_name} must route to the fixed GitHub-hosted "
            f"runner {GITHUB_HOSTED_RUNNER}"
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

    workflow_paths = sorted(
        path for path in workflows_dir.iterdir() if path.suffix in {".yaml", ".yml"}
    )
    allowed_path = workflows_dir / allowed_workflow
    if allowed_path not in workflow_paths:
        raise RoutingPolicyError(f"allowed workflow is missing: {allowed_workflow}")

    for path in workflow_paths:
        if path.is_symlink() or not path.is_file():
            raise RoutingPolicyError(f"workflow must be a regular file: {path.name}")
        jobs = _split_jobs(path)
        for job_name, lines in jobs.items():
            privileged = path == allowed_path and job_name == ALLOWED_JOB
            _validate_job_selector(
                path,
                job_name,
                lines,
                privileged=privileged,
                runner_label=runner_label,
            )
    if ALLOWED_JOB not in _split_jobs(allowed_path):
        raise RoutingPolicyError(f"allowed job is missing: {ALLOWED_JOB}")


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
        f"workflow routing validated: {ALLOWED_WORKFLOW}:{ALLOWED_JOB} is the "
        f"only {RUNNER_LABEL} job and every other job uses {GITHUB_HOSTED_RUNNER}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
