#!/usr/bin/env python3
"""Record an authorized request for a maintainer-approved fork CI rerun."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol


EXPECTED_REPOSITORY = "PKUHPC/CraneSched"
EXPECTED_WORKFLOW_NAME = "CraneTestKit K3s CI"
EXPECTED_WORKFLOW_PATH = ".github/workflows/build.yaml"
EXPECTED_AUTHORIZE_JOB = "Authorize trusted test dispatch"
EXPECTED_AUTHORIZE_STEP = "Authorize actor and resolve immutable revisions"
EXPECTED_SYSTEM_JOB = "system-test"
REQUEST_COMMAND = "/request-ci"
MARKER_PREFIX = "<!-- cranesched-ci-request:"
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
REF_RE = re.compile(r"^[^\x00-\x20\x7f]+(?: [^\x00-\x1f\x7f]+)*$")
MARKER_RE = re.compile(r"^<!-- cranesched-ci-request:(\{[^\r\n]*\}) -->$", re.MULTILINE)
_MAX_PAGES = 10


class RequestCiError(RuntimeError):
    pass


def _positive_int(value: object, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise RequestCiError(f"{label} is invalid")
    return value


def _required_string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise RequestCiError(f"{label} is invalid")
    return value


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise RequestCiError(f"{label} is malformed")
    return value


def _list(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise RequestCiError(f"{label} is malformed")
    return value


@dataclass(frozen=True)
class PullRequestSnapshot:
    number: int
    state: str
    draft: bool
    base_repository: str
    base_ref: str
    base_sha: str
    head_repository: str
    head_ref: str
    head_sha: str
    author: str


@dataclass(frozen=True)
class WorkflowRun:
    run_id: int
    created_at: datetime
    html_url: str
    head_repository: str
    head_ref: str
    head_sha: str


@dataclass(frozen=True)
class Attestation:
    pr_number: int
    head_sha: str
    request_comment_id: int
    run_id: int
    schema: int = 1

    def payload(self) -> dict[str, object]:
        return {
            "head_sha": self.head_sha,
            "pr_number": self.pr_number,
            "request_comment_id": self.request_comment_id,
            "run_id": self.run_id,
            "schema": self.schema,
        }


@dataclass(frozen=True)
class IssueComment:
    comment_id: int
    body: str
    author: str
    author_type: str
    issue_url: str = ""


@dataclass(frozen=True)
class RequestResult:
    action: str
    comment_id: int
    head_sha: str
    run_id: int


class RequestCiApi(Protocol):
    def permission(self, repository: str, actor: str) -> dict[str, object]: ...

    def pull_request(self, repository: str, number: int) -> PullRequestSnapshot: ...

    def workflow_runs(
        self, repository: str, head_sha: str
    ) -> list[dict[str, object]]: ...

    def run_jobs(
        self, repository: str, run_id: int, attempt: int
    ) -> list[dict[str, object]]: ...

    def issue_comments(self, repository: str, number: int) -> list[IssueComment]: ...

    def create_issue_comment(
        self, repository: str, number: int, body: str
    ) -> IssueComment: ...

    def update_issue_comment(
        self, repository: str, number: int, comment_id: int, body: str
    ) -> IssueComment: ...


def canonical_marker(attestation: Attestation) -> str:
    _validate_attestation(attestation)
    encoded = json.dumps(
        attestation.payload(), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    return f"{MARKER_PREFIX}{encoded} -->"


def _validate_attestation(attestation: Attestation) -> None:
    if attestation.schema != 1:
        raise RequestCiError("CI request attestation schema is unsupported")
    _positive_int(attestation.pr_number, "attestation pull request number")
    _positive_int(attestation.request_comment_id, "attestation request comment ID")
    _positive_int(attestation.run_id, "attestation workflow run ID")
    if SHA_RE.fullmatch(attestation.head_sha) is None:
        raise RequestCiError("attestation head SHA is invalid")


def parse_marker(body: str) -> Attestation | None:
    if not isinstance(body, str):
        raise RequestCiError("bot comment body is malformed")
    occurrences = body.count(MARKER_PREFIX)
    if occurrences == 0:
        return None
    if occurrences != 1:
        raise RequestCiError("bot comment contains multiple CI request markers")
    match = MARKER_RE.search(body)
    if match is None:
        raise RequestCiError("bot comment contains a malformed CI request marker")
    try:
        value = json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        raise RequestCiError("bot comment contains invalid attestation JSON") from exc
    if not isinstance(value, dict) or set(value) != {
        "head_sha",
        "pr_number",
        "request_comment_id",
        "run_id",
        "schema",
    }:
        raise RequestCiError("bot comment attestation fields are invalid")
    attestation = Attestation(
        pr_number=_positive_int(value["pr_number"], "attestation pull request number"),
        head_sha=_required_string(value["head_sha"], "attestation head SHA"),
        request_comment_id=_positive_int(
            value["request_comment_id"], "attestation request comment ID"
        ),
        run_id=_positive_int(value["run_id"], "attestation workflow run ID"),
        schema=_positive_int(value["schema"], "attestation schema"),
    )
    _validate_attestation(attestation)
    if match.group(0) != canonical_marker(attestation):
        raise RequestCiError("bot comment attestation is not canonical")
    return attestation


def render_response(attestation: Attestation, run_url: str) -> str:
    marker = canonical_marker(attestation)
    expected_url = (
        f"https://github.com/{EXPECTED_REPOSITORY}/actions/runs/{attestation.run_id}"
    )
    if run_url != expected_url:
        raise RequestCiError("workflow run URL is invalid")
    return (
        f"{marker}\n\n"
        f"CI was requested for fork pull request #{attestation.pr_number} at "
        f"head `{attestation.head_sha}`.\n\n"
        f"A maintainer can approve this exact revision by opening "
        f"[Actions run {attestation.run_id}]({run_url}) and selecting "
        "**Re-run all jobs**.\n\n"
        "If the pull request head changes, comment `/request-ci` again."
    )


def _parse_timestamp(value: object) -> datetime:
    timestamp = _required_string(value, "workflow run creation time")
    if not timestamp.endswith("Z"):
        raise RequestCiError("workflow run creation time is invalid")
    try:
        parsed = datetime.fromisoformat(f"{timestamp[:-1]}+00:00")
    except ValueError as exc:
        raise RequestCiError("workflow run creation time is invalid") from exc
    return parsed


def _repository_name(value: object, label: str) -> str:
    repository = _mapping(value, label)
    return _required_string(repository.get("full_name"), f"{label} name")


def _candidate_run(
    value: dict[str, object], snapshot: PullRequestSnapshot
) -> WorkflowRun:
    run_id = _positive_int(value.get("id"), "workflow run ID")
    if value.get("name") != EXPECTED_WORKFLOW_NAME:
        raise RequestCiError("workflow run name is unexpected")
    if value.get("path") != EXPECTED_WORKFLOW_PATH:
        raise RequestCiError("workflow run path is unexpected")
    if value.get("event") != "pull_request_target":
        raise RequestCiError("workflow run event is unexpected")
    _positive_int(value.get("run_attempt"), "workflow run attempt")
    if value.get("status") != "completed" or value.get("conclusion") != "failure":
        raise RequestCiError("initial workflow run did not finish as a failure")
    if (
        _repository_name(value.get("repository"), "workflow repository")
        != EXPECTED_REPOSITORY
    ):
        raise RequestCiError("workflow run repository is unexpected")
    head_repository = _repository_name(
        value.get("head_repository"), "workflow head repository"
    )
    head_ref = _required_string(value.get("head_branch"), "workflow head branch")
    head_sha = _required_string(value.get("head_sha"), "workflow head SHA")
    if (
        head_repository != snapshot.head_repository
        or head_ref != snapshot.head_ref
        or head_sha != snapshot.head_sha
    ):
        raise RequestCiError(
            "workflow run does not match the current pull request head"
        )
    expected_url = f"https://github.com/{EXPECTED_REPOSITORY}/actions/runs/{run_id}"
    html_url = _required_string(value.get("html_url"), "workflow run URL")
    if html_url != expected_url:
        raise RequestCiError("workflow run URL is invalid")
    return WorkflowRun(
        run_id=run_id,
        created_at=_parse_timestamp(value.get("created_at")),
        html_url=html_url,
        head_repository=head_repository,
        head_ref=head_ref,
        head_sha=head_sha,
    )


def select_workflow_run(
    values: list[dict[str, object]], snapshot: PullRequestSnapshot
) -> WorkflowRun:
    matching: list[dict[str, object]] = []
    seen_ids: set[int] = set()
    for value in values:
        if not isinstance(value, dict):
            raise RequestCiError("workflow runs response is malformed")
        run_id = _positive_int(value.get("id"), "workflow run ID")
        if run_id in seen_ids:
            raise RequestCiError("workflow runs response contains a duplicate run")
        seen_ids.add(run_id)
        if value.get("head_sha") == snapshot.head_sha:
            matching.append(value)
    if not matching:
        raise RequestCiError("no workflow run exists for the current pull request head")

    newest_value = max(
        matching,
        key=lambda item: (_parse_timestamp(item.get("created_at")), item["id"]),
    )
    return _candidate_run(newest_value, snapshot)


def _job_labels(value: object) -> list[str]:
    labels = _list(value, "workflow job labels")
    if any(not isinstance(label, str) for label in labels):
        raise RequestCiError("workflow job labels are malformed")
    return labels


def validate_initial_jobs(values: list[dict[str, object]]) -> None:
    authorize_jobs: list[dict[str, object]] = []
    system_jobs: list[dict[str, object]] = []
    seen_ids: set[int] = set()
    for value in values:
        if not isinstance(value, dict):
            raise RequestCiError("workflow jobs response is malformed")
        job_id = _positive_int(value.get("id"), "workflow job ID")
        if job_id in seen_ids:
            raise RequestCiError("workflow jobs response contains a duplicate job")
        seen_ids.add(job_id)
        name = _required_string(value.get("name"), "workflow job name")
        labels = _job_labels(value.get("labels"))
        if name == EXPECTED_AUTHORIZE_JOB:
            authorize_jobs.append(value)
        if name == EXPECTED_SYSTEM_JOB or labels == ["self-hosted", "cranesystemtest"]:
            system_jobs.append(value)

    if len(authorize_jobs) != 1:
        raise RequestCiError("initial workflow must contain exactly one authorize job")
    authorize_job = authorize_jobs[0]
    if (
        authorize_job.get("status") != "completed"
        or authorize_job.get("conclusion") != "failure"
        or _job_labels(authorize_job.get("labels")) != ["ubuntu-latest"]
    ):
        raise RequestCiError("initial authorize job did not fail on GitHub-hosted")
    authorize_steps = _list(authorize_job.get("steps"), "initial authorize job steps")
    policy_steps: list[dict[str, object]] = []
    for step in authorize_steps:
        parsed_step = _mapping(step, "initial authorize job step")
        if parsed_step.get("name") == EXPECTED_AUTHORIZE_STEP:
            policy_steps.append(parsed_step)
    if len(policy_steps) != 1 or (
        policy_steps[0].get("status") != "completed"
        or policy_steps[0].get("conclusion") != "failure"
    ):
        raise RequestCiError("initial authorization policy step did not fail")

    if len(system_jobs) != 1:
        raise RequestCiError(
            "initial workflow must contain exactly one system-test job"
        )
    system_job = system_jobs[0]
    if system_job.get("name") != EXPECTED_SYSTEM_JOB:
        raise RequestCiError("privileged workflow job name is unexpected")
    if _job_labels(system_job.get("labels")) != [
        "self-hosted",
        "cranesystemtest",
    ]:
        raise RequestCiError("privileged workflow job labels are unexpected")
    if (
        system_job.get("status") != "completed"
        or system_job.get("conclusion") != "skipped"
    ):
        raise RequestCiError("privileged workflow job was not safely skipped")
    runner_fields = (
        "runner_id",
        "runner_name",
        "runner_group_id",
        "runner_group_name",
    )
    if any(
        field not in system_job or system_job[field] is not None
        for field in runner_fields
    ):
        raise RequestCiError("privileged workflow job was assigned to a runner")
    if _list(system_job.get("steps"), "privileged workflow job steps"):
        raise RequestCiError("privileged workflow job started executing steps")


def _validate_pr(snapshot: PullRequestSnapshot, number: int) -> None:
    if snapshot.number != number:
        raise RequestCiError("GitHub returned an unexpected pull request")
    if snapshot.state != "open":
        raise RequestCiError("pull request is not open")
    if snapshot.draft:
        raise RequestCiError("draft pull requests cannot request CI")
    if snapshot.base_repository != EXPECTED_REPOSITORY or snapshot.base_ref != "master":
        raise RequestCiError("pull request does not target PKUHPC/CraneSched:master")
    if snapshot.head_repository == snapshot.base_repository:
        raise RequestCiError("same-repository pull requests do not use /request-ci")
    if SHA_RE.fullmatch(snapshot.base_sha) is None:
        raise RequestCiError("pull request base SHA is invalid")
    if SHA_RE.fullmatch(snapshot.head_sha) is None:
        raise RequestCiError("pull request head SHA is invalid")
    if REF_RE.fullmatch(snapshot.head_ref) is None:
        raise RequestCiError("pull request head ref is invalid")


def _has_maintain_permission(value: dict[str, object]) -> bool:
    if value.get("permission") in {"maintain", "admin"}:
        return True
    user = value.get("user")
    permissions = user.get("permissions") if isinstance(user, dict) else None
    return isinstance(permissions, dict) and (
        permissions.get("maintain") is True or permissions.get("admin") is True
    )


def _validate_requester(
    snapshot: PullRequestSnapshot,
    repository: str,
    commenter: str,
    commenter_type: str,
    api: RequestCiApi,
) -> None:
    if commenter_type != "User":
        raise RequestCiError("only a user may request CI")
    if commenter == snapshot.author:
        return
    if not _has_maintain_permission(api.permission(repository, commenter)):
        raise RequestCiError(
            "only the pull request author or a maintain/admin user may request CI"
        )


def _event_request(event: object, repository: str) -> tuple[int, int, str, str] | None:
    root = _mapping(event, "issue_comment event")
    if root.get("action") != "created":
        return None
    comment = _mapping(root.get("comment"), "issue comment")
    if comment.get("body") != REQUEST_COMMAND:
        return None
    issue = _mapping(root.get("issue"), "issue")
    if not isinstance(issue.get("pull_request"), dict):
        return None
    event_repository = _mapping(root.get("repository"), "event repository")
    if event_repository.get("full_name") != repository:
        raise RequestCiError("issue_comment repository does not match the workflow")
    number = _positive_int(issue.get("number"), "pull request number")
    comment_id = _positive_int(comment.get("id"), "request comment ID")
    user = _mapping(comment.get("user"), "request comment author")
    commenter = _required_string(user.get("login"), "request comment author login")
    commenter_type = _required_string(user.get("type"), "request comment author type")
    return number, comment_id, commenter, commenter_type


def handle_request(
    event: object, repository: str, api: RequestCiApi
) -> RequestResult | None:
    if repository != EXPECTED_REPOSITORY:
        raise RequestCiError("request workflow repository is unexpected")
    request = _event_request(event, repository)
    if request is None:
        return None
    number, request_comment_id, commenter, commenter_type = request

    snapshot = api.pull_request(repository, number)
    _validate_pr(snapshot, number)
    _validate_requester(snapshot, repository, commenter, commenter_type, api)
    run = select_workflow_run(
        api.workflow_runs(repository, snapshot.head_sha), snapshot
    )
    validate_initial_jobs(api.run_jobs(repository, run.run_id, 1))

    existing: list[IssueComment] = []
    for comment in api.issue_comments(repository, number):
        if comment.author != "github-actions[bot]" or comment.author_type != "Bot":
            continue
        marker = parse_marker(comment.body)
        if marker is None:
            continue
        if marker.pr_number != number:
            raise RequestCiError("bot attestation belongs to another pull request")
        if marker.head_sha == snapshot.head_sha:
            existing.append(comment)
    if len(existing) > 1:
        raise RequestCiError("multiple bot attestations exist for the current head")

    final_snapshot = api.pull_request(repository, number)
    _validate_pr(final_snapshot, number)
    if final_snapshot != snapshot:
        raise RequestCiError("pull request changed while recording the CI request")
    _validate_requester(final_snapshot, repository, commenter, commenter_type, api)

    attestation = Attestation(
        pr_number=number,
        head_sha=snapshot.head_sha,
        request_comment_id=request_comment_id,
        run_id=run.run_id,
    )
    body = render_response(attestation, run.html_url)
    if existing:
        response = api.update_issue_comment(
            repository, number, existing[0].comment_id, body
        )
        action = "updated"
        expected_comment_id = existing[0].comment_id
    else:
        response = api.create_issue_comment(repository, number, body)
        action = "created"
        expected_comment_id = response.comment_id
    if (
        response.comment_id != expected_comment_id
        or response.author != "github-actions[bot]"
        or response.author_type != "Bot"
        or response.body != body
    ):
        raise RequestCiError("GitHub returned an unexpected bot comment")
    if parse_marker(response.body) != attestation:
        raise RequestCiError(
            "GitHub did not persist the expected CI request attestation"
        )
    return RequestResult(
        action=action,
        comment_id=response.comment_id,
        head_sha=snapshot.head_sha,
        run_id=run.run_id,
    )


class GitHubApi:
    def __init__(self, token: str, api_url: str = "https://api.github.com") -> None:
        if not token:
            raise RequestCiError("GitHub token is unavailable")
        parsed = urllib.parse.urlsplit(api_url)
        if (
            parsed.scheme != "https"
            or not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise RequestCiError("GitHub API URL is invalid")
        self._token = token
        self._api_url = api_url.rstrip("/")

    def _request(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, str] | None = None,
        body: dict[str, object] | None = None,
    ) -> object:
        url = f"{self._api_url}/{path}"
        if query:
            url = f"{url}?{urllib.parse.urlencode(query)}"
        encoded_body = None
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self._token}",
            "User-Agent": "CraneSched-CI-request-handler",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if body is not None:
            encoded_body = json.dumps(body, separators=(",", ":")).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            url, data=encoded_body, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                expected_status = {"GET": 200, "POST": 201, "PATCH": 200}.get(method)
                if expected_status is None or response.status != expected_status:
                    raise RequestCiError(
                        "GitHub API returned an unexpected HTTP status"
                    )
                value = json.load(response)
        except urllib.error.HTTPError as exc:
            raise RequestCiError(
                f"GitHub API request failed with HTTP {exc.code}"
            ) from None
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise RequestCiError("GitHub API request failed") from exc
        return value

    def _object(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, str] | None = None,
        body: dict[str, object] | None = None,
    ) -> dict[str, object]:
        return _mapping(
            self._request(method, path, query=query, body=body),
            "GitHub API response",
        )

    def _paginated_list(
        self, path: str, *, query: dict[str, str] | None = None
    ) -> list[object]:
        values: list[object] = []
        base_query = dict(query or {})
        for page in range(1, _MAX_PAGES + 1):
            page_query = {**base_query, "per_page": "100", "page": str(page)}
            current = _list(
                self._request("GET", path, query=page_query),
                "GitHub API list response",
            )
            values.extend(current)
            if len(current) < 100:
                return values
        raise RequestCiError("GitHub API pagination exceeded the safety limit")

    def permission(self, repository: str, actor: str) -> dict[str, object]:
        encoded_actor = urllib.parse.quote(actor, safe="")
        return self._object(
            "GET", f"repos/{repository}/collaborators/{encoded_actor}/permission"
        )

    def pull_request(self, repository: str, number: int) -> PullRequestSnapshot:
        value = self._object("GET", f"repos/{repository}/pulls/{number}")
        base = _mapping(value.get("base"), "pull request base")
        head = _mapping(value.get("head"), "pull request head")
        base_repository = _repository_name(
            base.get("repo"), "pull request base repository"
        )
        head_repository = _repository_name(
            head.get("repo"), "pull request head repository"
        )
        user = _mapping(value.get("user"), "pull request author")
        actual_number = _positive_int(value.get("number"), "pull request number")
        state = _required_string(value.get("state"), "pull request state")
        draft = value.get("draft")
        if not isinstance(draft, bool):
            raise RequestCiError("pull request draft state is malformed")
        return PullRequestSnapshot(
            number=actual_number,
            state=state,
            draft=draft,
            base_repository=base_repository,
            base_ref=_required_string(base.get("ref"), "pull request base ref"),
            base_sha=_required_string(base.get("sha"), "pull request base SHA"),
            head_repository=head_repository,
            head_ref=_required_string(head.get("ref"), "pull request head ref"),
            head_sha=_required_string(head.get("sha"), "pull request head SHA"),
            author=_required_string(user.get("login"), "pull request author login"),
        )

    def workflow_runs(self, repository: str, head_sha: str) -> list[dict[str, object]]:
        value = self._object(
            "GET",
            f"repos/{repository}/actions/workflows/build.yaml/runs",
            query={
                "event": "pull_request_target",
                "head_sha": head_sha,
                "per_page": "100",
            },
        )
        total_count = value.get("total_count")
        if (
            not isinstance(total_count, int)
            or isinstance(total_count, bool)
            or total_count < 0
        ):
            raise RequestCiError("workflow runs count is malformed")
        runs = _list(value.get("workflow_runs"), "workflow runs response")
        if total_count != len(runs):
            raise RequestCiError("workflow runs response was unexpectedly truncated")
        return [_mapping(run, "workflow run") for run in runs]

    def run_jobs(
        self, repository: str, run_id: int, attempt: int
    ) -> list[dict[str, object]]:
        value = self._object(
            "GET",
            f"repos/{repository}/actions/runs/{run_id}/attempts/{attempt}/jobs",
            query={"filter": "all", "per_page": "100"},
        )
        total_count = value.get("total_count")
        jobs = _list(value.get("jobs"), "workflow jobs response")
        if (
            not isinstance(total_count, int)
            or isinstance(total_count, bool)
            or total_count < 0
            or total_count != len(jobs)
        ):
            raise RequestCiError("workflow jobs response was unexpectedly truncated")
        return [_mapping(job, "workflow job") for job in jobs]

    def issue_comments(self, repository: str, number: int) -> list[IssueComment]:
        values = self._paginated_list(f"repos/{repository}/issues/{number}/comments")
        return [
            self._issue_comment(_mapping(value, "issue comment"), repository, number)
            for value in values
        ]

    def _issue_comment(
        self, value: dict[str, object], repository: str, number: int
    ) -> IssueComment:
        raw_user = value.get("user")
        if raw_user is None:
            author = ""
            author_type = ""
        else:
            user = _mapping(raw_user, "issue comment author")
            author = _required_string(user.get("login"), "issue comment author login")
            author_type = _required_string(
                user.get("type"), "issue comment author type"
            )
        issue_url = _required_string(value.get("issue_url"), "issue comment URL")
        expected_issue_url = f"{self._api_url}/repos/{repository}/issues/{number}"
        if issue_url != expected_issue_url:
            raise RequestCiError("issue comment belongs to an unexpected issue")
        return IssueComment(
            comment_id=_positive_int(value.get("id"), "issue comment ID"),
            body=_required_string(value.get("body"), "issue comment body"),
            author=author,
            author_type=author_type,
            issue_url=issue_url,
        )

    def create_issue_comment(
        self, repository: str, number: int, body: str
    ) -> IssueComment:
        value = self._object(
            "POST", f"repos/{repository}/issues/{number}/comments", body={"body": body}
        )
        return self._issue_comment(value, repository, number)

    def update_issue_comment(
        self, repository: str, number: int, comment_id: int, body: str
    ) -> IssueComment:
        value = self._object(
            "PATCH",
            f"repos/{repository}/issues/comments/{comment_id}",
            body={"body": body},
        )
        return self._issue_comment(value, repository, number)


def _load_event(path: Path) -> object:
    try:
        with path.open(encoding="utf-8") as stream:
            return json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise RequestCiError("unable to read the issue_comment event") from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-path", type=Path, required=True)
    args = parser.parse_args(argv)
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    api = GitHubApi(
        os.environ.get("GITHUB_TOKEN", ""),
        os.environ.get("GITHUB_API_URL", "https://api.github.com"),
    )
    result = handle_request(_load_event(args.event_path), repository, api)
    if result is None:
        print(
            "Ignoring issue comment: it is not an exact /request-ci on a pull request"
        )
    else:
        print(
            f"{result.action.capitalize()} CI request comment {result.comment_id} "
            f"for {result.head_sha}; maintainer rerun target is {result.run_id}"
        )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RequestCiError as exc:
        print(f"CI request failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from None
