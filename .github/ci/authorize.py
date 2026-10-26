#!/usr/bin/env python3
"""Authorize a trusted CI dispatch and resolve all source refs to commit SHAs."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable, Optional


SHA_RE = re.compile(r"^[0-9a-f]{40}$")
PR_GATE_NAME = "PR CI Gate"
PR_GATE_PATH = ".github/workflows/pr-ci.yaml"


class AuthorizationError(RuntimeError):
    pass


@dataclass(frozen=True)
class DispatchContext:
    event_name: str
    repository: str
    workflow_ref: str
    triggering_actor: str
    event_sha: str
    frontend_repository: str
    pr_number: str = ""
    pr_base_repository: str = ""
    pr_base_ref: str = ""
    pr_base_sha: str = ""
    pr_merge_sha: str = ""
    pr_head_repository: str = ""
    pr_head_ref: str = ""
    pr_head_sha: str = ""
    run_id: str = ""
    run_attempt: str = ""
    source_run_id: str = ""
    source_run_attempt: str = ""
    manual_backend_ref: str = "master"
    manual_frontend_ref: str = "master"


PermissionLookup = Callable[[str, str], dict[str, object]]
CommitResolver = Callable[[str, str], Optional[str]]
BranchResolver = Callable[[str, str], Optional[str]]


@dataclass(frozen=True)
class PullRequestSnapshot:
    """The immutable PR fields used to bind a proposed merge result."""

    number: int
    state: str
    draft: bool
    base_repository: str
    base_ref: str
    base_sha: str
    head_repository: str
    head_ref: str
    head_sha: str
    author_login: str
    merge_commit_sha: str
    mergeable: bool | None
    mergeable_state: str


PullRequestLookup = Callable[[str, int], PullRequestSnapshot]
MergeRefResolver = Callable[[str, int], Optional[str]]
CommitParentsResolver = Callable[[str, str], Optional[tuple[str, ...]]]


Sleeper = Callable[[float], None]

_MERGE_RETRY_DELAYS = (0.5, 1.0, 2.0, 4.0, 8.0)


def _has_maintain_permission(value: dict[str, object]) -> bool:
    if value.get("permission") in {"maintain", "admin"}:
        return True
    user = value.get("user")
    permissions = user.get("permissions") if isinstance(user, dict) else None
    return isinstance(permissions, dict) and (
        permissions.get("maintain") is True or permissions.get("admin") is True
    )


def _require_maintainer(
    context: DispatchContext, permission_lookup: PermissionLookup
) -> None:
    if not _has_maintain_permission(
        permission_lookup(context.repository, context.triggering_actor)
    ):
        raise AuthorizationError(
            "only a triggering actor with maintain or admin permission may dispatch tests"
        )


def _required_commit(
    resolver: CommitResolver, repository: str, ref: str, label: str
) -> str:
    revision = resolver(repository, ref)
    if revision is None or not SHA_RE.fullmatch(revision):
        raise AuthorizationError(f"{label} ref did not resolve to a full commit SHA")
    return revision


def _required_branch(
    resolver: BranchResolver, repository: str, branch: str, label: str
) -> str:
    revision = resolver(repository, branch)
    if revision is None or not SHA_RE.fullmatch(revision):
        raise AuthorizationError(f"{label} branch did not resolve to a full commit SHA")
    return revision


def _required_pr_number(value: str) -> int:
    if re.fullmatch(r"[1-9][0-9]*", value) is None:
        raise AuthorizationError("pull request number is invalid")
    return int(value)


def _required_positive_integer(value: str, label: str) -> int:
    if re.fullmatch(r"[1-9][0-9]*", value) is None:
        raise AuthorizationError(f"{label} is invalid")
    return int(value)


def _validate_pr_event(context: DispatchContext) -> int:
    if context.pr_base_repository != context.repository:
        raise AuthorizationError("pull request base repository is unexpected")
    if context.pr_base_ref != "master":
        raise AuthorizationError("pull request base branch is not master")
    if not SHA_RE.fullmatch(context.pr_base_sha):
        raise AuthorizationError("pull request base is not a full commit SHA")
    if not SHA_RE.fullmatch(context.pr_head_sha):
        raise AuthorizationError("pull request head is not a full commit SHA")
    if not context.pr_head_ref or any(
        character in context.pr_head_ref for character in "\r\n"
    ):
        raise AuthorizationError("pull request head ref is invalid")
    if not context.pr_head_repository or any(
        character in context.pr_head_repository for character in "\r\n"
    ):
        raise AuthorizationError("pull request head repository is invalid")
    if context.pr_merge_sha and not SHA_RE.fullmatch(context.pr_merge_sha):
        raise AuthorizationError("event proposed merge is not a full commit SHA")
    return _required_pr_number(context.pr_number)


def _validate_pr_snapshot(
    context: DispatchContext, number: int, snapshot: PullRequestSnapshot
) -> None:
    if snapshot.number != number:
        raise AuthorizationError("GitHub returned an unexpected pull request")
    if snapshot.state != "open":
        raise AuthorizationError("pull request is no longer open")
    if snapshot.draft:
        raise AuthorizationError("draft pull requests cannot dispatch tests")
    if (
        snapshot.base_repository != context.pr_base_repository
        or snapshot.base_ref != context.pr_base_ref
        or snapshot.head_repository != context.pr_head_repository
        or snapshot.head_ref != context.pr_head_ref
        or snapshot.head_sha != context.pr_head_sha
    ):
        raise AuthorizationError(
            "pull request changed after this workflow event; wait for the new run"
        )


def _resolve_pr_merge(
    context: DispatchContext,
    number: int,
    pull_request_lookup: PullRequestLookup,
    merge_ref_resolver: MergeRefResolver,
    commit_parents_resolver: CommitParentsResolver,
    *,
    sleeper: Sleeper,
    retry_delays: tuple[float, ...] = _MERGE_RETRY_DELAYS,
) -> tuple[str, str]:
    attempts = len(retry_delays) + 1
    pending_reason = "GitHub has not computed the proposed merge"

    for attempt in range(attempts):
        snapshot = pull_request_lookup(context.repository, number)
        _validate_pr_snapshot(context, number, snapshot)
        # GitHub can retain the base SHA from the PR's last branch update. The
        # current merge ref and its ordered parents prove the routing base.
        if snapshot.mergeable is False:
            raise AuthorizationError("pull request has no mergeable proposed result")
        if snapshot.mergeable is not True or not SHA_RE.fullmatch(
            snapshot.merge_commit_sha
        ):
            pending_reason = "GitHub has not computed the proposed merge"
        else:
            merge_sha = snapshot.merge_commit_sha
            merge_ref_sha = merge_ref_resolver(context.repository, number)
            if merge_ref_sha != merge_sha:
                pending_reason = "the pull request merge ref is not synchronized"
            else:
                parents = commit_parents_resolver(context.repository, merge_sha)
                if parents is None:
                    pending_reason = "the proposed merge commit is not available"
                elif len(parents) != 2 or any(
                    not SHA_RE.fullmatch(parent) for parent in parents
                ):
                    raise AuthorizationError(
                        "proposed merge commit does not have exactly two full-SHA "
                        "parents"
                    )
                elif parents[1] != context.pr_head_sha:
                    raise AuthorizationError(
                        "proposed merge parents: second parent does not match the "
                        "current pull request head"
                    )
                else:
                    final_snapshot = pull_request_lookup(context.repository, number)
                    _validate_pr_snapshot(context, number, final_snapshot)
                    if final_snapshot.mergeable is False:
                        raise AuthorizationError(
                            "pull request has no mergeable proposed result"
                        )
                    elif (
                        final_snapshot.mergeable is True
                        and final_snapshot.merge_commit_sha == merge_sha
                    ):
                        final_ref_sha = merge_ref_resolver(context.repository, number)
                        if final_ref_sha == merge_sha:
                            # The first parent is the base used to construct the
                            # live proposed merge. It may legitimately differ from
                            # the base SHA captured by the original event when the
                            # PR remains mergeable while master advances.
                            return merge_sha, parents[0]
                        pending_reason = (
                            "the pull request merge ref changed during authorization"
                        )
                    else:
                        pending_reason = (
                            "the proposed merge changed during authorization"
                        )

        if attempt < len(retry_delays):
            sleeper(retry_delays[attempt])

    raise AuthorizationError(
        f"unable to resolve a stable proposed merge: {pending_reason}"
    )


def authorize_dispatch(
    context: DispatchContext,
    permission_lookup: PermissionLookup,
    commit_resolver: CommitResolver,
    pull_request_lookup: PullRequestLookup | None = None,
    merge_ref_resolver: MergeRefResolver | None = None,
    commit_parents_resolver: CommitParentsResolver | None = None,
    *,
    branch_resolver: BranchResolver | None = None,
    sleeper: Sleeper = time.sleep,
    retry_delays: tuple[float, ...] = _MERGE_RETRY_DELAYS,
) -> dict[str, str]:
    expected_workflow_ref = (
        f"{context.repository}/.github/workflows/build.yaml@refs/heads/master"
    )
    if context.workflow_ref != expected_workflow_ref:
        raise AuthorizationError(
            "refusing to dispatch a privileged job from a non-default workflow ref"
        )
    if not SHA_RE.fullmatch(context.event_sha):
        raise AuthorizationError("trusted workflow ref is not a full commit SHA")

    pr_base_sha = ""
    pr_head_sha = ""
    pr_merge_sha = ""
    if context.event_name == "workflow_run":
        pr_number = _validate_pr_event(context)
        if (
            pull_request_lookup is None
            or merge_ref_resolver is None
            or commit_parents_resolver is None
        ):
            raise AuthorizationError("pull request merge validation is unavailable")
        backend_sha = context.pr_head_sha
        routing_sha, pr_base_sha = _resolve_pr_merge(
            context,
            pr_number,
            pull_request_lookup,
            merge_ref_resolver,
            commit_parents_resolver,
            sleeper=sleeper,
            retry_delays=retry_delays,
        )
        pr_head_sha = context.pr_head_sha
        pr_merge_sha = routing_sha
        if branch_resolver is None:
            raise AuthorizationError("FrontEnd branch lookup is unavailable")
        frontend_ref = context.pr_head_ref
        frontend_sha = branch_resolver(context.frontend_repository, frontend_ref)
        if frontend_sha is None:
            frontend_ref = "master"
            frontend_sha = _required_branch(
                branch_resolver,
                context.frontend_repository,
                frontend_ref,
                "FrontEnd",
            )
            frontend_source = "master_fallback"
        else:
            if not SHA_RE.fullmatch(frontend_sha):
                raise AuthorizationError(
                    "FrontEnd matching branch did not resolve to a full commit SHA"
                )
            frontend_source = "matching_branch"
    elif context.event_name == "workflow_dispatch":
        _require_maintainer(context, permission_lookup)
        backend_sha = _required_commit(
            commit_resolver,
            context.repository,
            context.manual_backend_ref,
            "Backend",
        )
        frontend_ref = context.manual_frontend_ref
        frontend_sha = _required_commit(
            commit_resolver,
            context.frontend_repository,
            frontend_ref,
            "FrontEnd",
        )
        frontend_source = "manual_ref"
        routing_sha = backend_sha
    elif context.event_name in {"push", "schedule"}:
        backend_sha = context.event_sha
        routing_sha = backend_sha
        if branch_resolver is None:
            raise AuthorizationError("FrontEnd branch lookup is unavailable")
        frontend_ref = "master"
        frontend_sha = _required_branch(
            branch_resolver,
            context.frontend_repository,
            frontend_ref,
            "FrontEnd",
        )
        frontend_source = "master_default"
    else:
        raise AuthorizationError(f"unsupported dispatch event: {context.event_name}")

    if not SHA_RE.fullmatch(backend_sha):
        raise AuthorizationError("Backend ref did not resolve to a full commit SHA")
    verified_backend = _required_commit(
        commit_resolver, context.repository, backend_sha, "Backend"
    )
    if verified_backend != backend_sha:
        raise AuthorizationError("Backend commit verification failed")
    verified_routing = _required_commit(
        commit_resolver, context.repository, routing_sha, "Routing"
    )
    if verified_routing != routing_sha:
        raise AuthorizationError("Routing commit verification failed")
    if not SHA_RE.fullmatch(frontend_sha):
        raise AuthorizationError("FrontEnd ref did not resolve to a full commit SHA")
    verified_frontend = _required_commit(
        commit_resolver, context.frontend_repository, frontend_sha, "FrontEnd"
    )
    if verified_frontend != frontend_sha:
        raise AuthorizationError("FrontEnd commit verification failed")
    if any(character in frontend_ref for character in "\r\n"):
        raise AuthorizationError("FrontEnd ref contains an invalid output character")

    return {
        "authorized": "true",
        "backend_sha": backend_sha,
        "routing_sha": routing_sha,
        "pr_base_sha": pr_base_sha,
        "pr_head_sha": pr_head_sha,
        "pr_merge_sha": pr_merge_sha,
        "frontend_ref": frontend_ref,
        "frontend_sha": frontend_sha,
        "frontend_source": frontend_source,
        "workflow_sha": context.event_sha,
    }


class GitHubApi:
    def __init__(self, token: str) -> None:
        if not token:
            raise AuthorizationError(
                "GitHub token is unavailable to the hosted authorize job"
            )
        self._token = token

    def _request_json(
        self, path: str, *, missing_statuses: frozenset[int] = frozenset()
    ) -> object | None:
        request = urllib.request.Request(
            f"https://api.github.com/{path}",
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self._token}",
                "User-Agent": "CraneSched-trusted-CI-dispatcher",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                value = json.load(response)
        except urllib.error.HTTPError as exc:
            if exc.code in missing_statuses:
                return None
            raise AuthorizationError(
                f"GitHub API request failed with HTTP {exc.code}"
            ) from None
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise AuthorizationError("GitHub API request failed") from exc
        return value

    def _get(
        self, path: str, *, missing_statuses: frozenset[int] = frozenset()
    ) -> dict[str, object] | None:
        value = self._request_json(path, missing_statuses=missing_statuses)
        if value is None:
            return None
        if not isinstance(value, dict):
            raise AuthorizationError("GitHub API returned a non-object response")
        return value

    def _get_list(self, path: str) -> list[object]:
        value = self._request_json(path)
        if not isinstance(value, list):
            raise AuthorizationError("GitHub API returned a non-array response")
        return value

    def approved_gate_pr(self, context: DispatchContext) -> PullRequestSnapshot:
        """Resolve a completed native-approved gate using only GitHub API data."""
        run_id = _required_positive_integer(context.source_run_id, "source run ID")
        attempt = _required_positive_integer(
            context.source_run_attempt, "source run attempt"
        )
        run = self._get(f"repos/{context.repository}/actions/runs/{run_id}")
        workflow = self._get(f"repos/{context.repository}/actions/workflows/pr-ci.yaml")
        if run is None or workflow is None:
            raise AuthorizationError("PR gate metadata is unavailable")
        workflow_id = workflow.get("id")
        if (
            not isinstance(workflow_id, int)
            or isinstance(workflow_id, bool)
            or workflow_id <= 0
            or workflow.get("path") != PR_GATE_PATH
            or workflow.get("state") != "active"
            or type(run.get("id")) is not int
            or run["id"] != run_id
            or type(run.get("run_attempt")) is not int
            or run["run_attempt"] != attempt
            or type(run.get("workflow_id")) is not int
            or run["workflow_id"] != workflow_id
            or run.get("name") != PR_GATE_NAME
            or run.get("path") != PR_GATE_PATH
            or run.get("event") != "pull_request"
            or run.get("status") != "completed"
            or run.get("conclusion") != "success"
        ):
            raise AuthorizationError("source is not a successful current PR gate run")
        repository = run.get("repository")
        head_repository = run.get("head_repository")
        if (
            not isinstance(repository, dict)
            or repository.get("full_name") != context.repository
            or not isinstance(head_repository, dict)
        ):
            raise AuthorizationError("PR gate repository is unexpected")
        head_repo = head_repository.get("full_name")
        head_ref = run.get("head_branch")
        head_sha = run.get("head_sha")
        if (
            not isinstance(head_repo, str)
            or re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", head_repo) is None
            or not isinstance(head_ref, str)
            or not head_ref
            or any(
                ord(character) < 32 or ord(character) == 127 for character in head_ref
            )
            or not isinstance(head_sha, str)
            or SHA_RE.fullmatch(head_sha) is None
        ):
            raise AuthorizationError("PR gate head identity is invalid")

        jobs = self._get(
            f"repos/{context.repository}/actions/runs/{run_id}/attempts/{attempt}/jobs?per_page=100"
        )
        if (
            jobs is None
            or type(jobs.get("total_count")) is not int
            or jobs["total_count"] != 1
        ):
            raise AuthorizationError("PR gate must contain its one hosted request job")
        values = jobs.get("jobs")
        if (
            not isinstance(values, list)
            or len(values) != 1
            or not isinstance(values[0], dict)
        ):
            raise AuthorizationError("PR gate jobs response is malformed")
        job = values[0]
        if (
            job.get("name") != "Request system tests"
            or job.get("status") != "completed"
            or job.get("conclusion") != "success"
            or job.get("labels") != ["ubuntu-latest"]
            or type(job.get("runner_id")) is not int
            or job["runner_id"] <= 0
        ):
            raise AuthorizationError(
                "PR gate request job did not execute successfully on a hosted runner"
            )

        # GitHub commonly leaves workflow_run.pull_requests empty for forks.
        # Query by owner/branch, then independently verify repository and SHA.
        numbers: list[int] = []
        for page in range(1, 11):
            query = urllib.parse.urlencode(
                {
                    "state": "open",
                    "base": "master",
                    "head": f"{head_repo.split('/')[0]}:{head_ref}",
                    "per_page": 100,
                    "page": page,
                }
            )
            values = self._get_list(f"repos/{context.repository}/pulls?{query}")
            for value in values:
                if not isinstance(value, dict):
                    raise AuthorizationError("GitHub pull request list is malformed")
                head = value.get("head")
                base = value.get("base")
                if not isinstance(head, dict) or not isinstance(base, dict):
                    raise AuthorizationError("GitHub pull request list is malformed")
                head_repo_value = head.get("repo")
                base_repo_value = base.get("repo")
                if not isinstance(head_repo_value, dict) or not isinstance(
                    base_repo_value, dict
                ):
                    raise AuthorizationError("GitHub pull request list is malformed")
                if (
                    head_repo_value.get("full_name") == head_repo
                    and head.get("ref") == head_ref
                    and head.get("sha") == head_sha
                    and base_repo_value.get("full_name") == context.repository
                    and base.get("ref") == "master"
                    and value.get("state") == "open"
                ):
                    number = value.get("number")
                    if type(number) is not int or number <= 0:
                        raise AuthorizationError("pull request number is invalid")
                    numbers.append(number)
            if len(values) < 100:
                break
        else:
            raise AuthorizationError("PR lookup exceeded the pagination safety limit")
        if len(numbers) != 1:
            raise AuthorizationError("PR gate must match exactly one current open PR")
        snapshot = self.pull_request(context.repository, numbers[0])
        expected = replace(
            context,
            pr_number=str(numbers[0]),
            pr_base_repository=context.repository,
            pr_base_ref="master",
            pr_base_sha=snapshot.base_sha,
            pr_head_repository=head_repo,
            pr_head_ref=head_ref,
            pr_head_sha=head_sha,
            pr_merge_sha=snapshot.merge_commit_sha,
        )
        _validate_pr_event(expected)
        _validate_pr_snapshot(expected, numbers[0], snapshot)
        return snapshot

    def write_check(
        self, repository: str, body: dict[str, object], check_id: int | None = None
    ) -> dict[str, object]:
        path = f"repos/{repository}/check-runs"
        if check_id is not None:
            path += f"/{check_id}"
        request = urllib.request.Request(
            f"https://api.github.com/{path}",
            data=json.dumps(body).encode("utf-8"),
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json",
                "User-Agent": "CraneSched-trusted-CI-dispatcher",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            method="POST" if check_id is None else "PATCH",
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                if response.status != (201 if check_id is None else 200):
                    raise AuthorizationError("unexpected check API status")
                value = json.load(response)
        except urllib.error.HTTPError as exc:
            raise AuthorizationError(f"check API failed with HTTP {exc.code}") from None
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise AuthorizationError("check API request failed") from exc
        if not isinstance(value, dict):
            raise AuthorizationError("GitHub check response is malformed")
        return value

    def permission(self, repository: str, actor: str) -> dict[str, object]:
        encoded_actor = urllib.parse.quote(actor, safe="")
        value = self._get(
            f"repos/{repository}/collaborators/{encoded_actor}/permission"
        )
        assert value is not None
        return value

    def commit(self, repository: str, ref: str) -> str | None:
        encoded_ref = urllib.parse.quote(ref, safe="")
        value = self._get(
            f"repos/{repository}/commits/{encoded_ref}",
            missing_statuses=frozenset({404, 422}),
        )
        if value is None:
            return None
        revision = value.get("sha")
        return revision if isinstance(revision, str) else None

    def branch_head(self, repository: str, branch: str) -> str | None:
        encoded_branch = urllib.parse.quote(branch, safe="")
        value = self._get(
            f"repos/{repository}/git/ref/heads/{encoded_branch}",
            missing_statuses=frozenset({404}),
        )
        if value is None:
            return None
        target = value.get("object")
        if (
            value.get("ref") != f"refs/heads/{branch}"
            or not isinstance(target, dict)
            or target.get("type") != "commit"
            or not isinstance(target.get("sha"), str)
            or SHA_RE.fullmatch(target["sha"]) is None
        ):
            raise AuthorizationError("GitHub branch response is malformed")
        return target["sha"]

    def pull_request(self, repository: str, number: int) -> PullRequestSnapshot:
        value = self._get(f"repos/{repository}/pulls/{number}")
        assert value is not None
        base = value.get("base")
        head = value.get("head")
        user = value.get("user")
        if (
            not isinstance(base, dict)
            or not isinstance(head, dict)
            or not isinstance(user, dict)
        ):
            raise AuthorizationError("GitHub pull request response is malformed")
        base_repository = base.get("repo")
        head_repository = head.get("repo")
        if not isinstance(base_repository, dict) or not isinstance(
            head_repository, dict
        ):
            raise AuthorizationError("GitHub pull request response is malformed")

        actual_number = value.get("number")
        state = value.get("state")
        draft = value.get("draft")
        mergeable = value.get("mergeable")
        mergeable_state = value.get("mergeable_state")
        merge_commit_sha = value.get("merge_commit_sha")
        fields = (
            base_repository.get("full_name"),
            base.get("ref"),
            base.get("sha"),
            head_repository.get("full_name"),
            head.get("ref"),
            head.get("sha"),
            user.get("login"),
        )
        if (
            not isinstance(actual_number, int)
            or isinstance(actual_number, bool)
            or not isinstance(state, str)
            or not isinstance(draft, bool)
            or (mergeable is not None and not isinstance(mergeable, bool))
            or not isinstance(mergeable_state, str)
            or (merge_commit_sha is not None and not isinstance(merge_commit_sha, str))
            or any(not isinstance(field, str) for field in fields)
        ):
            raise AuthorizationError("GitHub pull request response is malformed")

        return PullRequestSnapshot(
            number=actual_number,
            state=state,
            draft=draft,
            base_repository=fields[0],
            base_ref=fields[1],
            base_sha=fields[2],
            head_repository=fields[3],
            head_ref=fields[4],
            head_sha=fields[5],
            author_login=fields[6],
            merge_commit_sha=merge_commit_sha or "",
            mergeable=mergeable,
            mergeable_state=mergeable_state,
        )

    def merge_ref(self, repository: str, number: int) -> str | None:
        value = self._get(
            f"repos/{repository}/git/ref/pull/{number}/merge",
            missing_statuses=frozenset({404, 409, 422}),
        )
        if value is None:
            return None
        target = value.get("object")
        if not isinstance(target, dict) or not isinstance(target.get("sha"), str):
            raise AuthorizationError("GitHub merge ref response is malformed")
        return target["sha"]

    def commit_parents(self, repository: str, revision: str) -> tuple[str, ...] | None:
        encoded_revision = urllib.parse.quote(revision, safe="")
        value = self._get(
            f"repos/{repository}/git/commits/{encoded_revision}",
            missing_statuses=frozenset({404, 422}),
        )
        if value is None:
            return None
        parents = value.get("parents")
        if not isinstance(parents, list):
            raise AuthorizationError("GitHub commit response is malformed")
        revisions: list[str] = []
        for parent in parents:
            if not isinstance(parent, dict) or not isinstance(parent.get("sha"), str):
                raise AuthorizationError("GitHub commit response is malformed")
            revisions.append(parent["sha"])
        return tuple(revisions)


def _context_from_environment() -> DispatchContext:
    return DispatchContext(
        event_name=os.environ.get("EVENT_NAME", ""),
        repository=os.environ.get("REPOSITORY", ""),
        workflow_ref=os.environ.get("WORKFLOW_REF", ""),
        triggering_actor=os.environ.get("TRIGGERING_ACTOR", ""),
        event_sha=os.environ.get("EVENT_SHA", ""),
        frontend_repository=os.environ.get(
            "FRONTEND_REPOSITORY", "PKUHPC/CraneSched-FrontEnd"
        ),
        run_id=os.environ.get("GITHUB_RUN_ID", ""),
        run_attempt=os.environ.get("GITHUB_RUN_ATTEMPT", ""),
        source_run_id=os.environ.get("SOURCE_RUN_ID", ""),
        source_run_attempt=os.environ.get("SOURCE_RUN_ATTEMPT", ""),
        manual_backend_ref=os.environ.get("MANUAL_BACKEND_REF", "master"),
        manual_frontend_ref=os.environ.get("MANUAL_FRONTEND_REF", "master"),
    )


def _trusted_context() -> DispatchContext:
    context = _context_from_environment()
    expected = f"{context.repository}/.github/workflows/build.yaml@refs/heads/master"
    if context.workflow_ref != expected or not SHA_RE.fullmatch(context.event_sha):
        raise AuthorizationError("PR controls must be loaded from master")
    return context


def _resolve_gate_context(context: DispatchContext, api: GitHubApi) -> DispatchContext:
    snapshot = api.approved_gate_pr(context)
    return replace(
        context,
        pr_number=str(snapshot.number),
        pr_base_repository=snapshot.base_repository,
        pr_base_ref=snapshot.base_ref,
        pr_base_sha=snapshot.base_sha,
        pr_merge_sha=snapshot.merge_commit_sha,
        pr_head_repository=snapshot.head_repository,
        pr_head_ref=snapshot.head_ref,
        pr_head_sha=snapshot.head_sha,
    )


def _check_identity(context: DispatchContext) -> tuple[str, str]:
    run_id = _required_positive_integer(context.run_id, "workflow run ID")
    attempt = _required_positive_integer(context.run_attempt, "workflow run attempt")
    return (
        f"cranesched-system-test:{run_id}:{attempt}",
        f"https://github.com/{context.repository}/actions/runs/{run_id}/attempts/{attempt}",
    )


def prepare_pr_check(context: DispatchContext, api: GitHubApi) -> dict[str, str]:
    if context.event_name != "workflow_run":
        raise AuthorizationError("PR preparation requires a workflow_run event")
    context = _resolve_gate_context(context, api)
    external_id, url = _check_identity(context)
    body = {
        "name": "test",
        "head_sha": context.pr_head_sha,
        "status": "in_progress",
        "external_id": external_id,
        "details_url": url,
        "output": {
            "title": "System tests requested",
            "summary": (
                "The GitHub-approved PR is being validated and tested. "
                f"[View the trusted workflow]({url})."
            ),
        },
    }
    value = api.write_check(context.repository, body)
    if (
        type(value.get("id")) is not int
        or value["id"] <= 0
        or value.get("head_sha") != context.pr_head_sha
        or value.get("name") != "test"
        or value.get("external_id") != external_id
    ):
        raise AuthorizationError("created PR check has an unexpected identity")
    return {"pr_check_id": str(value["id"]), "pr_head_sha": context.pr_head_sha}


def report_pr_check(
    context: DispatchContext, api: GitHubApi, environment: dict[str, str]
) -> None:
    if context.event_name != "workflow_run":
        raise AuthorizationError("PR reporting requires a workflow_run event")
    check_id = _required_positive_integer(
        environment.get("PR_CHECK_ID", ""), "PR check ID"
    )
    head_sha = environment.get("PR_CHECK_SHA", "")
    if not SHA_RE.fullmatch(head_sha):
        raise AuthorizationError("PR check SHA is invalid")
    external_id, url = _check_identity(context)
    current = api._get(f"repos/{context.repository}/check-runs/{check_id}")
    if (
        current is None
        or current.get("head_sha") != head_sha
        or type(current.get("id")) is not int
        or current["id"] != check_id
        or current.get("name") != "test"
        or current.get("external_id") != external_id
    ):
        raise AuthorizationError("refusing to update an unrelated PR check")
    # GitHub Actions rewrites details_url to /runs/<check_id>. It is display
    # metadata; the identity above binds this check to the exact head and attempt.
    results = (
        environment.get("AUTHORIZE_RESULT"),
        environment.get("SYSTEM_TEST_RESULT"),
    )
    if results == ("success", "success"):
        conclusion = "success"
    elif "cancelled" in results:
        conclusion = "cancelled"
    else:
        conclusion = "failure"
    api.write_check(
        context.repository,
        {
            "status": "completed",
            "conclusion": conclusion,
            "output": {
                "title": f"System tests: {conclusion}",
                "summary": (
                    f"Authorization: {results[0]}; system tests: {results[1]}. "
                    f"[View the trusted workflow]({url}) for results."
                ),
            },
        },
        check_id,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--prepare-pr", action="store_true")
    modes.add_argument("--report-pr-check", action="store_true")
    args = parser.parse_args()
    context = _trusted_context()
    api = GitHubApi(os.environ.get("GH_TOKEN", ""))
    if args.report_pr_check:
        report_pr_check(context, api, dict(os.environ))
        return 0
    output_path = Path(os.environ["GITHUB_OUTPUT"])
    if args.prepare_pr:
        result = prepare_pr_check(context, api)
    else:
        with output_path.open("a", encoding="utf-8") as output:
            output.write("authorized=false\n")
        if context.event_name == "workflow_run":
            context = _resolve_gate_context(context, api)
        result = authorize_dispatch(
            context,
            api.permission,
            api.commit,
            api.pull_request,
            api.merge_ref,
            api.commit_parents,
            branch_resolver=api.branch_head,
        )
    with output_path.open("a", encoding="utf-8") as output:
        for name, value in result.items():
            output.write(f"{name}={value}\n")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AuthorizationError as exc:
        print(f"CI authorization failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from None
