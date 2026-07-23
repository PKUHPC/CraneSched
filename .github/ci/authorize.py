#!/usr/bin/env python3
"""Authorize a trusted CI dispatch and resolve all source refs to commit SHAs."""

from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional


SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_REQUEST_MARKER_PREFIX = "<!-- cranesched-ci-request:"
_REQUEST_MARKER_RE = re.compile(
    r"^<!-- cranesched-ci-request:(\{[^\r\n]*\}) -->$", re.MULTILINE
)
_REQUEST_MARKER_KEYS = {
    "head_sha",
    "pr_number",
    "request_comment_id",
    "run_id",
    "schema",
}


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
    manual_backend_ref: str = "master"
    manual_frontend_ref: str = "master"


PermissionLookup = Callable[[str, str], dict[str, object]]
CommitResolver = Callable[[str, str], Optional[str]]


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


@dataclass(frozen=True)
class IssueComment:
    """The comment identity and content needed for a fork CI request."""

    comment_id: int
    body: str
    author_login: str
    author_type: str


@dataclass(frozen=True)
class RequestAttestation:
    """A canonical request marker emitted by the trusted hosted workflow."""

    pr_number: int
    head_sha: str
    request_comment_id: int
    run_id: int


IssueCommentsLookup = Callable[[str, int], tuple[IssueComment, ...]]
IssueCommentLookup = Callable[[str, int, int], Optional[IssueComment]]
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


def _required_pr_number(value: str) -> int:
    if re.fullmatch(r"[1-9][0-9]*", value) is None:
        raise AuthorizationError("pull request number is invalid")
    return int(value)


def _required_positive_integer(value: str, label: str) -> int:
    if re.fullmatch(r"[1-9][0-9]*", value) is None:
        raise AuthorizationError(f"{label} is invalid")
    return int(value)


def _parse_request_attestation(body: str) -> RequestAttestation | None:
    if body.count(_REQUEST_MARKER_PREFIX) != 1:
        return None
    matches = _REQUEST_MARKER_RE.findall(body)
    if len(matches) != 1:
        return None
    raw_payload = matches[0]
    try:
        payload = json.loads(raw_payload)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict) or set(payload) != _REQUEST_MARKER_KEYS:
        return None
    if json.dumps(payload, sort_keys=True, separators=(",", ":")) != raw_payload:
        return None
    schema = payload.get("schema")
    if not isinstance(schema, int) or isinstance(schema, bool) or schema != 1:
        return None
    integer_fields = ("pr_number", "request_comment_id", "run_id")
    if any(
        not isinstance(payload.get(field), int)
        or isinstance(payload.get(field), bool)
        or payload[field] <= 0
        for field in integer_fields
    ):
        return None
    head_sha = payload.get("head_sha")
    if not isinstance(head_sha, str) or SHA_RE.fullmatch(head_sha) is None:
        return None
    return RequestAttestation(
        pr_number=payload["pr_number"],
        head_sha=head_sha,
        request_comment_id=payload["request_comment_id"],
        run_id=payload["run_id"],
    )


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


def _validate_fork_request(
    context: DispatchContext,
    number: int,
    permission_lookup: PermissionLookup,
    pull_request_lookup: PullRequestLookup | None,
    issue_comments_lookup: IssueCommentsLookup | None,
    issue_comment_lookup: IssueCommentLookup | None,
) -> None:
    run_attempt = _required_positive_integer(
        context.run_attempt, "workflow run attempt"
    )
    if run_attempt == 1:
        raise AuthorizationError(
            "fork pull requests require approval: the pull request author must "
            "comment exactly `/request-ci`, then a maintainer must use `Re-run all "
            "jobs` on the workflow run linked by github-actions[bot]"
        )

    if (
        pull_request_lookup is None
        or issue_comments_lookup is None
        or issue_comment_lookup is None
    ):
        raise AuthorizationError("fork CI request validation is unavailable")

    run_id = _required_positive_integer(context.run_id, "workflow run ID")
    _require_maintainer(context, permission_lookup)

    snapshot = pull_request_lookup(context.repository, number)
    _validate_pr_snapshot(context, number, snapshot)

    attestations: list[RequestAttestation] = []
    for comment in issue_comments_lookup(context.repository, number):
        if (
            comment.author_login != "github-actions[bot]"
            or comment.author_type != "Bot"
        ):
            continue
        attestation = _parse_request_attestation(comment.body)
        if (
            attestation is not None
            and attestation.pr_number == number
            and attestation.head_sha == context.pr_head_sha
            and attestation.run_id == run_id
        ):
            attestations.append(attestation)

    if not attestations:
        raise AuthorizationError(
            "no valid `/request-ci` attestation matches this workflow run and PR head"
        )
    if len(attestations) != 1:
        raise AuthorizationError(
            "multiple `/request-ci` attestations match this workflow run and PR head"
        )

    attestation = attestations[0]
    request = issue_comment_lookup(
        context.repository, number, attestation.request_comment_id
    )
    if request is None:
        raise AuthorizationError("the original `/request-ci` comment no longer exists")
    if request.comment_id != attestation.request_comment_id:
        raise AuthorizationError("GitHub returned an unexpected request comment")
    if request.body != "/request-ci":
        raise AuthorizationError("the original `/request-ci` comment was edited")
    if request.author_login != snapshot.author_login:
        raise AuthorizationError(
            "the original `/request-ci` comment was not created by the PR author"
        )


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
) -> str:
    attempts = len(retry_delays) + 1
    pending_reason = "GitHub has not computed the proposed merge"

    for attempt in range(attempts):
        snapshot = pull_request_lookup(context.repository, number)
        _validate_pr_snapshot(context, number, snapshot)
        if snapshot.base_sha != context.event_sha:
            pending_reason = (
                "the current pull request base has not converged to the trusted "
                "workflow revision"
            )
        elif snapshot.mergeable is False:
            raise AuthorizationError("pull request has no mergeable proposed result")
        elif snapshot.mergeable is not True or not SHA_RE.fullmatch(
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
                elif parents != (context.event_sha, context.pr_head_sha):
                    raise AuthorizationError(
                        "proposed merge parents do not match the trusted base and "
                        "event head"
                    )
                else:
                    final_snapshot = pull_request_lookup(context.repository, number)
                    _validate_pr_snapshot(context, number, final_snapshot)
                    if final_snapshot.base_sha != context.event_sha:
                        raise AuthorizationError(
                            "pull request base changed during authorization; wait for "
                            "the new run"
                        )
                    elif final_snapshot.mergeable is False:
                        raise AuthorizationError(
                            "pull request has no mergeable proposed result"
                        )
                    elif (
                        final_snapshot.mergeable is True
                        and final_snapshot.merge_commit_sha == merge_sha
                    ):
                        final_ref_sha = merge_ref_resolver(context.repository, number)
                        if final_ref_sha == merge_sha:
                            return merge_sha
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
    issue_comments_lookup: IssueCommentsLookup | None = None,
    issue_comment_lookup: IssueCommentLookup | None = None,
    *,
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
    if context.event_name == "pull_request_target":
        pr_number = _validate_pr_event(context)
        if context.pr_head_repository == context.repository:
            _require_maintainer(context, permission_lookup)
        else:
            _validate_fork_request(
                context,
                pr_number,
                permission_lookup,
                pull_request_lookup,
                issue_comments_lookup,
                issue_comment_lookup,
            )
        if (
            pull_request_lookup is None
            or merge_ref_resolver is None
            or commit_parents_resolver is None
        ):
            raise AuthorizationError("pull request merge validation is unavailable")
        backend_sha = context.pr_head_sha
        routing_sha = _resolve_pr_merge(
            context,
            pr_number,
            pull_request_lookup,
            merge_ref_resolver,
            commit_parents_resolver,
            sleeper=sleeper,
            retry_delays=retry_delays,
        )
        pr_base_sha = context.event_sha
        pr_head_sha = context.pr_head_sha
        pr_merge_sha = routing_sha
        frontend_ref = context.pr_head_ref
        frontend_sha = commit_resolver(context.frontend_repository, frontend_ref)
        if frontend_sha is None:
            frontend_ref = "master"
            frontend_sha = _required_commit(
                commit_resolver,
                context.frontend_repository,
                frontend_ref,
                "FrontEnd",
            )
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
        routing_sha = backend_sha
    elif context.event_name in {"push", "schedule"}:
        backend_sha = context.event_sha
        routing_sha = backend_sha
        frontend_ref = "master"
        frontend_sha = _required_commit(
            commit_resolver,
            context.frontend_repository,
            frontend_ref,
            "FrontEnd",
        )
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

    @staticmethod
    def _parse_issue_comment(value: object) -> IssueComment:
        if not isinstance(value, dict):
            raise AuthorizationError("GitHub issue comment response is malformed")
        user = value.get("user")
        if user is None:
            author_login = ""
            author_type = ""
        elif isinstance(user, dict):
            author_login = user.get("login")
            author_type = user.get("type")
        else:
            raise AuthorizationError("GitHub issue comment response is malformed")
        comment_id = value.get("id")
        body = value.get("body")
        if (
            not isinstance(comment_id, int)
            or isinstance(comment_id, bool)
            or comment_id <= 0
            or not isinstance(body, str)
            or not isinstance(author_login, str)
            or not isinstance(author_type, str)
        ):
            raise AuthorizationError("GitHub issue comment response is malformed")
        return IssueComment(
            comment_id=comment_id,
            body=body,
            author_login=author_login,
            author_type=author_type,
        )

    def issue_comments(self, repository: str, number: int) -> tuple[IssueComment, ...]:
        comments: list[IssueComment] = []
        for page in range(1, 11):
            values = self._get_list(
                f"repos/{repository}/issues/{number}/comments?per_page=100&page={page}"
            )
            comments.extend(self._parse_issue_comment(value) for value in values)
            if len(values) < 100:
                return tuple(comments)
        raise AuthorizationError(
            "pull request has too many comments to validate `/request-ci` safely"
        )

    def issue_comment(
        self, repository: str, number: int, comment_id: int
    ) -> IssueComment | None:
        value = self._get(
            f"repos/{repository}/issues/comments/{comment_id}",
            missing_statuses=frozenset({404}),
        )
        if value is None:
            return None
        expected_issue_url = (
            f"https://api.github.com/repos/{repository}/issues/{number}"
        )
        if value.get("issue_url") != expected_issue_url:
            raise AuthorizationError(
                "GitHub returned a request comment from an unexpected pull request"
            )
        return self._parse_issue_comment(value)

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
        pr_number=os.environ.get("PR_NUMBER", ""),
        pr_base_repository=os.environ.get("PR_BASE_REPOSITORY", ""),
        pr_base_ref=os.environ.get("PR_BASE_REF", ""),
        pr_base_sha=os.environ.get("PR_BASE_SHA", ""),
        pr_merge_sha=os.environ.get("PR_MERGE_SHA", ""),
        pr_head_repository=os.environ.get("PR_HEAD_REPOSITORY", ""),
        pr_head_ref=os.environ.get("PR_HEAD_REF", ""),
        pr_head_sha=os.environ.get("PR_HEAD_SHA", ""),
        run_id=os.environ.get("GITHUB_RUN_ID", ""),
        run_attempt=os.environ.get("GITHUB_RUN_ATTEMPT", ""),
        manual_backend_ref=os.environ.get("MANUAL_BACKEND_REF", "master"),
        manual_frontend_ref=os.environ.get("MANUAL_FRONTEND_REF", "master"),
    )


def main() -> int:
    output_path = Path(os.environ["GITHUB_OUTPUT"])
    with output_path.open("a", encoding="utf-8") as output:
        output.write("authorized=false\n")
    api = GitHubApi(os.environ.get("GH_TOKEN", ""))
    result = authorize_dispatch(
        _context_from_environment(),
        api.permission,
        api.commit,
        api.pull_request,
        api.merge_ref,
        api.commit_parents,
        api.issue_comments,
        api.issue_comment,
    )
    with output_path.open("a", encoding="utf-8") as output:
        for name in (
            "authorized",
            "backend_sha",
            "routing_sha",
            "pr_base_sha",
            "pr_head_sha",
            "pr_merge_sha",
            "frontend_ref",
            "frontend_sha",
            "workflow_sha",
        ):
            output.write(f"{name}={result[name]}\n")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AuthorizationError as exc:
        print(f"CI authorization failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from None
