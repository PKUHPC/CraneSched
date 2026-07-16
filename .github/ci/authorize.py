#!/usr/bin/env python3
"""Authorize a trusted CI dispatch and resolve all source refs to commit SHAs."""

from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional


SHA_RE = re.compile(r"^[0-9a-f]{40}$")


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
    pr_head_repository: str = ""
    pr_head_ref: str = ""
    pr_head_sha: str = ""
    manual_backend_ref: str = "master"
    manual_frontend_ref: str = "master"


PermissionLookup = Callable[[str, str], dict[str, object]]
CommitResolver = Callable[[str, str], Optional[str]]


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


def authorize_dispatch(
    context: DispatchContext,
    permission_lookup: PermissionLookup,
    commit_resolver: CommitResolver,
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

    if context.event_name == "pull_request_target":
        if context.pr_head_repository != context.repository:
            raise AuthorizationError(
                "fork pull requests cannot dispatch the self-hosted runner"
            )
        _require_maintainer(context, permission_lookup)
        backend_sha = context.pr_head_sha
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
    elif context.event_name in {"push", "schedule"}:
        backend_sha = context.event_sha
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
    if not SHA_RE.fullmatch(frontend_sha):
        raise AuthorizationError("FrontEnd ref did not resolve to a full commit SHA")
    if any(character in frontend_ref for character in "\r\n"):
        raise AuthorizationError("FrontEnd ref contains an invalid output character")

    return {
        "authorized": "true",
        "backend_sha": backend_sha,
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

    def _get(self, path: str, *, missing_ok: bool = False) -> dict[str, object] | None:
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
            if missing_ok and exc.code == 404:
                return None
            raise AuthorizationError(
                f"GitHub API request failed with HTTP {exc.code}"
            ) from None
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise AuthorizationError("GitHub API request failed") from exc
        if not isinstance(value, dict):
            raise AuthorizationError("GitHub API returned a non-object response")
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
        value = self._get(f"repos/{repository}/commits/{encoded_ref}", missing_ok=True)
        if value is None:
            return None
        revision = value.get("sha")
        return revision if isinstance(revision, str) else None


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
        pr_head_repository=os.environ.get("PR_HEAD_REPOSITORY", ""),
        pr_head_ref=os.environ.get("PR_HEAD_REF", ""),
        pr_head_sha=os.environ.get("PR_HEAD_SHA", ""),
        manual_backend_ref=os.environ.get("MANUAL_BACKEND_REF", "master"),
        manual_frontend_ref=os.environ.get("MANUAL_FRONTEND_REF", "master"),
    )


def main() -> int:
    output_path = Path(os.environ["GITHUB_OUTPUT"])
    with output_path.open("a", encoding="utf-8") as output:
        output.write("authorized=false\n")
    api = GitHubApi(os.environ.get("GH_TOKEN", ""))
    result = authorize_dispatch(_context_from_environment(), api.permission, api.commit)
    with output_path.open("a", encoding="utf-8") as output:
        for name in (
            "authorized",
            "backend_sha",
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
