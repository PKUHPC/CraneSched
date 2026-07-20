from __future__ import annotations

import importlib.util
import io
import sys
import unittest
import urllib.error
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).with_name("authorize.py")
SPEC = importlib.util.spec_from_file_location("cranesched_ci_authorize", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
authorize = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = authorize
SPEC.loader.exec_module(authorize)


BACKEND_SHA = "a" * 40
FRONTEND_SHA = "b" * 40
WORKFLOW_SHA = "c" * 40


def _context(**overrides):
    values = {
        "event_name": "pull_request_target",
        "repository": "PKUHPC/CraneSched",
        "workflow_ref": (
            "PKUHPC/CraneSched/.github/workflows/build.yaml@refs/heads/master"
        ),
        "triggering_actor": "maintainer",
        "event_sha": WORKFLOW_SHA,
        "frontend_repository": "PKUHPC/CraneSched-FrontEnd",
        "pr_head_repository": "PKUHPC/CraneSched",
        "pr_head_ref": "feature/test",
        "pr_head_sha": BACKEND_SHA,
    }
    values.update(overrides)
    return authorize.DispatchContext(**values)


def _resolver(repository: str, ref: str) -> str | None:
    if repository == "PKUHPC/CraneSched" and ref in {BACKEND_SHA, "master"}:
        return BACKEND_SHA
    if repository == "PKUHPC/CraneSched-FrontEnd" and ref in {"feature/test", "master"}:
        return FRONTEND_SHA
    return None


class AuthorizationPolicyTest(unittest.TestCase):
    def test_same_repository_maintainer_pr_is_authorized(self) -> None:
        result = authorize.authorize_dispatch(
            _context(), lambda _repo, _actor: {"permission": "maintain"}, _resolver
        )
        self.assertEqual(result["backend_sha"], BACKEND_SHA)
        self.assertEqual(result["frontend_sha"], FRONTEND_SHA)
        self.assertEqual(result["workflow_sha"], WORKFLOW_SHA)

    def test_fork_is_rejected_before_permission_lookup(self) -> None:
        def unexpected_permission(_repo, _actor):
            self.fail("permission lookup must not run for a fork")

        with self.assertRaisesRegex(authorize.AuthorizationError, "Fork|fork"):
            authorize.authorize_dispatch(
                _context(pr_head_repository="external/fork"),
                unexpected_permission,
                _resolver,
            )

    def test_non_maintainer_is_rejected(self) -> None:
        with self.assertRaisesRegex(authorize.AuthorizationError, "maintain or admin"):
            authorize.authorize_dispatch(
                _context(), lambda _repo, _actor: {"permission": "write"}, _resolver
            )

    def test_push_and_schedule_lock_backend_to_trusted_workflow_sha(self) -> None:
        def unexpected_permission(_repo, _actor):
            self.fail("permission lookup must not run for push or schedule")

        def resolver(repository: str, ref: str) -> str | None:
            if repository == "PKUHPC/CraneSched" and ref == WORKFLOW_SHA:
                return WORKFLOW_SHA
            if repository == "PKUHPC/CraneSched-FrontEnd" and ref == "master":
                return FRONTEND_SHA
            return None

        for event_name in ("push", "schedule"):
            with self.subTest(event_name=event_name):
                result = authorize.authorize_dispatch(
                    _context(event_name=event_name, event_sha=WORKFLOW_SHA),
                    unexpected_permission,
                    resolver,
                )
                self.assertEqual(result["backend_sha"], WORKFLOW_SHA)
                self.assertEqual(result["workflow_sha"], WORKFLOW_SHA)

    def test_non_default_workflow_ref_is_rejected(self) -> None:
        with self.assertRaisesRegex(authorize.AuthorizationError, "non-default"):
            authorize.authorize_dispatch(
                _context(
                    workflow_ref=(
                        "PKUHPC/CraneSched/.github/workflows/build.yaml@refs/heads/feature"
                    )
                ),
                lambda _repo, _actor: {"permission": "admin"},
                _resolver,
            )

    def test_manual_dispatch_also_requires_maintainer(self) -> None:
        with self.assertRaisesRegex(authorize.AuthorizationError, "maintain or admin"):
            authorize.authorize_dispatch(
                _context(event_name="workflow_dispatch"),
                lambda _repo, _actor: {"permission": "write"},
                _resolver,
            )

    def test_missing_matching_frontend_branch_falls_back_to_master(self) -> None:
        def resolver(repository: str, ref: str) -> str | None:
            if repository == "PKUHPC/CraneSched" and ref == BACKEND_SHA:
                return BACKEND_SHA
            if repository == "PKUHPC/CraneSched-FrontEnd" and ref == "master":
                return FRONTEND_SHA
            return None

        result = authorize.authorize_dispatch(
            _context(pr_head_ref="backend-only-branch"),
            lambda _repo, _actor: {"permission": "admin"},
            resolver,
        )
        self.assertEqual(result["frontend_ref"], "master")
        self.assertEqual(result["frontend_sha"], FRONTEND_SHA)


class GitHubApiTest(unittest.TestCase):
    @staticmethod
    def _http_error(status: int) -> urllib.error.HTTPError:
        return urllib.error.HTTPError(
            "https://api.github.com/test",
            status,
            "test error",
            hdrs=None,
            fp=io.BytesIO(b"{}"),
        )

    def test_missing_commit_accepts_github_404_and_422_responses(self) -> None:
        api = authorize.GitHubApi("test-token")
        for status in (404, 422):
            with self.subTest(status=status), mock.patch.object(
                authorize.urllib.request,
                "urlopen",
                side_effect=self._http_error(status),
            ):
                self.assertIsNone(api.commit("PKUHPC/CraneSched", "missing/ref"))

    def test_permission_lookup_keeps_422_fail_closed(self) -> None:
        api = authorize.GitHubApi("test-token")
        with mock.patch.object(
            authorize.urllib.request,
            "urlopen",
            side_effect=self._http_error(422),
        ), self.assertRaisesRegex(authorize.AuthorizationError, "HTTP 422"):
            api.permission("PKUHPC/CraneSched", "maintainer")


if __name__ == "__main__":
    unittest.main()
