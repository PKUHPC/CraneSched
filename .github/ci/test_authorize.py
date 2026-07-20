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
MERGE_SHA = "d" * 40


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
        "pr_number": "935",
        "pr_base_repository": "PKUHPC/CraneSched",
        "pr_base_ref": "master",
        "pr_base_sha": WORKFLOW_SHA,
        "pr_merge_sha": MERGE_SHA,
        "pr_head_repository": "PKUHPC/CraneSched",
        "pr_head_ref": "feature/test",
        "pr_head_sha": BACKEND_SHA,
    }
    values.update(overrides)
    return authorize.DispatchContext(**values)


def _resolver(repository: str, ref: str) -> str | None:
    if repository == "PKUHPC/CraneSched" and ref in {
        BACKEND_SHA,
        MERGE_SHA,
        WORKFLOW_SHA,
        "master",
    }:
        return BACKEND_SHA if ref == "master" else ref
    if repository == "PKUHPC/CraneSched-FrontEnd" and ref in {"feature/test", "master"}:
        return FRONTEND_SHA
    return None


def _snapshot(**overrides) -> authorize.PullRequestSnapshot:
    values = {
        "number": 935,
        "state": "open",
        "draft": False,
        "base_repository": "PKUHPC/CraneSched",
        "base_ref": "master",
        "base_sha": WORKFLOW_SHA,
        "head_repository": "PKUHPC/CraneSched",
        "head_ref": "feature/test",
        "head_sha": BACKEND_SHA,
        "merge_commit_sha": MERGE_SHA,
        "mergeable": True,
    }
    values.update(overrides)
    return authorize.PullRequestSnapshot(**values)


def _pull_request(_repository: str, _number: int) -> authorize.PullRequestSnapshot:
    return _snapshot()


def _merge_ref(_repository: str, _number: int) -> str:
    return MERGE_SHA


def _commit_parents(_repository: str, _revision: str) -> tuple[str, ...]:
    return (WORKFLOW_SHA, BACKEND_SHA)


def _authorize(
    context: authorize.DispatchContext | None = None,
    *,
    permission: str = "maintain",
    resolver=_resolver,
    pull_request_lookup=_pull_request,
    merge_ref_resolver=_merge_ref,
    commit_parents_resolver=_commit_parents,
    retry_delays: tuple[float, ...] = (),
) -> dict[str, str]:
    return authorize.authorize_dispatch(
        context or _context(),
        lambda _repo, _actor: {"permission": permission},
        resolver,
        pull_request_lookup,
        merge_ref_resolver,
        commit_parents_resolver,
        sleeper=lambda _delay: None,
        retry_delays=retry_delays,
    )


class AuthorizationPolicyTest(unittest.TestCase):
    def test_same_repository_maintainer_pr_is_authorized(self) -> None:
        result = _authorize()
        self.assertEqual(result["backend_sha"], BACKEND_SHA)
        self.assertEqual(result["routing_sha"], MERGE_SHA)
        self.assertEqual(result["pr_base_sha"], WORKFLOW_SHA)
        self.assertEqual(result["pr_head_sha"], BACKEND_SHA)
        self.assertEqual(result["pr_merge_sha"], MERGE_SHA)
        self.assertEqual(result["frontend_sha"], FRONTEND_SHA)
        self.assertEqual(result["workflow_sha"], WORKFLOW_SHA)

    def test_missing_event_merge_sha_uses_verified_current_merge(self) -> None:
        result = _authorize(_context(pr_merge_sha=""))

        self.assertEqual(result["routing_sha"], MERGE_SHA)
        self.assertEqual(result["pr_merge_sha"], MERGE_SHA)

    def test_waits_for_github_to_compute_mergeability(self) -> None:
        snapshots = iter(
            [
                _snapshot(mergeable=None, merge_commit_sha=""),
                _snapshot(),
                _snapshot(),
            ]
        )
        delays: list[float] = []

        result = authorize.authorize_dispatch(
            _context(pr_merge_sha=""),
            lambda _repo, _actor: {"permission": "admin"},
            _resolver,
            lambda _repo, _number: next(snapshots),
            _merge_ref,
            _commit_parents,
            sleeper=delays.append,
            retry_delays=(0.25,),
        )

        self.assertEqual(result["routing_sha"], MERGE_SHA)
        self.assertEqual(delays, [0.25])

    def test_mergeability_polling_is_bounded(self) -> None:
        calls = 0

        def pending(_repository: str, _number: int):
            nonlocal calls
            calls += 1
            return _snapshot(mergeable=None, merge_commit_sha="")

        with self.assertRaisesRegex(
            authorize.AuthorizationError, "unable to resolve a stable proposed merge"
        ):
            _authorize(
                _context(pr_merge_sha=""),
                pull_request_lookup=pending,
                retry_delays=(0.0, 0.0),
            )

        self.assertEqual(calls, 3)

    def test_event_base_must_match_trusted_workflow_revision(self) -> None:
        with self.assertRaisesRegex(
            authorize.AuthorizationError, "trusted workflow revision"
        ):
            _authorize(_context(pr_base_sha="e" * 40))

    def test_current_pr_snapshot_must_match_event(self) -> None:
        with self.assertRaisesRegex(authorize.AuthorizationError, "changed"):
            _authorize(
                pull_request_lookup=lambda _repo, _number: _snapshot(
                    head_sha="e" * 40
                )
            )

    def test_unmergeable_pr_is_rejected(self) -> None:
        with self.assertRaisesRegex(authorize.AuthorizationError, "no mergeable"):
            _authorize(
                pull_request_lookup=lambda _repo, _number: _snapshot(
                    mergeable=False, merge_commit_sha=""
                )
            )

    def test_event_merge_must_match_current_merge(self) -> None:
        with self.assertRaisesRegex(authorize.AuthorizationError, "event proposed merge"):
            _authorize(_context(pr_merge_sha="e" * 40))

    def test_merge_ref_must_match_current_merge(self) -> None:
        with self.assertRaisesRegex(authorize.AuthorizationError, "merge ref"):
            _authorize(merge_ref_resolver=lambda _repo, _number: "e" * 40)

    def test_merge_parents_are_exactly_base_then_head(self) -> None:
        for parents in (
            (BACKEND_SHA, WORKFLOW_SHA),
            (WORKFLOW_SHA,),
            (WORKFLOW_SHA, BACKEND_SHA, "e" * 40),
        ):
            with self.subTest(parents=parents), self.assertRaisesRegex(
                authorize.AuthorizationError, "parents"
            ):
                _authorize(
                    commit_parents_resolver=lambda _repo, _sha, value=parents: value
                )

    def test_second_snapshot_detects_authorization_race(self) -> None:
        snapshots = iter([_snapshot(), _snapshot(head_sha="e" * 40)])

        with self.assertRaisesRegex(authorize.AuthorizationError, "changed"):
            _authorize(
                pull_request_lookup=lambda _repo, _number: next(snapshots)
            )

    def test_fork_is_rejected_before_permission_lookup(self) -> None:
        def unexpected_permission(_repo, _actor):
            self.fail("permission lookup must not run for a fork")

        with self.assertRaisesRegex(authorize.AuthorizationError, "Fork|fork"):
            authorize.authorize_dispatch(
                _context(pr_head_repository="external/fork"),
                unexpected_permission,
                _resolver,
                _pull_request,
                _merge_ref,
                _commit_parents,
            )

    def test_non_maintainer_is_rejected(self) -> None:
        with self.assertRaisesRegex(authorize.AuthorizationError, "maintain or admin"):
            _authorize(permission="write")

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
                self.assertEqual(result["routing_sha"], WORKFLOW_SHA)
                self.assertEqual(result["pr_base_sha"], "")
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
                _pull_request,
                _merge_ref,
                _commit_parents,
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
            if repository == "PKUHPC/CraneSched" and ref in {
                BACKEND_SHA,
                MERGE_SHA,
            }:
                return ref
            if repository == "PKUHPC/CraneSched-FrontEnd" and ref == "master":
                return FRONTEND_SHA
            return None

        result = _authorize(
            _context(pr_head_ref="backend-only-branch"),
            permission="admin",
            resolver=resolver,
            pull_request_lookup=lambda _repo, _number: _snapshot(
                head_ref="backend-only-branch"
            ),
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

    def test_pull_request_response_is_parsed_into_snapshot(self) -> None:
        api = authorize.GitHubApi("test-token")
        response = {
            "number": 935,
            "state": "open",
            "draft": False,
            "mergeable": True,
            "merge_commit_sha": MERGE_SHA,
            "base": {
                "ref": "master",
                "sha": WORKFLOW_SHA,
                "repo": {"full_name": "PKUHPC/CraneSched"},
            },
            "head": {
                "ref": "feature/test",
                "sha": BACKEND_SHA,
                "repo": {"full_name": "PKUHPC/CraneSched"},
            },
        }

        with mock.patch.object(api, "_get", return_value=response):
            snapshot = api.pull_request("PKUHPC/CraneSched", 935)

        self.assertEqual(snapshot, _snapshot())

    def test_pull_request_allows_pending_merge_fields(self) -> None:
        api = authorize.GitHubApi("test-token")
        response = {
            "number": 935,
            "state": "open",
            "draft": False,
            "mergeable": None,
            "merge_commit_sha": None,
            "base": {
                "ref": "master",
                "sha": WORKFLOW_SHA,
                "repo": {"full_name": "PKUHPC/CraneSched"},
            },
            "head": {
                "ref": "feature/test",
                "sha": BACKEND_SHA,
                "repo": {"full_name": "PKUHPC/CraneSched"},
            },
        }

        with mock.patch.object(api, "_get", return_value=response):
            snapshot = api.pull_request("PKUHPC/CraneSched", 935)

        self.assertIsNone(snapshot.mergeable)
        self.assertEqual(snapshot.merge_commit_sha, "")

    def test_merge_ref_and_commit_parents_are_parsed(self) -> None:
        api = authorize.GitHubApi("test-token")
        with mock.patch.object(
            api, "_get", return_value={"object": {"sha": MERGE_SHA}}
        ) as get:
            self.assertEqual(api.merge_ref("PKUHPC/CraneSched", 935), MERGE_SHA)
        self.assertEqual(
            get.call_args.args[0],
            "repos/PKUHPC/CraneSched/git/ref/pull/935/merge",
        )
        with mock.patch.object(
            api,
            "_get",
            return_value={
                "parents": [{"sha": WORKFLOW_SHA}, {"sha": BACKEND_SHA}]
            },
        ):
            self.assertEqual(
                api.commit_parents("PKUHPC/CraneSched", MERGE_SHA),
                (WORKFLOW_SHA, BACKEND_SHA),
            )


if __name__ == "__main__":
    unittest.main()
