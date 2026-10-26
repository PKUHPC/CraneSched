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
FORK_HEAD_SHA = "e" * 40
RUN_ID = 29794463501
FORK_REPOSITORY = "external/CraneSched"
FORK_AUTHOR = "fork-author"


def _context(**overrides):
    values = {
        "event_name": "workflow_run",
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
        "run_id": str(RUN_ID),
        "run_attempt": "1",
    }
    values.update(overrides)
    return authorize.DispatchContext(**values)


def _resolver(repository: str, ref: str) -> str | None:
    if repository == "PKUHPC/CraneSched" and ref in {
        BACKEND_SHA,
        FORK_HEAD_SHA,
        MERGE_SHA,
        WORKFLOW_SHA,
        "master",
    }:
        return BACKEND_SHA if ref == "master" else ref
    if repository == "PKUHPC/CraneSched-FrontEnd" and ref in {
        FRONTEND_SHA,
        "feature/test",
        "master",
    }:
        return FRONTEND_SHA
    return None


def _branch_resolver(repository: str, branch: str) -> str | None:
    if repository == "PKUHPC/CraneSched-FrontEnd" and branch in {
        "feature/test",
        "master",
    }:
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
        "author_login": "contributor",
        "merge_commit_sha": MERGE_SHA,
        "mergeable": True,
        "mergeable_state": "blocked",
    }
    values.update(overrides)
    return authorize.PullRequestSnapshot(**values)


def _pull_request(_repository: str, _number: int) -> authorize.PullRequestSnapshot:
    return _snapshot()


def _merge_ref(_repository: str, _number: int) -> str:
    return MERGE_SHA


def _commit_parents(_repository: str, _revision: str) -> tuple[str, ...]:
    return (WORKFLOW_SHA, BACKEND_SHA)


def _fork_context(**overrides) -> authorize.DispatchContext:
    values = {
        "pr_head_repository": FORK_REPOSITORY,
        "pr_head_sha": FORK_HEAD_SHA,
        "run_attempt": "2",
    }
    values.update(overrides)
    return _context(**values)


def _fork_snapshot(**overrides) -> authorize.PullRequestSnapshot:
    values = {
        "head_repository": FORK_REPOSITORY,
        "head_sha": FORK_HEAD_SHA,
        "author_login": FORK_AUTHOR,
    }
    values.update(overrides)
    return _snapshot(**values)


def _fork_pull_request(_repository: str, _number: int) -> authorize.PullRequestSnapshot:
    return _fork_snapshot()


def _fork_commit_parents(_repository: str, _revision: str) -> tuple[str, ...]:
    return (WORKFLOW_SHA, FORK_HEAD_SHA)


def _authorize(
    context: authorize.DispatchContext | None = None,
    *,
    permission: str = "maintain",
    resolver=_resolver,
    pull_request_lookup=_pull_request,
    merge_ref_resolver=_merge_ref,
    commit_parents_resolver=_commit_parents,
    branch_resolver=_branch_resolver,
    retry_delays: tuple[float, ...] = (),
) -> dict[str, str]:
    resolved_context = context or _context()
    permissions = {resolved_context.triggering_actor: permission}
    return authorize.authorize_dispatch(
        resolved_context,
        lambda _repo, actor: {"permission": permissions.get(actor, "none")},
        resolver,
        pull_request_lookup,
        merge_ref_resolver,
        commit_parents_resolver,
        branch_resolver=branch_resolver,
        sleeper=lambda _delay: None,
        retry_delays=retry_delays,
    )


class AuthorizationPolicyTest(unittest.TestCase):
    def test_workflow_run_identity_is_loaded_from_environment(self) -> None:
        with mock.patch.dict(
            authorize.os.environ,
            {"GITHUB_RUN_ID": str(RUN_ID), "GITHUB_RUN_ATTEMPT": "3"},
            clear=True,
        ):
            context = authorize._context_from_environment()

        self.assertEqual(context.run_id, str(RUN_ID))
        self.assertEqual(context.run_attempt, "3")

    def test_same_repository_maintainer_pr_is_authorized(self) -> None:
        result = _authorize()
        self.assertEqual(result["backend_sha"], BACKEND_SHA)
        self.assertEqual(result["routing_sha"], MERGE_SHA)
        self.assertEqual(result["pr_base_sha"], WORKFLOW_SHA)
        self.assertEqual(result["pr_head_sha"], BACKEND_SHA)
        self.assertEqual(result["pr_merge_sha"], MERGE_SHA)
        self.assertEqual(result["frontend_sha"], FRONTEND_SHA)
        self.assertEqual(result["frontend_ref"], "feature/test")
        self.assertEqual(result["frontend_source"], "matching_branch")
        self.assertEqual(result["workflow_sha"], WORKFLOW_SHA)

    def test_missing_event_merge_sha_uses_verified_current_merge(self) -> None:
        result = _authorize(_context(pr_merge_sha=""))

        self.assertEqual(result["routing_sha"], MERGE_SHA)
        self.assertEqual(result["pr_merge_sha"], MERGE_SHA)

    def test_stale_event_base_and_merge_hints_use_current_api_snapshot(self) -> None:
        result = _authorize(_context(pr_base_sha="e" * 40, pr_merge_sha="f" * 40))

        self.assertEqual(result["backend_sha"], BACKEND_SHA)
        self.assertEqual(result["routing_sha"], MERGE_SHA)
        self.assertEqual(result["pr_base_sha"], WORKFLOW_SHA)
        self.assertEqual(result["pr_head_sha"], BACKEND_SHA)
        self.assertEqual(result["pr_merge_sha"], MERGE_SHA)

    def test_stale_api_base_is_allowed_when_merge_parents_are_trusted(self) -> None:
        result = _authorize(
            _context(pr_base_sha="e" * 40, pr_merge_sha="f" * 40),
            pull_request_lookup=lambda _repo, _number: _snapshot(base_sha="e" * 40),
        )

        self.assertEqual(result["pr_base_sha"], WORKFLOW_SHA)
        self.assertEqual(result["routing_sha"], MERGE_SHA)

    def test_live_proposed_base_may_advance_beyond_event_base(self) -> None:
        proposed_base = "f" * 40

        result = _authorize(
            _context(pr_base_sha="e" * 40),
            commit_parents_resolver=lambda _repo, _sha: (
                proposed_base,
                BACKEND_SHA,
            ),
        )

        self.assertEqual(result["pr_base_sha"], proposed_base)
        self.assertEqual(result["pr_head_sha"], BACKEND_SHA)
        self.assertEqual(result["routing_sha"], MERGE_SHA)

    def test_stale_api_base_does_not_override_live_proposed_base(self) -> None:
        result = _authorize(
            pull_request_lookup=lambda _repo, _number: _snapshot(base_sha="e" * 40),
            commit_parents_resolver=lambda _repo, _sha: (
                "e" * 40,
                BACKEND_SHA,
            ),
        )

        self.assertEqual(result["pr_base_sha"], "e" * 40)
        self.assertEqual(result["pr_head_sha"], BACKEND_SHA)

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
            branch_resolver=_branch_resolver,
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

    def test_event_base_and_merge_hints_must_be_full_shas(self) -> None:
        for field in ("pr_base_sha", "pr_merge_sha"):
            with (
                self.subTest(field=field),
                self.assertRaisesRegex(authorize.AuthorizationError, "full commit SHA"),
            ):
                _authorize(_context(**{field: "invalid"}))

    def test_current_pr_snapshot_must_match_event(self) -> None:
        with self.assertRaisesRegex(authorize.AuthorizationError, "changed"):
            _authorize(
                pull_request_lookup=lambda _repo, _number: _snapshot(head_sha="e" * 40)
            )

    def test_unmergeable_pr_is_rejected(self) -> None:
        with self.assertRaisesRegex(authorize.AuthorizationError, "no mergeable"):
            _authorize(
                pull_request_lookup=lambda _repo, _number: _snapshot(
                    mergeable=False, merge_commit_sha=""
                )
            )

    def test_merge_ref_must_match_current_merge(self) -> None:
        with self.assertRaisesRegex(authorize.AuthorizationError, "merge ref"):
            _authorize(merge_ref_resolver=lambda _repo, _number: "e" * 40)

    def test_merge_parents_are_exactly_base_then_head(self) -> None:
        for parents in (
            (BACKEND_SHA, WORKFLOW_SHA),
            (WORKFLOW_SHA,),
            (WORKFLOW_SHA, BACKEND_SHA, "e" * 40),
        ):
            with (
                self.subTest(parents=parents),
                self.assertRaisesRegex(authorize.AuthorizationError, "parents"),
            ):
                _authorize(
                    commit_parents_resolver=lambda _repo, _sha, value=parents: value
                )

    def test_second_snapshot_detects_authorization_race(self) -> None:
        snapshots = iter([_snapshot(), _snapshot(head_sha="e" * 40)])

        with self.assertRaisesRegex(authorize.AuthorizationError, "changed"):
            _authorize(pull_request_lookup=lambda _repo, _number: next(snapshots))

    def test_second_snapshot_allows_api_base_hint_to_change(self) -> None:
        snapshots = iter([_snapshot(), _snapshot(base_sha="e" * 40)])

        result = _authorize(pull_request_lookup=lambda _repo, _number: next(snapshots))

        self.assertEqual(result["pr_base_sha"], WORKFLOW_SHA)
        self.assertEqual(result["routing_sha"], MERGE_SHA)

    def test_mergeable_blocked_pr_is_authorized(self) -> None:
        result = _authorize(
            pull_request_lookup=lambda _repo, _number: _snapshot(
                mergeable=True, mergeable_state="blocked"
            )
        )

        self.assertEqual(result["routing_sha"], MERGE_SHA)

    def test_native_approved_pr_does_not_require_a_repository_role(self) -> None:
        for permission in ("none", "read", "triage", "write", "maintain", "admin"):
            with self.subTest(permission=permission):
                result = _authorize(permission=permission)
                self.assertEqual(result["backend_sha"], BACKEND_SHA)

    def test_native_approved_fork_uses_exact_head_without_comments(self) -> None:
        def unexpected_permission(_repo, _actor):
            self.fail("PR authorization must use GitHub native approval")

        result = authorize.authorize_dispatch(
            _fork_context(run_attempt="1"),
            unexpected_permission,
            _resolver,
            _fork_pull_request,
            _merge_ref,
            _fork_commit_parents,
            branch_resolver=_branch_resolver,
        )
        self.assertEqual(result["backend_sha"], FORK_HEAD_SHA)
        self.assertEqual(result["frontend_source"], "matching_branch")

    def test_old_pr_target_entry_is_rejected(self) -> None:
        with self.assertRaisesRegex(authorize.AuthorizationError, "unsupported"):
            _authorize(_context(event_name="pull_request_target"))

    def test_push_and_schedule_lock_backend_to_trusted_workflow_sha(self) -> None:
        def unexpected_permission(_repo, _actor):
            self.fail("permission lookup must not run for push or schedule")

        def resolver(repository: str, ref: str) -> str | None:
            if repository == "PKUHPC/CraneSched" and ref == WORKFLOW_SHA:
                return WORKFLOW_SHA
            if repository == "PKUHPC/CraneSched-FrontEnd" and ref == FRONTEND_SHA:
                return FRONTEND_SHA
            return None

        for event_name in ("push", "schedule"):
            with self.subTest(event_name=event_name):
                result = authorize.authorize_dispatch(
                    _context(event_name=event_name, event_sha=WORKFLOW_SHA),
                    unexpected_permission,
                    resolver,
                    branch_resolver=_branch_resolver,
                )
                self.assertEqual(result["backend_sha"], WORKFLOW_SHA)
                self.assertEqual(result["routing_sha"], WORKFLOW_SHA)
                self.assertEqual(result["pr_base_sha"], "")
                self.assertEqual(result["frontend_ref"], "master")
                self.assertEqual(result["frontend_source"], "master_default")
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

    def test_manual_dispatch_keeps_explicit_commitish_resolution(self) -> None:
        result = _authorize(
            _context(
                event_name="workflow_dispatch",
                manual_backend_ref="master",
                manual_frontend_ref="feature/test",
            )
        )

        self.assertEqual(result["frontend_ref"], "feature/test")
        self.assertEqual(result["frontend_sha"], FRONTEND_SHA)
        self.assertEqual(result["frontend_source"], "manual_ref")

    def test_missing_matching_frontend_branch_falls_back_to_master(self) -> None:
        def resolver(repository: str, ref: str) -> str | None:
            if repository == "PKUHPC/CraneSched" and ref in {
                BACKEND_SHA,
                MERGE_SHA,
            }:
                return ref
            if repository == "PKUHPC/CraneSched-FrontEnd" and ref == FRONTEND_SHA:
                return FRONTEND_SHA
            return None

        def branch_resolver(repository: str, branch: str) -> str | None:
            if repository == "PKUHPC/CraneSched-FrontEnd" and branch == "master":
                return FRONTEND_SHA
            return None

        result = _authorize(
            _context(pr_head_ref="backend-only-branch"),
            permission="admin",
            resolver=resolver,
            branch_resolver=branch_resolver,
            pull_request_lookup=lambda _repo, _number: _snapshot(
                head_ref="backend-only-branch"
            ),
        )
        self.assertEqual(result["frontend_ref"], "master")
        self.assertEqual(result["frontend_sha"], FRONTEND_SHA)
        self.assertEqual(result["frontend_source"], "master_fallback")

    def test_same_name_tag_does_not_count_as_a_matching_branch(self) -> None:
        commit_queries: list[tuple[str, str]] = []

        def resolver(repository: str, ref: str) -> str | None:
            commit_queries.append((repository, ref))
            if repository == "PKUHPC/CraneSched" and ref in {
                BACKEND_SHA,
                MERGE_SHA,
            }:
                return ref
            if repository == "PKUHPC/CraneSched-FrontEnd" and ref in {
                "tag-only",
                FRONTEND_SHA,
            }:
                return FRONTEND_SHA
            return None

        result = _authorize(
            _context(pr_head_ref="tag-only"),
            resolver=resolver,
            branch_resolver=lambda _repository, branch: (
                FRONTEND_SHA if branch == "master" else None
            ),
            pull_request_lookup=lambda _repo, _number: _snapshot(head_ref="tag-only"),
        )

        self.assertEqual(result["frontend_ref"], "master")
        self.assertEqual(result["frontend_source"], "master_fallback")
        self.assertNotIn(("PKUHPC/CraneSched-FrontEnd", "tag-only"), commit_queries)

    def test_slash_branch_is_matched_exactly(self) -> None:
        result = _authorize(
            _context(pr_head_ref="feature/nested/test"),
            branch_resolver=lambda repository, branch: (
                FRONTEND_SHA
                if repository == "PKUHPC/CraneSched-FrontEnd"
                and branch in {"feature/nested/test", "master"}
                else None
            ),
            pull_request_lookup=lambda _repo, _number: _snapshot(
                head_ref="feature/nested/test"
            ),
        )

        self.assertEqual(result["frontend_ref"], "feature/nested/test")
        self.assertEqual(result["frontend_source"], "matching_branch")

    def test_missing_frontend_master_fails_closed(self) -> None:
        with self.assertRaisesRegex(authorize.AuthorizationError, "FrontEnd branch"):
            _authorize(
                _context(pr_head_ref="missing"),
                branch_resolver=lambda _repository, _branch: None,
                pull_request_lookup=lambda _repo, _number: _snapshot(
                    head_ref="missing"
                ),
            )

    def test_frontend_sha_must_still_resolve_as_a_commit(self) -> None:
        def resolver(repository: str, ref: str) -> str | None:
            if repository == "PKUHPC/CraneSched" and ref in {
                BACKEND_SHA,
                MERGE_SHA,
            }:
                return ref
            return None

        with self.assertRaisesRegex(authorize.AuthorizationError, "FrontEnd ref"):
            _authorize(resolver=resolver)

    def test_frontend_commit_verification_must_match_selected_sha(self) -> None:
        different_sha = "f" * 40

        def resolver(repository: str, ref: str) -> str | None:
            if repository == "PKUHPC/CraneSched" and ref in {
                BACKEND_SHA,
                MERGE_SHA,
            }:
                return ref
            if repository == "PKUHPC/CraneSched-FrontEnd" and ref == FRONTEND_SHA:
                return different_sha
            return None

        with self.assertRaisesRegex(
            authorize.AuthorizationError, "verification failed"
        ):
            _authorize(resolver=resolver)


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
            with (
                self.subTest(status=status),
                mock.patch.object(
                    authorize.urllib.request,
                    "urlopen",
                    side_effect=self._http_error(status),
                ),
            ):
                self.assertIsNone(api.commit("PKUHPC/CraneSched", "missing/ref"))

    def test_branch_head_uses_exact_encoded_heads_ref(self) -> None:
        api = authorize.GitHubApi("test-token")
        response = {
            "ref": "refs/heads/feature/nested/test",
            "object": {"type": "commit", "sha": FRONTEND_SHA},
        }

        with mock.patch.object(api, "_get", return_value=response) as get:
            revision = api.branch_head(
                "PKUHPC/CraneSched-FrontEnd", "feature/nested/test"
            )

        self.assertEqual(revision, FRONTEND_SHA)
        self.assertEqual(
            get.call_args.args[0],
            "repos/PKUHPC/CraneSched-FrontEnd/git/ref/heads/feature%2Fnested%2Ftest",
        )
        self.assertEqual(get.call_args.kwargs["missing_statuses"], frozenset({404}))

    def test_only_branch_404_is_treated_as_missing(self) -> None:
        api = authorize.GitHubApi("test-token")
        with mock.patch.object(
            authorize.urllib.request,
            "urlopen",
            side_effect=self._http_error(404),
        ):
            self.assertIsNone(api.branch_head("PKUHPC/CraneSched-FrontEnd", "missing"))

        for status in (403, 422, 429, 500):
            with (
                self.subTest(status=status),
                mock.patch.object(
                    authorize.urllib.request,
                    "urlopen",
                    side_effect=self._http_error(status),
                ),
                self.assertRaisesRegex(authorize.AuthorizationError, f"HTTP {status}"),
            ):
                api.branch_head("PKUHPC/CraneSched-FrontEnd", "missing")

    def test_branch_timeout_fails_closed(self) -> None:
        api = authorize.GitHubApi("test-token")
        with (
            mock.patch.object(
                authorize.urllib.request,
                "urlopen",
                side_effect=TimeoutError,
            ),
            self.assertRaisesRegex(authorize.AuthorizationError, "request failed"),
        ):
            api.branch_head("PKUHPC/CraneSched-FrontEnd", "feature/test")

    def test_branch_response_must_be_an_exact_commit_ref(self) -> None:
        api = authorize.GitHubApi("test-token")
        malformed = (
            {
                "ref": "refs/heads/Feature/Test",
                "object": {"type": "commit", "sha": FRONTEND_SHA},
            },
            {
                "ref": "refs/tags/feature/test",
                "object": {"type": "commit", "sha": FRONTEND_SHA},
            },
            {
                "ref": "refs/heads/feature/test",
                "object": {"type": "tag", "sha": FRONTEND_SHA},
            },
            {
                "ref": "refs/heads/feature/test",
                "object": {"type": "commit", "sha": "short"},
            },
            {"ref": "refs/heads/feature/test", "object": "invalid"},
        )

        for response in malformed:
            with (
                self.subTest(response=response),
                mock.patch.object(api, "_get", return_value=response),
                self.assertRaisesRegex(authorize.AuthorizationError, "malformed"),
            ):
                api.branch_head("PKUHPC/CraneSched-FrontEnd", "feature/test")

    def test_permission_lookup_keeps_422_fail_closed(self) -> None:
        api = authorize.GitHubApi("test-token")
        with (
            mock.patch.object(
                authorize.urllib.request,
                "urlopen",
                side_effect=self._http_error(422),
            ),
            self.assertRaisesRegex(authorize.AuthorizationError, "HTTP 422"),
        ):
            api.permission("PKUHPC/CraneSched", "maintainer")

    def test_api_timeout_fails_closed(self) -> None:
        api = authorize.GitHubApi("test-token")
        with (
            mock.patch.object(
                authorize.urllib.request,
                "urlopen",
                side_effect=TimeoutError,
            ),
            self.assertRaisesRegex(authorize.AuthorizationError, "request failed"),
        ):
            api.permission("PKUHPC/CraneSched", "maintainer")

    def test_pull_request_response_is_parsed_into_snapshot(self) -> None:
        api = authorize.GitHubApi("test-token")
        response = {
            "number": 935,
            "state": "open",
            "draft": False,
            "mergeable": True,
            "mergeable_state": "blocked",
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
            "user": {"login": "contributor"},
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
            "mergeable_state": "unknown",
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
            "user": {"login": "contributor"},
        }

        with mock.patch.object(api, "_get", return_value=response):
            snapshot = api.pull_request("PKUHPC/CraneSched", 935)

        self.assertIsNone(snapshot.mergeable)
        self.assertEqual(snapshot.mergeable_state, "unknown")
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
            return_value={"parents": [{"sha": WORKFLOW_SHA}, {"sha": BACKEND_SHA}]},
        ):
            self.assertEqual(
                api.commit_parents("PKUHPC/CraneSched", MERGE_SHA),
                (WORKFLOW_SHA, BACKEND_SHA),
            )


if __name__ == "__main__":
    unittest.main()
