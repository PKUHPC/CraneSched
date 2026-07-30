from __future__ import annotations

import importlib.util
import io
import json
import sys
import unittest
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).with_name("request_ci.py")
SPEC = importlib.util.spec_from_file_location("cranesched_ci_request", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
request_ci = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = request_ci
SPEC.loader.exec_module(request_ci)


BASE_SHA = "a" * 40
HEAD_SHA = "b" * 40
OLD_HEAD_SHA = "c" * 40
RUN_ID = 123456
REQUEST_COMMENT_ID = 456789
PR_NUMBER = 933


def _event(**overrides):
    value = {
        "action": "created",
        "repository": {"full_name": "PKUHPC/CraneSched"},
        "issue": {"number": PR_NUMBER, "pull_request": {"url": "ignored"}},
        "comment": {
            "id": REQUEST_COMMENT_ID,
            "body": "/request-ci",
            "user": {"login": "fork-author", "type": "User"},
        },
    }
    value.update(overrides)
    return value


def _snapshot(**overrides):
    values = {
        "number": PR_NUMBER,
        "state": "open",
        "draft": False,
        "base_repository": "PKUHPC/CraneSched",
        "base_ref": "master",
        "base_sha": BASE_SHA,
        "head_repository": "fork-author/CraneSched",
        "head_ref": "feature/change",
        "head_sha": HEAD_SHA,
        "author": "fork-author",
    }
    values.update(overrides)
    return request_ci.PullRequestSnapshot(**values)


def _run(**overrides):
    values = {
        "id": RUN_ID,
        "name": "CraneTestKit K3s CI",
        "path": ".github/workflows/build.yaml",
        "event": "pull_request_target",
        "run_attempt": 1,
        "status": "completed",
        "conclusion": "failure",
        "repository": {"full_name": "PKUHPC/CraneSched"},
        "head_repository": {"full_name": "fork-author/CraneSched"},
        "head_branch": "feature/change",
        "head_sha": HEAD_SHA,
        "html_url": f"https://github.com/PKUHPC/CraneSched/actions/runs/{RUN_ID}",
        "created_at": "2026-07-22T08:00:00Z",
    }
    values.update(overrides)
    return values


def _jobs():
    return [
        {
            "id": 1,
            "name": "Authorize trusted test dispatch",
            "status": "completed",
            "conclusion": "failure",
            "labels": ["ubuntu-latest"],
            "runner_id": 99,
            "runner_name": "GitHub Actions 99",
            "runner_group_id": 0,
            "runner_group_name": "GitHub Actions",
            "steps": [
                {
                    "name": "Authorize actor and resolve immutable revisions",
                    "status": "completed",
                    "conclusion": "failure",
                }
            ],
        },
        {
            "id": 2,
            "name": "system-test",
            "status": "completed",
            "conclusion": "skipped",
            "labels": ["self-hosted", "cranesystemtest"],
            "runner_id": None,
            "runner_name": None,
            "runner_group_id": None,
            "runner_group_name": None,
            "steps": [],
        },
        {
            "id": 3,
            "name": "test",
            "status": "completed",
            "conclusion": "failure",
            "labels": ["ubuntu-latest"],
            "runner_id": 100,
            "runner_name": "GitHub Actions 100",
            "runner_group_id": 0,
            "runner_group_name": "GitHub Actions",
            "steps": [],
        },
    ]


class FakeApi:
    def __init__(self):
        self.snapshots = [_snapshot()]
        self.runs = [_run()]
        self.jobs = _jobs()
        self.comments = []
        self.permissions = {}
        self.created = []
        self.updated = []
        self.job_requests = []
        self.permission_requests = []

    def permission(self, repository, actor):
        self.permission_requests.append((repository, actor))
        return self.permissions.get(actor, {"permission": "none"})

    def pull_request(self, _repository, _number):
        if len(self.snapshots) > 1:
            return self.snapshots.pop(0)
        return self.snapshots[0]

    def workflow_runs(self, _repository, _head_sha):
        return self.runs

    def run_jobs(self, _repository, _run_id, _attempt):
        self.job_requests.append((_run_id, _attempt))
        return self.jobs

    def issue_comments(self, _repository, _number):
        return self.comments

    def create_issue_comment(self, _repository, number, body):
        self.created.append((number, body))
        return request_ci.IssueComment(
            comment_id=900, body=body, author="github-actions[bot]", author_type="Bot"
        )

    def update_issue_comment(self, _repository, _number, comment_id, body):
        self.updated.append((comment_id, body))
        return request_ci.IssueComment(
            comment_id=comment_id,
            body=body,
            author="github-actions[bot]",
            author_type="Bot",
        )


def _handle(api=None, event=None):
    return request_ci.handle_request(
        event if event is not None else _event(),
        "PKUHPC/CraneSched",
        api if api is not None else FakeApi(),
    )


class RequestProtocolTest(unittest.TestCase):
    def test_creates_attestation_for_exact_fork_author_request(self):
        api = FakeApi()

        result = _handle(api)

        self.assertEqual(result.action, "created")
        self.assertEqual(result.comment_id, 900)
        self.assertEqual(result.head_sha, HEAD_SHA)
        self.assertEqual(result.run_id, RUN_ID)
        self.assertEqual(len(api.created), 1)
        marker = request_ci.parse_marker(api.created[0][1])
        self.assertEqual(
            marker,
            request_ci.Attestation(
                pr_number=PR_NUMBER,
                head_sha=HEAD_SHA,
                request_comment_id=REQUEST_COMMENT_ID,
                run_id=RUN_ID,
            ),
        )
        self.assertIn(f"`{HEAD_SHA}`", api.created[0][1])
        self.assertIn(f"actions/runs/{RUN_ID}", api.created[0][1])
        self.assertIn("Re-run all jobs", api.created[0][1])
        self.assertEqual(api.permission_requests, [])

    def test_current_maintainer_or_admin_can_request_ci(self):
        for permission in ("maintain", "admin"):
            with self.subTest(permission=permission):
                api = FakeApi()
                api.permissions["requester"] = {"permission": permission}
                event = _event(
                    comment={
                        "id": REQUEST_COMMENT_ID,
                        "body": "/request-ci",
                        "user": {"login": "requester", "type": "User"},
                    }
                )

                result = _handle(api, event)

                self.assertEqual(result.action, "created")
                self.assertEqual(
                    api.permission_requests,
                    [
                        ("PKUHPC/CraneSched", "requester"),
                        ("PKUHPC/CraneSched", "requester"),
                    ],
                )

    def test_non_author_without_maintain_permission_is_rejected(self):
        for permission in ("write", "triage", "read", "none"):
            with self.subTest(permission=permission):
                api = FakeApi()
                api.permissions["requester"] = {"permission": permission}
                event = _event(
                    comment={
                        "id": REQUEST_COMMENT_ID,
                        "body": "/request-ci",
                        "user": {"login": "requester", "type": "User"},
                    }
                )

                with self.assertRaisesRegex(
                    request_ci.RequestCiError, "author or a maintain/admin"
                ):
                    _handle(api, event)

                self.assertEqual(api.created, [])

    def test_non_user_and_permission_lookup_failure_are_rejected(self):
        api = FakeApi()
        api.permissions["requester"] = {"permission": "admin"}
        bot_event = _event(
            comment={
                "id": REQUEST_COMMENT_ID,
                "body": "/request-ci",
                "user": {"login": "requester", "type": "Bot"},
            }
        )
        with self.assertRaisesRegex(request_ci.RequestCiError, "only a user"):
            _handle(api, bot_event)
        self.assertEqual(api.permission_requests, [])

        api = FakeApi()
        api.permission = mock.Mock(
            side_effect=request_ci.RequestCiError("GitHub API request failed")
        )
        user_event = _event(
            comment={
                "id": REQUEST_COMMENT_ID,
                "body": "/request-ci",
                "user": {"login": "requester", "type": "User"},
            }
        )
        with self.assertRaisesRegex(request_ci.RequestCiError, "API request failed"):
            _handle(api, user_event)

    def test_maintainer_permission_revoked_while_recording_is_rejected(self):
        api = FakeApi()
        api.permission = mock.Mock(
            side_effect=[{"permission": "maintain"}, {"permission": "write"}]
        )
        event = _event(
            comment={
                "id": REQUEST_COMMENT_ID,
                "body": "/request-ci",
                "user": {"login": "requester", "type": "User"},
            }
        )

        with self.assertRaisesRegex(
            request_ci.RequestCiError, "author or a maintain/admin"
        ):
            _handle(api, event)

        self.assertEqual(api.created, [])
        self.assertEqual(api.updated, [])

    def test_non_created_wrong_command_and_non_pr_comments_are_ignored(self):
        cases = [
            _event(action="edited"),
            _event(
                comment={
                    "id": 1,
                    "body": "/request-ci ",
                    "user": {"login": "fork-author", "type": "User"},
                }
            ),
            _event(issue={"number": PR_NUMBER}),
        ]
        for event in cases:
            with self.subTest(event=event):
                api = FakeApi()
                self.assertIsNone(_handle(api, event))
                self.assertEqual(api.created, [])

    def test_repository_identity_is_fixed(self):
        with self.assertRaisesRegex(request_ci.RequestCiError, "repository"):
            request_ci.handle_request(_event(), "other/repository", FakeApi())
        with self.assertRaisesRegex(request_ci.RequestCiError, "repository"):
            _handle(
                FakeApi(),
                _event(repository={"full_name": "attacker/CraneSched"}),
            )

    def test_pr_must_be_open_ready_fork_targeting_master_and_requested_by_authorized_user(
        self,
    ):
        cases = [
            (_snapshot(state="closed"), "not open"),
            (_snapshot(draft=True), "draft"),
            (_snapshot(base_ref="release"), "does not target"),
            (_snapshot(base_repository="other/repo"), "does not target"),
            (_snapshot(head_repository="PKUHPC/CraneSched"), "same-repository"),
            (_snapshot(author="someone-else"), "author or a maintain/admin"),
        ]
        for snapshot, message in cases:
            with (
                self.subTest(snapshot=snapshot),
                self.assertRaisesRegex(request_ci.RequestCiError, message),
            ):
                api = FakeApi()
                api.snapshots = [snapshot]
                _handle(api)

    def test_invalid_head_identity_is_rejected(self):
        for snapshot in (
            _snapshot(head_sha="not-a-sha"),
            _snapshot(head_ref="bad\nref"),
        ):
            with (
                self.subTest(snapshot=snapshot),
                self.assertRaises(request_ci.RequestCiError),
            ):
                api = FakeApi()
                api.snapshots = [snapshot]
                _handle(api)

    def test_pr_change_during_request_is_rejected_before_comment_write(self):
        api = FakeApi()
        api.snapshots = [_snapshot(), _snapshot(head_sha="d" * 40)]

        with self.assertRaisesRegex(request_ci.RequestCiError, "changed"):
            _handle(api)

        self.assertEqual(api.created, [])
        self.assertEqual(api.updated, [])


class RunSelectionTest(unittest.TestCase):
    def test_selects_newest_current_head_run(self):
        api = FakeApi()
        api.runs = [
            _run(
                id=100,
                html_url="https://github.com/PKUHPC/CraneSched/actions/runs/100",
                created_at="2026-07-22T07:00:00Z",
            ),
            _run(),
            _run(
                id=200,
                head_sha=OLD_HEAD_SHA,
                html_url="https://github.com/PKUHPC/CraneSched/actions/runs/200",
                created_at="2026-07-22T09:00:00Z",
            ),
        ]

        result = _handle(api)

        self.assertEqual(result.run_id, RUN_ID)

    def test_selects_newest_run_and_always_validates_its_first_attempt(self):
        api = FakeApi()
        api.runs = [
            _run(
                id=100,
                html_url="https://github.com/PKUHPC/CraneSched/actions/runs/100",
                created_at="2026-07-22T07:00:00Z",
            ),
            _run(run_attempt=2, created_at="2026-07-22T09:00:00Z"),
        ]

        result = _handle(api)

        self.assertEqual(result.run_id, RUN_ID)
        self.assertEqual(api.job_requests, [(RUN_ID, 1)])

    def test_run_must_match_trusted_workflow_and_current_head(self):
        cases = [
            (_run(name="Other"), "name"),
            (_run(path=".github/workflows/other.yaml"), "path"),
            (_run(event="pull_request"), "event"),
            (_run(run_attempt=True), "attempt"),
            (_run(status="queued", conclusion=None), "finish"),
            (_run(head_repository={"full_name": "other/fork"}), "current"),
            (_run(head_branch="other"), "current"),
            (_run(repository={"full_name": "other/repo"}), "repository"),
        ]
        for run, message in cases:
            with (
                self.subTest(run=run),
                self.assertRaisesRegex(request_ci.RequestCiError, message),
            ):
                api = FakeApi()
                api.runs = [run]
                _handle(api)

    def test_no_current_head_run_is_rejected(self):
        api = FakeApi()
        api.runs = [_run(head_sha=OLD_HEAD_SHA)]
        with self.assertRaisesRegex(request_ci.RequestCiError, "no workflow run"):
            _handle(api)

    def test_duplicate_run_ids_are_rejected(self):
        api = FakeApi()
        api.runs = [_run(), _run()]
        with self.assertRaisesRegex(request_ci.RequestCiError, "duplicate run"):
            _handle(api)


class InitialJobSafetyTest(unittest.TestCase):
    def test_initial_jobs_match_authorize_failure_and_unassigned_system_job(self):
        request_ci.validate_initial_jobs(_jobs())

    def test_authorize_must_fail_on_hosted_runner(self):
        for change in (
            {"conclusion": "success"},
            {"labels": ["self-hosted"]},
        ):
            jobs = _jobs()
            jobs[0].update(change)
            with (
                self.subTest(change=change),
                self.assertRaises(request_ci.RequestCiError),
            ):
                request_ci.validate_initial_jobs(jobs)

    def test_system_job_must_never_have_reached_a_runner(self):
        for change in (
            {"conclusion": "failure"},
            {"runner_id": 88},
            {"runner_name": "cranesystemtest"},
            {"steps": [{"name": "Set up job"}]},
            {"labels": ["self-hosted", "other"]},
            {"name": "test"},
        ):
            jobs = _jobs()
            jobs[1].update(change)
            with (
                self.subTest(change=change),
                self.assertRaises(request_ci.RequestCiError),
            ):
                request_ci.validate_initial_jobs(jobs)

        jobs = _jobs()
        del jobs[1]["runner_id"]
        with self.assertRaises(request_ci.RequestCiError):
            request_ci.validate_initial_jobs(jobs)

    def test_duplicate_authorize_or_system_jobs_are_rejected(self):
        for duplicate in (_jobs()[0], _jobs()[1]):
            jobs = _jobs()
            duplicate = {**duplicate, "id": 99}
            jobs.append(duplicate)
            with (
                self.subTest(name=duplicate["name"]),
                self.assertRaises(request_ci.RequestCiError),
            ):
                request_ci.validate_initial_jobs(jobs)


class AttestationTest(unittest.TestCase):
    def _attestation(self, **overrides):
        values = {
            "pr_number": PR_NUMBER,
            "head_sha": HEAD_SHA,
            "request_comment_id": REQUEST_COMMENT_ID,
            "run_id": RUN_ID,
        }
        values.update(overrides)
        return request_ci.Attestation(**values)

    def test_marker_is_canonical_and_round_trips(self):
        attestation = self._attestation()
        marker = request_ci.canonical_marker(attestation)
        self.assertEqual(
            marker,
            '<!-- cranesched-ci-request:{"head_sha":"'
            + HEAD_SHA
            + '","pr_number":933,"request_comment_id":456789,'
            '"run_id":123456,"schema":1} -->',
        )
        self.assertEqual(request_ci.parse_marker(marker), attestation)

    def test_marker_rejects_noncanonical_or_forged_payloads(self):
        payload = self._attestation().payload()
        cases = [
            "<!-- cranesched-ci-request:not-json -->",
            "<!-- cranesched-ci-request:" + json.dumps(payload) + " -->",
            request_ci.canonical_marker(self._attestation())
            + "\n"
            + request_ci.canonical_marker(self._attestation()),
            request_ci.canonical_marker(self._attestation()).replace(
                '"schema":1', '"extra":1,"schema":1'
            ),
            request_ci.canonical_marker(self._attestation()).replace(
                HEAD_SHA, HEAD_SHA.upper()
            ),
        ]
        for body in cases:
            with self.subTest(body=body), self.assertRaises(request_ci.RequestCiError):
                request_ci.parse_marker(body)

    def test_same_head_updates_single_bot_response_with_new_request(self):
        api = FakeApi()
        old = self._attestation(request_comment_id=111)
        api.comments = [
            request_ci.IssueComment(
                comment_id=700,
                body=request_ci.render_response(
                    old,
                    f"https://github.com/PKUHPC/CraneSched/actions/runs/{RUN_ID}",
                ),
                author="github-actions[bot]",
                author_type="Bot",
            )
        ]

        result = _handle(api)

        self.assertEqual(result.action, "updated")
        self.assertEqual(result.comment_id, 700)
        self.assertEqual(api.created, [])
        self.assertEqual(len(api.updated), 1)
        self.assertEqual(
            request_ci.parse_marker(api.updated[0][1]).request_comment_id,
            REQUEST_COMMENT_ID,
        )

    def test_old_head_response_is_not_reused(self):
        api = FakeApi()
        old = self._attestation(head_sha=OLD_HEAD_SHA)
        api.comments = [
            request_ci.IssueComment(
                comment_id=700,
                body=request_ci.render_response(
                    old,
                    f"https://github.com/PKUHPC/CraneSched/actions/runs/{RUN_ID}",
                ),
                author="github-actions[bot]",
                author_type="Bot",
            )
        ]

        result = _handle(api)

        self.assertEqual(result.action, "created")
        self.assertEqual(api.updated, [])

    def test_non_bot_forgery_is_ignored(self):
        api = FakeApi()
        marker = request_ci.canonical_marker(self._attestation())
        api.comments = [
            request_ci.IssueComment(
                comment_id=700,
                body=marker,
                author="attacker",
                author_type="User",
            )
        ]
        self.assertEqual(_handle(api).action, "created")

    def test_malformed_or_duplicate_bot_attestations_fail_closed(self):
        good = request_ci.IssueComment(
            comment_id=700,
            body=request_ci.canonical_marker(self._attestation()),
            author="github-actions[bot]",
            author_type="Bot",
        )
        cases = [
            [
                request_ci.IssueComment(
                    comment_id=700,
                    body="<!-- cranesched-ci-request:bad -->",
                    author="github-actions[bot]",
                    author_type="Bot",
                )
            ],
            [good, request_ci.IssueComment(**{**good.__dict__, "comment_id": 701})],
        ]
        for comments in cases:
            with (
                self.subTest(comments=comments),
                self.assertRaises(request_ci.RequestCiError),
            ):
                api = FakeApi()
                api.comments = comments
                _handle(api)


class _Response(io.BytesIO):
    def __init__(self, value, status=200):
        super().__init__(json.dumps(value).encode("utf-8"))
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, _type, _value, _traceback):
        self.close()


class GitHubApiValidationTest(unittest.TestCase):
    def setUp(self):
        self.api = request_ci.GitHubApi("test-token")

    def test_http_status_is_exact_for_each_method(self):
        with (
            mock.patch.object(
                request_ci.urllib.request,
                "urlopen",
                return_value=_Response({}, status=202),
            ),
            self.assertRaisesRegex(request_ci.RequestCiError, "HTTP status"),
        ):
            self.api._request("GET", "repos/PKUHPC/CraneSched")

    def test_permission_lookup_encodes_actor_and_returns_exact_response(self):
        response = {"permission": "maintain"}
        with mock.patch.object(self.api, "_object", return_value=response) as get:
            self.assertEqual(
                self.api.permission("PKUHPC/CraneSched", "maintainer/name"),
                response,
            )
        get.assert_called_once_with(
            "GET",
            "repos/PKUHPC/CraneSched/collaborators/maintainer%2Fname/permission",
        )

    def test_run_and_job_counts_cannot_be_boolean_or_truncated(self):
        for method, response in (
            (
                lambda: self.api.workflow_runs("PKUHPC/CraneSched", HEAD_SHA),
                {"total_count": True, "workflow_runs": [{}]},
            ),
            (
                lambda: self.api.run_jobs("PKUHPC/CraneSched", RUN_ID, 1),
                {"total_count": 2, "jobs": [{}]},
            ),
        ):
            with (
                self.subTest(response=response),
                mock.patch.object(self.api, "_object", return_value=response),
                self.assertRaises(request_ci.RequestCiError),
            ):
                method()

    def test_deleted_comment_author_is_ignored_but_issue_binding_is_required(self):
        value = {
            "id": 12,
            "body": "old comment",
            "user": None,
            "issue_url": "https://api.github.com/repos/PKUHPC/CraneSched/issues/933",
        }
        comment = self.api._issue_comment(value, "PKUHPC/CraneSched", 933)
        self.assertEqual(comment.author, "")
        self.assertEqual(comment.author_type, "")

        value["issue_url"] = "https://api.github.com/repos/PKUHPC/CraneSched/issues/934"
        with self.assertRaisesRegex(request_ci.RequestCiError, "unexpected issue"):
            self.api._issue_comment(value, "PKUHPC/CraneSched", 933)

    def test_comment_mutation_response_must_include_exact_issue_identity(self):
        response = {
            "id": 12,
            "body": "response",
            "user": {"login": "github-actions[bot]", "type": "Bot"},
            "issue_url": "https://api.github.com/repos/PKUHPC/CraneSched/issues/934",
        }
        with (
            mock.patch.object(self.api, "_object", return_value=response),
            self.assertRaisesRegex(request_ci.RequestCiError, "unexpected issue"),
        ):
            self.api.create_issue_comment("PKUHPC/CraneSched", 933, "response")


if __name__ == "__main__":
    unittest.main()
