from __future__ import annotations

import importlib.util
import io
import json
import sys
import tempfile
import unittest
import urllib.error
import urllib.parse
from pathlib import Path
from unittest import mock


SPEC = importlib.util.spec_from_file_location(
    "native_pr_authorize", Path(__file__).with_name("authorize.py")
)
assert SPEC is not None and SPEC.loader is not None
ci = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = ci
SPEC.loader.exec_module(ci)

REPOSITORY = "PKUHPC/CraneSched"
HEAD_REPO = "external/CraneSched"
HEAD_SHA = "a" * 40
MASTER_SHA = "b" * 40
MERGE_SHA = "c" * 40


def context(**changes):
    values = dict(
        event_name="workflow_run",
        repository=REPOSITORY,
        workflow_ref=f"{REPOSITORY}/.github/workflows/build.yaml@refs/heads/master",
        triggering_actor="external-contributor",
        event_sha=MASTER_SHA,
        frontend_repository="PKUHPC/CraneSched-FrontEnd",
        source_run_id="123",
        source_run_attempt="1",
        run_id="456",
        run_attempt="2",
    )
    return ci.DispatchContext(**{**values, **changes})


def snapshot(**changes):
    values = dict(
        number=1033,
        state="open",
        draft=False,
        base_repository=REPOSITORY,
        base_ref="master",
        base_sha=MASTER_SHA,
        head_repository=HEAD_REPO,
        head_ref="feature/nested/test",
        head_sha=HEAD_SHA,
        author_login="external-contributor",
        merge_commit_sha=MERGE_SHA,
        mergeable=True,
        mergeable_state="blocked",
    )
    return ci.PullRequestSnapshot(**{**values, **changes})


def gate_run(**changes):
    return dict(
        id=123,
        run_attempt=1,
        name="PR CI Gate",
        path=ci.PR_GATE_PATH,
        workflow_id=789,
        event="pull_request",
        status="completed",
        conclusion="success",
        repository={"full_name": REPOSITORY},
        head_repository={"full_name": HEAD_REPO},
        head_branch="feature/nested/test",
        head_sha=HEAD_SHA,
        pull_requests=[],
        **changes,
    )


def listed_pr(**changes):
    values = dict(
        number=1033,
        state="open",
        head={
            "repo": {"full_name": HEAD_REPO},
            "ref": "feature/nested/test",
            "sha": HEAD_SHA,
        },
        base={"repo": {"full_name": REPOSITORY}, "ref": "master"},
    )
    return {**values, **changes}


class NativeGateTest(unittest.TestCase):
    def setUp(self):
        self.api = ci.GitHubApi("dummy-token")
        self.run = gate_run()
        self.workflow = {"id": 789, "path": ci.PR_GATE_PATH, "state": "active"}
        self.rows = [listed_pr()]
        self.pr = snapshot()
        self.get = mock.patch.object(self.api, "_get", side_effect=self._get).start()
        self.get_list = mock.patch.object(
            self.api, "_get_list", side_effect=lambda _path: self.rows
        ).start()
        self.pull = mock.patch.object(
            self.api, "pull_request", side_effect=lambda _repo, _number: self.pr
        ).start()
        self.addCleanup(mock.patch.stopall)

    def _get(self, path):
        if path.endswith("/actions/runs/123"):
            return self.run
        if path.endswith("/actions/workflows/pr-ci.yaml"):
            return self.workflow
        self.fail(f"Unexpected API path: {path}")

    def test_fork_with_empty_associations_is_resolved_by_exact_identity(self):
        resolved = ci._resolve_gate_context(context(), self.api)
        self.assertEqual(resolved.pr_number, "1033")
        self.assertEqual(resolved.pr_head_sha, HEAD_SHA)
        path = self.get_list.call_args.args[0]
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(path).query)
        self.assertEqual(query["head"], ["external:feature/nested/test"])
        self.assertEqual(query["state"], ["open"])
        self.assertEqual(query["base"], ["master"])

    def test_no_native_success_means_no_pr_lookup(self):
        for changes in (
            {"conclusion": "failure"},
            {"conclusion": "cancelled"},
            {"conclusion": None, "status": "waiting"},
            {"conclusion": None, "status": "action_required"},
            {"event": "push"},
            {"path": ".github/workflows/format.yaml"},
            {"workflow_id": 790},
            {"workflow_id": True},
            {"id": True},
            {"name": "Fake Gate"},
            {"run_attempt": 2},
            {"repository": {"full_name": "external/CraneSched"}},
            {"head_sha": "not-a-sha"},
            {"head_branch": "feature\nref"},
            {"head_repository": None},
        ):
            with self.subTest(changes=changes):
                self.run = {**gate_run(), **changes}
                self.get_list.reset_mock()
                with self.assertRaises(ci.AuthorizationError):
                    self.api.approved_gate_pr(context())
                self.get_list.assert_not_called()

    def test_workflow_identity_must_be_active_and_exact(self):
        for changes in (
            {"id": True},
            {"state": "disabled_manually"},
            {"path": "fake.yaml"},
        ):
            with self.subTest(changes=changes):
                self.workflow = {
                    "id": 789,
                    "path": ci.PR_GATE_PATH,
                    "state": "active",
                    **changes,
                }
                with self.assertRaises(ci.AuthorizationError):
                    self.api.approved_gate_pr(context())

    def test_old_head_wrong_repository_or_ambiguous_pr_is_rejected(self):
        for rows in (
            [],
            [
                listed_pr(
                    head={
                        "repo": {"full_name": HEAD_REPO},
                        "ref": "feature/nested/test",
                        "sha": "d" * 40,
                    }
                )
            ],
            [
                listed_pr(
                    head={
                        "repo": {"full_name": "other/CraneSched"},
                        "ref": "feature/nested/test",
                        "sha": HEAD_SHA,
                    }
                )
            ],
            [listed_pr(), listed_pr(number=1034)],
        ):
            with self.subTest(rows=rows):
                self.rows = rows
                with self.assertRaises(ci.AuthorizationError):
                    self.api.approved_gate_pr(context())

    def test_pr_is_rechecked_after_listing(self):
        for changes in (
            {"draft": True},
            {"state": "closed"},
            {"head_sha": "e" * 40},
            {"base_ref": "release"},
            {"head_repository": "other/CraneSched"},
        ):
            with self.subTest(changes=changes):
                self.pr = snapshot(**changes)
                with self.assertRaises(ci.AuthorizationError):
                    self.api.approved_gate_pr(context())

    def test_pagination_is_bounded_and_complete(self):
        wrong = listed_pr(head={"repo": {"full_name": "other/CraneSched"}})
        self.get_list.side_effect = [[wrong] * 100, [listed_pr()]]
        self.assertEqual(self.api.approved_gate_pr(context()).number, 1033)
        self.get_list.side_effect = lambda _path: [wrong] * 100
        with self.assertRaisesRegex(ci.AuthorizationError, "pagination"):
            self.api.approved_gate_pr(context())

    def test_api_error_never_falls_back_to_a_trusted_actor(self):
        self.get.side_effect = ci.AuthorizationError("HTTP 403")
        with self.assertRaises(ci.AuthorizationError):
            self.api.approved_gate_pr(context(triggering_actor="maintainer"))

    def test_deleted_or_malformed_repository_in_pr_list_fails_closed(self):
        for invalid in (None, "not-an-object"):
            with self.subTest(invalid=invalid):
                self.rows = [listed_pr(head={"repo": invalid})]
                with self.assertRaises(ci.AuthorizationError):
                    self.api.approved_gate_pr(context())


class PrCheckTest(unittest.TestCase):
    def setUp(self):
        self.api = mock.Mock(spec=ci.GitHubApi)
        self.api.approved_gate_pr.return_value = snapshot()
        external_id, url = ci._check_identity(context())
        self.check = dict(
            id=42,
            name="test",
            head_sha=HEAD_SHA,
            external_id=external_id,
            details_url=url,
        )
        self.api.write_check.return_value = self.check
        self.api._get.return_value = self.check
        self.environment = dict(
            PR_CHECK_ID="42",
            PR_CHECK_SHA=HEAD_SHA,
            AUTHORIZE_RESULT="success",
            SYSTEM_TEST_RESULT="success",
        )

    def test_check_is_created_on_pr_head_with_run_identity(self):
        self.assertEqual(
            ci.prepare_pr_check(context(), self.api),
            {"pr_check_id": "42", "pr_head_sha": HEAD_SHA},
        )
        body = self.api.write_check.call_args.args[1]
        self.assertEqual(body["head_sha"], HEAD_SHA)
        self.assertNotEqual(body["head_sha"], MASTER_SHA)
        self.assertEqual(body["name"], "test")
        self.assertEqual(body["status"], "in_progress")

    def test_preparation_failure_cannot_create_a_check(self):
        self.api.approved_gate_pr.side_effect = ci.AuthorizationError("unapproved")
        with self.assertRaises(ci.AuthorizationError):
            ci.prepare_pr_check(context(), self.api)
        self.api.write_check.assert_not_called()

    def test_only_complete_success_becomes_green(self):
        for authorization, system, expected in (
            ("success", "success", "success"),
            ("failure", "skipped", "failure"),
            ("success", "failure", "failure"),
            ("success", "skipped", "failure"),
            ("success", "cancelled", "cancelled"),
            ("cancelled", "skipped", "cancelled"),
            ("success", "", "failure"),
        ):
            with self.subTest(authorization=authorization, system=system):
                env = {
                    **self.environment,
                    "AUTHORIZE_RESULT": authorization,
                    "SYSTEM_TEST_RESULT": system,
                }
                ci.report_pr_check(context(), self.api, env)
                args = self.api.write_check.call_args.args
                self.assertEqual(args[0], REPOSITORY)
                self.assertEqual(args[1]["status"], "completed")
                self.assertEqual(args[1]["conclusion"], expected)
                self.assertEqual(args[2], 42)

    def test_cannot_complete_a_check_for_another_head_or_attempt(self):
        for changes in (
            {"head_sha": MASTER_SHA},
            {"external_id": "another-run"},
            {"name": "format"},
            {"id": True},
            {"details_url": "https://example.invalid"},
        ):
            with self.subTest(changes=changes):
                self.api._get.return_value = {**self.check, **changes}
                self.api.write_check.reset_mock()
                with self.assertRaises(ci.AuthorizationError):
                    ci.report_pr_check(context(), self.api, self.environment)
                self.api.write_check.assert_not_called()

    def test_reporting_requires_workflow_run_and_numeric_id(self):
        for ctx, env in (
            (context(event_name="push"), self.environment),
            (context(), {**self.environment, "PR_CHECK_ID": "42\n99"}),
            (context(), {**self.environment, "PR_CHECK_SHA": "invalid"}),
        ):
            with (
                self.subTest(ctx=ctx, env=env),
                self.assertRaises(ci.AuthorizationError),
            ):
                ci.report_pr_check(ctx, self.api, env)
        self.api.write_check.assert_not_called()

    def test_check_http_errors_and_timeouts_are_failures(self):
        api = ci.GitHubApi("dummy-token")
        for error in (
            urllib.error.HTTPError(
                "https://example.invalid", 403, "forbidden", None, None
            ),
            TimeoutError(),
        ):
            with (
                self.subTest(error=error),
                mock.patch.object(ci.urllib.request, "urlopen", side_effect=error),
            ):
                with self.assertRaises(ci.AuthorizationError):
                    api.write_check(REPOSITORY, {"name": "test"})

    def test_check_api_uses_post_for_create_and_patch_for_completion(self):
        api = ci.GitHubApi("dummy-token")
        for check_id, method, status in ((None, "POST", 201), (42, "PATCH", 200)):
            response = io.BytesIO(json.dumps({"id": 42}).encode())
            response.status = status
            with (
                self.subTest(method=method),
                mock.patch.object(
                    ci.urllib.request, "urlopen", return_value=response
                ) as call,
            ):
                self.assertEqual(
                    api.write_check(REPOSITORY, {"name": "test"}, check_id), {"id": 42}
                )
                request = call.call_args.args[0]
                self.assertEqual(request.method, method)
                self.assertEqual(json.loads(request.data), {"name": "test"})


class CommandLineFlowTest(unittest.TestCase):
    def test_all_three_phases_use_trusted_controls_and_api_resolved_head(self):
        api = mock.Mock(spec=ci.GitHubApi)
        api.approved_gate_pr.return_value = snapshot()
        external_id, url = ci._check_identity(context())
        api.write_check.return_value = {
            "id": 42,
            "name": "test",
            "head_sha": HEAD_SHA,
            "external_id": external_id,
            "details_url": url,
        }
        api._get.return_value = api.write_check.return_value
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "outputs"
            environment = {
                "GITHUB_OUTPUT": str(output),
                "GH_TOKEN": "dummy-token",
                "REPOSITORY": REPOSITORY,
                "EVENT_NAME": "workflow_run",
                "WORKFLOW_REF": context().workflow_ref,
                "EVENT_SHA": MASTER_SHA,
                "GITHUB_RUN_ID": "456",
                "GITHUB_RUN_ATTEMPT": "2",
                "SOURCE_RUN_ID": "123",
                "SOURCE_RUN_ATTEMPT": "1",
            }
            with (
                mock.patch.dict(ci.os.environ, environment, clear=True),
                mock.patch.object(ci, "GitHubApi", return_value=api),
            ):
                with mock.patch.object(sys, "argv", ["authorize.py", "--prepare-pr"]):
                    self.assertEqual(ci.main(), 0)
                self.assertIn("pr_check_id=42\n", output.read_text())
                self.assertIn(f"pr_head_sha={HEAD_SHA}\n", output.read_text())
                output.write_text("")
                with (
                    mock.patch.object(sys, "argv", ["authorize.py"]),
                    mock.patch.object(
                        ci, "authorize_dispatch", return_value={"authorized": "true"}
                    ) as dispatch,
                ):
                    self.assertEqual(ci.main(), 0)
                self.assertEqual(dispatch.call_args.args[0].pr_head_sha, HEAD_SHA)
                self.assertEqual(
                    output.read_text(), "authorized=false\nauthorized=true\n"
                )
                with (
                    mock.patch.dict(
                        ci.os.environ,
                        {
                            "PR_CHECK_ID": "42",
                            "PR_CHECK_SHA": HEAD_SHA,
                            "AUTHORIZE_RESULT": "success",
                            "SYSTEM_TEST_RESULT": "failure",
                        },
                    ),
                    mock.patch.object(
                        sys, "argv", ["authorize.py", "--report-pr-check"]
                    ),
                ):
                    self.assertEqual(ci.main(), 0)
                self.assertEqual(
                    api.write_check.call_args.args[1]["conclusion"], "failure"
                )

    def test_candidate_control_ref_is_rejected_before_any_api_call(self):
        with (
            mock.patch.dict(
                ci.os.environ,
                {
                    "REPOSITORY": REPOSITORY,
                    "EVENT_SHA": MASTER_SHA,
                    "WORKFLOW_REF": f"{REPOSITORY}/.github/workflows/build.yaml@refs/pull/1033/merge",
                },
                clear=True,
            ),
            mock.patch.object(sys, "argv", ["authorize.py"]),
            mock.patch.object(ci, "GitHubApi") as api,
        ):
            with self.assertRaises(ci.AuthorizationError):
                ci.main()
            api.assert_not_called()


if __name__ == "__main__":
    unittest.main()
