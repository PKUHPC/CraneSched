from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("validate-workflow-routing.py")
BUILD_WORKFLOW = Path(__file__).parents[1] / "workflows" / "build.yaml"
WORKFLOW_PATH = Path(__file__).parents[1] / "workflows" / "build.yaml"
WORKFLOWS_DIR = Path(__file__).parents[1] / "workflows"
REQUEST_WORKFLOW = WORKFLOWS_DIR / "request-ci.yaml"
SPEC = importlib.util.spec_from_file_location(
    "cranesched_ci_validate_workflow_routing", MODULE_PATH
)
assert SPEC is not None and SPEC.loader is not None
routing = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = routing
SPEC.loader.exec_module(routing)


class WorkflowRoutingPolicyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.workflows_dir = Path(self.temporary_directory.name)
        (self.workflows_dir / "build.yaml").write_text(
            "jobs:\n  test:\n    runs-on: [self-hosted, cranesystemtest]\n",
            encoding="utf-8",
        )

    def test_allows_backend_label_only_in_build_workflow(self) -> None:
        (self.workflows_dir / "format.yaml").write_text(
            "jobs:\n  format:\n    runs-on: ubuntu-latest\n",
            encoding="utf-8",
        )

        routing.validate_workflow_routing(self.workflows_dir)

    def test_rejects_backend_label_in_another_workflow(self) -> None:
        (self.workflows_dir / "debug.yml").write_text(
            "jobs:\n  debug:\n    runs-on: cranesystemtest\n",
            encoding="utf-8",
        )

        with self.assertRaisesRegex(routing.RoutingPolicyError, "fixed GitHub-hosted"):
            routing.validate_workflow_routing(self.workflows_dir)

    def test_rejects_mixed_case_backend_label_in_another_workflow(self) -> None:
        (self.workflows_dir / "debug.yml").write_text(
            "jobs:\n  debug:\n    runs-on: CraneSystemTest\n",
            encoding="utf-8",
        )

        with self.assertRaisesRegex(routing.RoutingPolicyError, "fixed GitHub-hosted"):
            routing.validate_workflow_routing(self.workflows_dir)

    def test_rejects_other_self_hosted_runner(self) -> None:
        (self.workflows_dir / "maintenance.yaml").write_text(
            "jobs:\n  clean:\n    runs-on: [self-hosted, cranesystemtest-autotest]\n",
            encoding="utf-8",
        )

        with self.assertRaisesRegex(routing.RoutingPolicyError, "fixed GitHub-hosted"):
            routing.validate_workflow_routing(self.workflows_dir)

    def test_rejects_dotted_runner_label_suffix(self) -> None:
        (self.workflows_dir / "debug.yaml").write_text(
            "jobs:\n  debug:\n    runs-on: cranesystemtest.debug\n",
            encoding="utf-8",
        )

        with self.assertRaisesRegex(routing.RoutingPolicyError, "fixed GitHub-hosted"):
            routing.validate_workflow_routing(self.workflows_dir)

    def test_rejects_expression_runner_routing(self) -> None:
        (self.workflows_dir / "debug.yaml").write_text(
            "jobs:\n  debug:\n    runs-on: ${{ matrix.runner }}\n",
            encoding="utf-8",
        )

        with self.assertRaisesRegex(routing.RoutingPolicyError, "fixed GitHub-hosted"):
            routing.validate_workflow_routing(self.workflows_dir)

    def test_rejects_quoted_runner_key_escape(self) -> None:
        (self.workflows_dir / "debug.yaml").write_text(
            'jobs:\n  debug:\n    runs-on: ubuntu-latest\n    "runs-on": self-hosted\n',
            encoding="utf-8",
        )

        with self.assertRaisesRegex(routing.RoutingPolicyError, "non-canonical"):
            routing.validate_workflow_routing(self.workflows_dir)

    def test_rejects_job_level_reusable_workflow(self) -> None:
        (self.workflows_dir / "debug.yaml").write_text(
            "jobs:\n  debug:\n    uses: owner/repo/.github/workflows/job.yaml@main\n",
            encoding="utf-8",
        )

        with self.assertRaisesRegex(routing.RoutingPolicyError, "job-level reusable"):
            routing.validate_workflow_routing(self.workflows_dir)

    def test_rejects_noncanonical_job_indentation(self) -> None:
        (self.workflows_dir / "debug.yaml").write_text(
            "jobs:\n   debug:\n      runs-on: self-hosted\n",
            encoding="utf-8",
        )

        with self.assertRaisesRegex(routing.RoutingPolicyError, "non-canonical"):
            routing.validate_workflow_routing(self.workflows_dir)

    def test_rejects_duplicate_top_level_jobs_after_canonical_section(self) -> None:
        (self.workflows_dir / "debug.yaml").write_text(
            "jobs:\n  safe:\n    runs-on: ubuntu-latest\n"
            '"jobs":\n  unsafe:\n    runs-on: self-hosted\n',
            encoding="utf-8",
        )

        with self.assertRaisesRegex(routing.RoutingPolicyError, "final top-level"):
            routing.validate_workflow_routing(self.workflows_dir)

    def test_requires_backend_label_in_allowed_workflow(self) -> None:
        (self.workflows_dir / "build.yaml").write_text(
            "jobs:\n  test:\n    runs-on: [self-hosted]\n",
            encoding="utf-8",
        )

        with self.assertRaisesRegex(routing.RoutingPolicyError, "must route only"):
            routing.validate_workflow_routing(self.workflows_dir)

    def test_comment_does_not_satisfy_allowed_job_selector(self) -> None:
        (self.workflows_dir / "build.yaml").write_text(
            "jobs:\n  test:\n    # cranesystemtest\n    runs-on: [self-hosted]\n",
            encoding="utf-8",
        )

        with self.assertRaisesRegex(routing.RoutingPolicyError, "must route only"):
            routing.validate_workflow_routing(self.workflows_dir)

    def test_fake_test_mapping_outside_jobs_does_not_satisfy_selector(self) -> None:
        (self.workflows_dir / "build.yaml").write_text(
            "env:\n  test:\n    runs-on: [self-hosted, cranesystemtest]\n"
            "jobs:\n  test:\n    runs-on: [self-hosted]\n",
            encoding="utf-8",
        )

        with self.assertRaisesRegex(routing.RoutingPolicyError, "must route only"):
            routing.validate_workflow_routing(self.workflows_dir)

    def test_rejects_symlinked_workflow(self) -> None:
        target = self.workflows_dir / "target"
        target.write_text("jobs: {}\n", encoding="utf-8")
        (self.workflows_dir / "debug.yaml").symlink_to(target)

        with self.assertRaisesRegex(
            routing.RoutingPolicyError, "workflow must be a regular file"
        ):
            routing.validate_workflow_routing(self.workflows_dir)

    def test_build_workflow_does_not_suppress_python_bytecode(self) -> None:
        text = BUILD_WORKFLOW.read_text(encoding="utf-8")

        self.assertNotIn("PYTHONDONTWRITEBYTECODE", text)

    def test_candidate_routing_steps_require_successful_authorization(self) -> None:
        workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
        condition = "if: steps.authorize.outputs.authorized == 'true'"
        for step_name in (
            "Checkout proposed merge workflow routing",
            "Verify proposed merge routing revision",
            "Validate repository runner routing",
        ):
            with self.subTest(step_name=step_name):
                step = workflow.index(f"- name: {step_name}")
                next_step = workflow.find("\n      - name:", step + 1)
                if next_step == -1:
                    next_step = len(workflow)
                self.assertIn(condition, workflow[step:next_step])

    def test_candidate_checkout_uses_routing_output_and_parent_guard(self) -> None:
        workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
        checkout = workflow.index("- name: Checkout proposed merge workflow routing")
        checkout_end = workflow.find("\n      - name:", checkout + 1)
        if checkout_end == -1:
            checkout_end = len(workflow)
        checkout_block = workflow[checkout:checkout_end]
        self.assertIn("ref: ${{ steps.authorize.outputs.routing_sha }}", checkout_block)
        self.assertIn("fetch-depth: 2", checkout_block)

        verify = workflow.index("- name: Verify proposed merge routing revision")
        verify_end = workflow.find("\n      - name:", verify + 1)
        if verify_end == -1:
            verify_end = len(workflow)
        verify_block = workflow[verify:verify_end]
        self.assertIn("git -C candidate-routing rev-parse HEAD", verify_block)
        self.assertIn("git -C candidate-routing rev-list --parents", verify_block)
        self.assertIn("PR_BASE_SHA", verify_block)
        self.assertIn("PR_HEAD_SHA", verify_block)

    def test_pr_build_still_uses_backend_head_output(self) -> None:
        workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
        self.assertIn(
            "ref: ${{ needs.authorize.outputs.backend_sha }}",
            workflow,
        )

    def test_frontend_routing_is_sha_pinned_and_fully_reported(self) -> None:
        workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
        self.assertIn(
            "frontend_source: ${{ steps.authorize.outputs.frontend_source }}",
            workflow,
        )
        self.assertIn(
            "FRONTEND_SOURCE: ${{ needs.authorize.outputs.frontend_source }}",
            workflow,
        )

        checkout = workflow.index("- name: Checkout exact FrontEnd revision")
        checkout_end = workflow.find("\n      - name:", checkout + 1)
        checkout_block = workflow[checkout:checkout_end]
        self.assertIn(
            "ref: ${{ needs.authorize.outputs.frontend_sha }}", checkout_block
        )
        self.assertNotIn("frontend_ref", checkout_block)

        collect = workflow.index("- name: Collect sanitized CI artifacts")
        collect_end = workflow.find("\n      - name:", collect + 1)
        collect_block = workflow[collect:collect_end]
        self.assertIn('--frontend-ref "$FRONTEND_REF"', collect_block)
        self.assertIn('--frontend-sha "$FRONTEND_SHA"', collect_block)
        self.assertIn('--frontend-source "$FRONTEND_SOURCE"', collect_block)

    def test_revision_summary_escapes_frontend_routing_as_json_and_html(self) -> None:
        workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
        start = workflow.index("- name: Record revision routing provenance")
        end = workflow.find("\n      - name:", start + 1)
        block = workflow[start:end]
        self.assertIn("json.dumps(", block)
        self.assertIn("html.escape(", block)
        self.assertIn('os.environ["FRONTEND_REF"]', block)
        self.assertIn('os.environ["FRONTEND_SOURCE"]', block)
        self.assertNotIn('echo "- FrontEnd', block)

    def test_authorization_receives_actions_run_identity(self) -> None:
        workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
        self.assertIn("GITHUB_RUN_ID: ${{ github.run_id }}", workflow)
        self.assertIn("GITHUB_RUN_ATTEMPT: ${{ github.run_attempt }}", workflow)
        authorize_job = workflow.index("  authorize:\n")
        privileged_job = workflow.index("  test:\n", authorize_job)
        self.assertIn("      issues: read\n", workflow[authorize_job:privileged_job])

    def test_final_test_is_hosted_and_never_skipped(self) -> None:
        workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
        final_job = workflow.index("  final-test:\n")
        final_block = workflow[final_job:]
        self.assertIn("    name: test\n", final_block)
        self.assertIn("    if: always()\n", final_block)
        self.assertIn("    runs-on: ubuntu-latest\n", final_block)
        self.assertIn("      - authorize\n", final_block)
        self.assertIn("      - test\n", final_block)
        self.assertIn("needs.authorize.result", final_block)
        self.assertIn("needs.test.result", final_block)
        self.assertIn('[[ "$AUTHORIZE_RESULT" != "success" ]]', final_block)
        self.assertIn('[[ "$SYSTEM_TEST_RESULT" != "success" ]]', final_block)
        privileged_job = workflow.index("  test:\n")
        privileged_end = workflow.index("  final-test:\n", privileged_job)
        privileged_block = workflow[privileged_job:privileged_end]
        self.assertIn("    name: system-test\n", privileged_block)
        self.assertIn("    runs-on: [self-hosted, cranesystemtest]\n", privileged_block)

    def test_request_workflow_is_hosted_and_does_not_execute_pr_code(self) -> None:
        workflow = REQUEST_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("  issue_comment:\n", workflow)
        self.assertIn("      - created\n", workflow)
        self.assertIn("  contents: read\n", workflow)
        self.assertIn("  actions: read\n", workflow)
        self.assertIn("  issues: write\n", workflow)
        self.assertIn("  pull-requests: write\n", workflow)
        self.assertNotIn("  pull-requests: read\n", workflow)
        self.assertIn(
            "  group: request-fork-ci-${{ github.event.issue.number }}\n", workflow
        )
        self.assertIn("  cancel-in-progress: false\n", workflow)
        self.assertIn(
            "if: github.event.issue.pull_request && "
            "github.event.comment.body == '/request-ci'",
            workflow,
        )
        self.assertIn("    runs-on: ubuntu-latest\n", workflow)
        self.assertIn("ref: ${{ github.sha }}", workflow)
        self.assertIn("persist-credentials: false", workflow)
        self.assertIn('[[ "$DEFAULT_BRANCH" == "master" ]]', workflow)
        self.assertIn("git -C trusted-ci rev-parse HEAD", workflow)
        self.assertIn("trusted-ci/.github/ci/request_ci.py", workflow)
        self.assertNotIn("pull_request.head", workflow)
        self.assertNotIn("secrets.", workflow)
        self.assertNotIn("self-hosted", workflow)

    def test_repository_workflows_obey_routing_policy(self) -> None:
        routing.validate_workflow_routing(WORKFLOWS_DIR)

    def test_stale_head_is_not_used_as_routing_input(self) -> None:
        stale_head = self.workflows_dir / "stale-head"
        merged_tree = self.workflows_dir / "merged-tree"
        stale_head.mkdir()
        merged_tree.mkdir()
        (stale_head / "build.yaml").write_text(
            "jobs:\n  test:\n    runs-on: [self-hosted, CraneSched]\n",
            encoding="utf-8",
        )
        (merged_tree / "build.yaml").write_text(
            "jobs:\n  test:\n    runs-on: [self-hosted, cranesystemtest]\n",
            encoding="utf-8",
        )

        with self.assertRaises(routing.RoutingPolicyError):
            routing.validate_workflow_routing(stale_head)
        routing.validate_workflow_routing(merged_tree)


if __name__ == "__main__":
    unittest.main()
