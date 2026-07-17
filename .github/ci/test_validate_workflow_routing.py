from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("validate-workflow-routing.py")
BUILD_WORKFLOW = Path(__file__).parents[1] / "workflows" / "build.yaml"
WORKFLOW_PATH = Path(__file__).parents[1] / "workflows" / "build.yaml"
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

        with self.assertRaisesRegex(
            routing.RoutingPolicyError, r"debug.yml:3"
        ):
            routing.validate_workflow_routing(self.workflows_dir)

    def test_rejects_mixed_case_backend_label_in_another_workflow(self) -> None:
        (self.workflows_dir / "debug.yml").write_text(
            "jobs:\n  debug:\n    runs-on: CraneSystemTest\n",
            encoding="utf-8",
        )

        with self.assertRaisesRegex(routing.RoutingPolicyError, r"debug.yml:3"):
            routing.validate_workflow_routing(self.workflows_dir)

    def test_does_not_match_maintenance_runner_label_substring(self) -> None:
        (self.workflows_dir / "maintenance.yaml").write_text(
            "jobs:\n  clean:\n"
            "    runs-on: [self-hosted, cranesystemtest-autotest]\n",
            encoding="utf-8",
        )

        routing.validate_workflow_routing(self.workflows_dir)

    def test_does_not_match_dotted_runner_label_suffix(self) -> None:
        (self.workflows_dir / "debug.yaml").write_text(
            "jobs:\n  debug:\n    runs-on: cranesystemtest.debug\n",
            encoding="utf-8",
        )

        routing.validate_workflow_routing(self.workflows_dir)

    def test_requires_backend_label_in_allowed_workflow(self) -> None:
        (self.workflows_dir / "build.yaml").write_text(
            "jobs:\n  test:\n    runs-on: [self-hosted]\n",
            encoding="utf-8",
        )

        with self.assertRaisesRegex(
            routing.RoutingPolicyError, "must route only"
        ):
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
            "Checkout candidate workflow routing",
            "Validate repository runner routing",
        ):
            with self.subTest(step_name=step_name):
                step = workflow.index(f"- name: {step_name}")
                next_step = workflow.find("\n      - name:", step + 1)
                if next_step == -1:
                    next_step = len(workflow)
                self.assertIn(condition, workflow[step:next_step])


if __name__ == "__main__":
    unittest.main()
