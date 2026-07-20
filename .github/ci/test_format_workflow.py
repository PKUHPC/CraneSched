from __future__ import annotations

import re
import unittest
from pathlib import Path


WORKFLOW = Path(__file__).parents[1] / "workflows" / "format.yaml"


class FormatWorkflowContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = WORKFLOW.read_text(encoding="utf-8")

    def test_keeps_pull_request_triggers_and_paths(self) -> None:
        for event_type in ("opened", "synchronize", "reopened", "ready_for_review"):
            self.assertRegex(self.workflow, rf"(?m)^      - {event_type}$")
        for path in (
            "'.clang-format*'",
            "'.github/workflows/**'",
            "'dependencies/**'",
            "'CMakeModule/**'",
            "'protos/**'",
            "'src/**'",
            "'test/**'",
            "'CMakeLists.txt'",
            "'VERSION'",
        ):
            self.assertIn(f"      - {path}", self.workflow)

    def test_uses_read_only_permissions_and_no_git_write_path(self) -> None:
        self.assertRegex(self.workflow, r"(?m)^permissions:\n  contents: read$")
        self.assertNotIn("contents: write", self.workflow)
        self.assertNotIn("secrets.", self.workflow)
        self.assertNotIn("GITHUB_TOKEN", self.workflow)
        self.assertNotIn("self-hosted", self.workflow)
        for command in ("git add", "git commit", "git config", "git push", "git checkout"):
            self.assertNotIn(command, self.workflow)

    def test_checks_exact_pull_request_head_without_persisting_credentials(self) -> None:
        checkout = self._step("Checkout pull request head")
        self.assertIn(
            "uses: actions/checkout@93cb6efe18208431cddfb8368fd83d5badbf9bfd # v5",
            checkout,
        )
        self.assertIn(
            "repository: ${{ github.event.pull_request.head.repo.full_name }}", checkout
        )
        self.assertIn("ref: ${{ github.event.pull_request.head.sha }}", checkout)
        self.assertIn("fetch-depth: 1", checkout)
        self.assertIn("persist-credentials: false", checkout)

    def test_format_step_is_local_only_and_produces_a_patch(self) -> None:
        step = self._step("Check clang-format and prepare patch")
        self.assertIn("continue-on-error: true", step)
        self.assertIn("--dry-run --Werror", step)
        self.assertIn("-print0", step)
        self.assertIn("clang-format-20 -i", step)
        self.assertIn("git diff --binary -- src protos > format.patch", step)
        self.assertIn("changed=true", step)

    def test_uploads_patch_for_both_repository_origins(self) -> None:
        step = self._step("Upload format patch")
        self.assertIn(
            "uses: actions/upload-artifact@b7c566a772e6b6bfb58ed0dc250532a479d7789f # v6",
            step,
        )
        self.assertIn("if: always() && steps.format.outputs.changed == 'true'", step)
        self.assertNotIn("head.repo.full_name != github.repository", step)
        self.assertIn("if-no-files-found: error", step)
        self.assertIn("retention-days: 3", step)

    def test_failure_gate_runs_after_artifact_upload(self) -> None:
        upload_start = self.workflow.index("- name: Upload format patch")
        gate_start = self.workflow.index("- name: Fail when formatting is required")
        self.assertLess(upload_start, gate_start)
        gate = self.workflow[gate_start:]
        self.assertIn("steps.format.outputs.changed == 'true'", gate)
        self.assertIn("steps.format.outcome == 'failure'", gate)
        self.assertRegex(gate, r"(?m)^          exit 1$")

    def test_keeps_required_check_identity_and_serializes_pr_runs(self) -> None:
        self.assertIn("name: Clang Format Check", self.workflow)
        self.assertRegex(self.workflow, r"(?m)^  format:\s*$")
        self.assertIn("group: clang-format-pr-${{ github.event.pull_request.number }}", self.workflow)
        self.assertIn("cancel-in-progress: true", self.workflow)

    def _step(self, name: str) -> str:
        match = re.search(
            rf"(?ms)^      - name: {re.escape(name)}\n(.*?)(?=^      - name:|\Z)",
            self.workflow,
        )
        self.assertIsNotNone(match, f"workflow step not found: {name}")
        return match.group(0) if match else ""


if __name__ == "__main__":
    unittest.main()
