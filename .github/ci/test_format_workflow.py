from __future__ import annotations

import re
import unittest
from pathlib import Path


WORKFLOW = Path(__file__).parents[1] / "workflows" / "ci.yaml"


class FormatWorkflowContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.document = WORKFLOW.read_text(encoding="utf-8")
        cls.workflow = cls.document.split("  format:\n", 1)[1].split("  build:\n", 1)[0]

    def test_build_uses_the_revision_checked_by_formatting(self) -> None:
        import yaml

        workflow = yaml.safe_load(self.document)
        jobs = workflow['jobs']
        self.assertEqual(jobs['build']['needs'], 'format')
        checkout = jobs['build']['steps'][0]['with']
        self.assertEqual(checkout['ref'], '${{ needs.format.outputs.source_sha }}')
        self.assertIn('format', jobs['summary']['needs'])

    def test_uses_read_only_permissions_and_no_git_write_path(self) -> None:
        self.assertRegex(self.document, r"(?m)^permissions:\n  contents: read$")
        self.assertNotIn("contents: write", self.workflow)
        self.assertNotIn("secrets.", self.workflow)
        self.assertNotIn("GITHUB_TOKEN", self.workflow)
        self.assertNotIn("self-hosted", self.workflow)
        for command in ("git add", "git commit", "git config", "git push", "git checkout"):
            self.assertNotIn(command, self.workflow)

    def test_checks_selected_revision_without_persisting_credentials(self) -> None:
        checkout = self._step("Checkout the selected product source")
        self.assertIn(
            "uses: actions/checkout@93cb6efe18208431cddfb8368fd83d5badbf9bfd # v5",
            checkout,
        )
        self.assertIn("ref: ${{ inputs.source_ref || github.sha }}", checkout)
        self.assertIn("git rev-parse HEAD", self.workflow)
        self.assertIn("fetch-depth: 1", checkout)
        self.assertIn("persist-credentials: false", checkout)

    def test_format_step_is_local_only_and_produces_a_patch(self) -> None:
        step = self._step("Check clang-format and prepare patch")
        self.assertIn("continue-on-error: true", step)
        self.assertIn("--dry-run --Werror", step)
        self.assertIn("find src test -type f", step)
        self.assertIn("-print0", step)
        self.assertIn("clang-format -i", step)
        self.assertIn("git diff --binary -- src test protos > format.patch", step)
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

    def test_pins_the_formatter_environment(self) -> None:
        import yaml

        job = yaml.safe_load(self.document)['jobs']['format']
        self.assertEqual(job['runs-on'], 'crane-ci')
        self.assertEqual(job['container']['image'], '${{ vars.CRANE_CI_DEV_IMAGE }}')
        self.assertIn('clang-format --version', self.workflow)
        for command in ('apt-get', 'apt.llvm.org', 'wget', 'sudo'):
            self.assertNotIn(command, self.workflow)

    def _step(self, name: str) -> str:
        match = re.search(
            rf"(?ms)^      - name: {re.escape(name)}\n(.*?)(?=^      - name:|\Z)",
            self.workflow,
        )
        self.assertIsNotNone(match, f"workflow step not found: {name}")
        return match.group(0) if match else ""


if __name__ == "__main__":
    unittest.main()
