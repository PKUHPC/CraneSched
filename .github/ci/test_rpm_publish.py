from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml


UPLOADER = Path(__file__).with_name("upload-packages.sh")


class RpmRepositoryUploadTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.root = Path(self.temporary_directory.name)
        self.packages = self.root / "RPM packages"
        self.packages.mkdir()
        self.bin_dir = self.root / "bin"
        self.bin_dir.mkdir()
        self.calls = self.root / "curl.jsonl"
        curl = self.bin_dir / "curl"
        curl.write_text(
            "#!/usr/bin/env python3\n"
            "import json, os, pathlib, sys\n"
            "arguments = sys.argv[1:]\n"
            "package = pathlib.Path(arguments[arguments.index('--upload-file') + 1])\n"
            "log = pathlib.Path(os.environ['UPLOAD_TEST_CALLS'])\n"
            "count = len(log.read_text().splitlines()) if log.exists() else 0\n"
            "with log.open('a') as stream:\n"
            "    stream.write(json.dumps({'args': arguments, 'data': package.read_text()}) + '\\n')\n"
            "statuses = os.environ.get('UPLOAD_TEST_STATUSES', '201').split(',')\n"
            "print('\\n' + statuses[min(count, len(statuses) - 1)])\n",
            encoding="utf-8",
        )
        curl.chmod(0o755)
        self.environment = {
            "PATH": str(self.bin_dir) + os.pathsep + os.environ["PATH"],
            "NEXUS_URL": "https://rpm-upload.example.invalid/",
            "NEXUS_USERNAME": "test-uploader",
            "NEXUS_PASSWORD": "dummy-test-password",
            "PACKAGE_DIR": str(self.packages),
            "UPLOAD_MODE": "RPM",
            "RPM_REPO_NAME": "crane",
            "RPM_DISTRO": "8",
            "RPM_ARCH": "x86_64",
            "VERSION": "master",
            "RETRY_DELAY": "0",
            "UPLOAD_TEST_CALLS": str(self.calls),
        }

    def _run(self, **overrides: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", str(UPLOADER)],
            env={**self.environment, **overrides},
            text=True,
            capture_output=True,
            timeout=10,
            check=False,
        )

    def _calls(self) -> list[dict]:
        if not self.calls.exists():
            return []
        return [json.loads(line) for line in self.calls.read_text().splitlines()]

    def test_uploads_rpms_to_the_historical_path_and_ignores_debs(self) -> None:
        (self.packages / "craned.rpm").write_text("craned RPM")
        nested = self.packages / "nested"
        nested.mkdir()
        (nested / "frontend.rpm").write_text("frontend RPM")
        (self.packages / "frontend.deb").write_text("not published")

        result = self._run()

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        calls = self._calls()
        self.assertEqual(len(calls), 2)
        self.assertEqual(
            {call["args"][-1] for call in calls},
            {
                "https://rpm-upload.example.invalid/repository/crane/master/8/x86_64/RPMS/craned.rpm",
                "https://rpm-upload.example.invalid/repository/crane/master/8/x86_64/RPMS/nested/frontend.rpm",
            },
        )
        self.assertEqual({call["data"] for call in calls}, {"craned RPM", "frontend RPM"})
        for call in calls:
            arguments = call["args"]
            self.assertEqual(arguments[arguments.index("-u") + 1], "test-uploader:dummy-test-password")
            self.assertEqual(arguments[arguments.index("--connect-timeout") + 1], "15")
            self.assertEqual(arguments[arguments.index("--max-time") + 1], "300")
        self.assertNotIn("dummy-test-password", result.stdout + result.stderr)
        self.assertNotIn("rpm-upload.example.invalid", result.stdout + result.stderr)

    def test_retries_a_failed_upload_then_succeeds(self) -> None:
        (self.packages / "craned.rpm").write_text("RPM")

        result = self._run(UPLOAD_TEST_STATUSES="503,201")

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(len(self._calls()), 2)
        self.assertIn("Retrying", result.stdout)

    def test_reports_failure_after_all_attempts_and_continues_other_packages(self) -> None:
        (self.packages / "craned.rpm").write_text("craned")
        (self.packages / "frontend.rpm").write_text("frontend")

        result = self._run(UPLOAD_TEST_STATUSES="503", CONTINUE_ON_ERROR="true")

        self.assertEqual(result.returncode, 1)
        calls = self._calls()
        self.assertEqual(len(calls), 6)
        self.assertEqual({call["data"] for call in calls}, {"craned", "frontend"})
        self.assertIn("Failed uploads: 2", result.stdout)

    def test_dry_run_does_not_call_curl(self) -> None:
        (self.packages / "craned.rpm").write_text("RPM")

        result = self._run(DRY_RUN="true")

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self._calls(), [])
        self.assertIn("DRY RUN", result.stdout)

    def test_missing_password_fails_before_upload(self) -> None:
        (self.packages / "craned.rpm").write_text("RPM")

        result = self._run(NEXUS_PASSWORD="")

        self.assertEqual(result.returncode, 1)
        self.assertEqual(self._calls(), [])
        self.assertIn("Nexus password is required", result.stdout)


class RpmPublishWorkflowTest(unittest.TestCase):
    def test_each_missing_secret_skips_before_loading_packages(self) -> None:
        workflow_path = Path(__file__).parents[1] / "workflows" / "build.yaml"
        workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
        step = next(
            step for step in workflow["jobs"]["test"]["steps"]
            if step["name"] == "Upload tested RPMs to private repository"
        )
        secrets = {
            "NEXUS_URL": "https://rpm-upload.example.invalid",
            "NEXUS_USERNAME": "test-uploader",
            "NEXUS_PASSWORD": "dummy-test-password",
        }
        for missing_secret in secrets:
            with self.subTest(missing_secret=missing_secret):
                result = subprocess.run(
                    ["bash", "-c", step["run"]],
                    env={"PATH": os.environ["PATH"], **secrets, missing_secret: ""},
                    text=True,
                    capture_output=True,
                    timeout=10,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertIn("Skipping RPM upload", result.stdout)
                self.assertNotIn("dummy-test-password", result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
