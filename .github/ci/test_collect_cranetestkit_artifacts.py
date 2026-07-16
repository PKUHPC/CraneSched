from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("collect-cranetestkit-artifacts.py")


class ArtifactCollectionTest(unittest.TestCase):
    def _collect(
        self, run_root: Path, destination: Path, *, logs: bool = False
    ) -> None:
        command = [
            sys.executable,
            str(SCRIPT),
            "--run-root",
            str(run_root),
            "--destination",
            str(destination),
            "--run-id",
            "gh-123-1",
        ]
        if logs:
            command.append("--include-logs")
        subprocess.run(command, check=True)

    def test_allowlists_and_redacts_failure_logs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run_root = root / "runs/gh-123-1"
            (run_root / "plan").mkdir(parents=True)
            (run_root / "input").mkdir()
            (run_root / "logs/wrl02/0/services").mkdir(parents=True)
            (run_root / "plan/manifest.json").write_text(
                json.dumps(
                    {
                        "sources": {
                            "backend": "a" * 40,
                            "frontend": "b" * 40,
                            "autotest": "c" * 40,
                        },
                        "build_id": "build-1",
                        "plan_digest": "d" * 64,
                    }
                ),
                encoding="utf-8",
            )
            (run_root / "state.json").write_text(
                json.dumps({"phase": "password=hunter2", "exit_code": 1}),
                encoding="utf-8",
            )
            (run_root / "result.json").write_text(
                json.dumps(
                    {
                        "exit_code": 1,
                        "total": 2,
                        "passed": 1,
                        "failed": 1,
                        "error": 0,
                        "build_id": ("build`|<b>github_pat_abcdefghijklmnopqrstuvwxyz"),
                    }
                ),
                encoding="utf-8",
            )
            (run_root / "input/build-manifest.json").write_text(
                "{}\n", encoding="utf-8"
            )
            oversized = run_root / "input/packages.json"
            with oversized.open("wb") as stream:
                stream.seek(8 * 1024 * 1024)
                stream.write(b"x")
            allowed_log = run_root / "logs/wrl02/0/services/cranectld.log"
            allowed_log.write_text(
                "password=hunter2 github_pat_abcdefghijklmnopqrstuvwxyz\n",
                encoding="utf-8",
            )
            (run_root / "logs/wrl02/0/env.log").write_text(
                "SHOULD_NOT_BE_COPIED\n", encoding="utf-8"
            )
            (run_root / "logs/wrl02/0/link.log").symlink_to(allowed_log)

            destination = root / "artifact"
            self._collect(run_root, destination, logs=True)

            copied = destination / "logs/wrl02/0/services/cranectld.log"
            self.assertTrue(copied.is_file())
            self.assertEqual(copied.read_text(encoding="utf-8").count("[REDACTED]"), 2)
            self.assertFalse((destination / "logs/wrl02/0/env.log").exists())
            self.assertFalse((destination / "logs/wrl02/0/link.log").exists())
            manifest = json.loads(
                (destination / "artifact-manifest.json").read_text(encoding="utf-8")
            )
            self.assertTrue(manifest["run_present"])
            self.assertTrue(manifest["failure_logs_included"])
            packages_entry = next(
                item
                for item in manifest["files"]
                if item["path"] == "input/packages.json"
            )
            self.assertTrue(packages_entry["truncated"])
            self.assertLessEqual(
                (destination / "input/packages.json").stat().st_size, 8 * 1024 * 1024
            )
            self.assertIn(
                "Backend SHA", (destination / "summary.md").read_text(encoding="utf-8")
            )
            summary = (destination / "summary.md").read_text(encoding="utf-8")
            self.assertNotIn("hunter2", summary)
            self.assertNotIn("github_pat_", summary)
            self.assertNotIn("<b>", summary)
            self.assertIn("build&#96;\\|&lt;b&gt;[REDACTED]", summary)

    def test_missing_run_still_produces_diagnostic_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            destination = root / "artifact"
            self._collect(root / "missing", destination)
            manifest = json.loads(
                (destination / "artifact-manifest.json").read_text(encoding="utf-8")
            )
            self.assertFalse(manifest["run_present"])
            self.assertEqual(manifest["files"], [])
            self.assertIn(
                "not-created", (destination / "summary.md").read_text(encoding="utf-8")
            )


if __name__ == "__main__":
    unittest.main()
