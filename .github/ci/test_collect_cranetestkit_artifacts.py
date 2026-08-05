from __future__ import annotations

import importlib.util
import json
import math
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).with_name("collect-cranetestkit-artifacts.py")
SPEC = importlib.util.spec_from_file_location(
    "cranesched_ci_artifact_collection", SCRIPT
)
assert SPEC is not None and SPEC.loader is not None
collection = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = collection
SPEC.loader.exec_module(collection)


class ArtifactCollectionTest(unittest.TestCase):
    def _collect(
        self,
        run_root: Path,
        destination: Path,
        *,
        logs: bool = False,
        cache_hit: str | None = None,
        build_duration: float | None = None,
        execute_exit_code: int | None = None,
        routing_sha: str | None = None,
        pr_base_sha: str | None = None,
        pr_head_sha: str | None = None,
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
        if cache_hit is not None:
            command.extend(("--build-cache-hit", cache_hit))
        if build_duration is not None:
            command.extend(("--build-duration-seconds", str(build_duration)))
        if execute_exit_code is not None:
            command.extend(("--execute-exit-code", str(execute_exit_code)))
        if routing_sha is not None:
            command.extend(("--routing-sha", routing_sha))
        if pr_base_sha is not None:
            command.extend(("--pr-base-sha", pr_base_sha))
        if pr_head_sha is not None:
            command.extend(("--pr-head-sha", pr_head_sha))
        subprocess.run(command, check=True)

    @staticmethod
    def _write_json(path: Path, value: object) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value), encoding="utf-8")

    @staticmethod
    def _case(
        case_id: str,
        status: str,
        duration: float,
        *,
        name: str | None = None,
        message: str | None = None,
    ) -> dict[str, object]:
        return {
            "id": case_id,
            "name": name or f"case-{case_id}",
            "path": f"Module/TC{case_id}.json",
            "status": status,
            "duration_seconds": duration,
            "message": message,
        }

    def _checkpoint(
        self,
        run_root: Path,
        worker: str,
        slot: int,
        shard: int,
        cases: list[dict[str, object]],
        *,
        exit_code: int | None,
        fatal: dict[str, str] | None = None,
        duration: int = 30,
    ) -> None:
        self._write_json(
            run_root / f"results/{worker}/{slot}/shard-{shard}.json",
            {
                "shard_index": shard,
                "started_at": "2026-07-16T00:00:00Z",
                "finished_at": f"2026-07-16T00:00:{duration:02d}Z",
                "fatal": fatal,
                "worker_exit_code": exit_code,
                "cases": cases,
            },
        )

    def _run(
        self,
        root: Path,
        cases: list[dict[str, object]],
        *,
        exit_code: int,
        shards: list[list[dict[str, object]]],
        workers: dict[str, list[int]],
        infrastructure_errors: list[str] | None = None,
        missing_case_ids: list[str] | None = None,
        execution_flow: dict[str, object] | None = None,
    ) -> Path:
        run_root = root / "runs/gh-123-1"
        planned_shards = [
            {
                "index": index,
                "estimated_duration_seconds": 20 + index,
                "cases": [{"case": case} for case in shard_cases],
            }
            for index, shard_cases in enumerate(shards)
        ]
        self._write_json(
            run_root / "plan/manifest.json",
            {
                "sources": {
                    "backend": "a" * 40,
                    "frontend": "b" * 40,
                    "autotest": "c" * 40,
                },
                "build_id": "build-1",
                "plan_digest": "d" * 64,
                "image_digest": "localhost/autotest@sha256:" + "e" * 64,
                "sharding": {"shards": planned_shards},
            },
        )
        self._write_json(
            run_root / "plan/allocation.json",
            {"run_id": "gh-123-1", "slots": len(shards), "workers": workers},
        )
        counts = {
            status: sum(case["status"] == status for case in cases)
            for status in ("passed", "failed", "error", "not_run")
        }
        self._write_json(
            run_root / "result.json",
            {
                "exit_code": exit_code,
                "total": len(cases),
                "passed": counts["passed"],
                "failed": counts["failed"],
                "error": counts["error"],
                "not_run_case_ids": [
                    case["id"] for case in cases if case["status"] == "not_run"
                ],
                "infrastructure_errors": infrastructure_errors or [],
                "missing_case_ids": missing_case_ids or [],
                "duplicate_case_ids": [],
                "extra_case_ids": [],
                "cases": cases,
                "sources": {
                    "backend": "a" * 40,
                    "frontend": "b" * 40,
                    "autotest": "c" * 40,
                },
                "build_id": "build-1",
                "suite_digest": "f" * 64,
                "plan_digest": "d" * 64,
                "image_digest": "localhost/autotest@sha256:" + "e" * 64,
                "started_at": "2026-07-16T00:00:00Z",
                "finished_at": "2026-07-16T00:01:05.200000Z",
                "phase_durations_seconds": {"wait": 59.999, "total": 65.2},
                "execution_flow": execution_flow
                or {
                    "case_count": 0,
                    "iteration_count": 0,
                    "subject_count": 0,
                    "status_counts": {},
                    "mode_counts": {},
                    "contract_counts": {},
                },
            },
        )
        self._write_json(
            run_root / "state.json", {"phase": "stopped", "exit_code": exit_code}
        )
        return run_root

    def test_summary_renders_bounded_execution_flow_findings(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            case = self._case("4.1.0.4", "passed", 2.0, name="batch_flow")
            case["execution_flow"] = [
                {
                    "apiVersion": "cranesched.io/flow-result/v1",
                    "contract": "batch/v1",
                    "mode": "shadow",
                    "iteration": 0,
                    "deadline_at": "2026-07-30T00:03:00Z",
                    "subjects": [
                        {
                            "process_index": 0,
                            "flow_id": "0" * 32,
                            "job_ids": [42],
                            "status": "flow_violation",
                            "last_point": "flow/v1/ctld/job/resources_released",
                            "expected_next": ["embedded_persisted"],
                            "pipeline_healthy": True,
                            "violation_codes": ["edge_deadline_exceeded"],
                        }
                    ],
                }
            ]
            run_root = self._run(
                root,
                [case],
                exit_code=0,
                shards=[[case]],
                workers={"wrl02": [0]},
                execution_flow={
                    "case_count": 1,
                    "iteration_count": 1,
                    "subject_count": 1,
                    "status_counts": {
                        "satisfied": 0,
                        "flow_violation": 1,
                        "trace_pipeline_inconclusive": 0,
                        "unsupported": 0,
                    },
                    "mode_counts": {"shadow": 1, "enforce": 0},
                    "contract_counts": {"batch/v1": 1},
                },
            )
            destination = root / "artifact"

            self._collect(run_root, destination)
            summary = (destination / "summary.md").read_text(encoding="utf-8")

            self.assertIn("### Execution flow contracts", summary)
            self.assertIn("batch/v1 (1)", summary)
            self.assertIn("flow_violation", summary)
            self.assertIn("edge_deadline_exceeded", summary)

    def test_summary_filters_malformed_execution_flow_detail_lists(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            case = self._case("4.1.0.4", "passed", 2.0, name="batch_flow")
            case["execution_flow"] = [
                {
                    "mode": "shadow",
                    "subjects": [
                        {
                            "process_index": 0,
                            "status": "trace_pipeline_inconclusive",
                            "expected_next": [
                                None,
                                42,
                                {"not": "text"},
                                *[f"point-{index}" for index in range(100)],
                            ],
                            "violation_codes": [False, "pipeline_gap"],
                        }
                    ],
                }
            ]
            run_root = self._run(
                root,
                [case],
                exit_code=2,
                shards=[[case]],
                workers={"wrl02": [0]},
                execution_flow={
                    "case_count": 1,
                    "iteration_count": 1,
                    "subject_count": 1,
                    "status_counts": {"trace_pipeline_inconclusive": 1},
                    "mode_counts": {"shadow": 1},
                    "contract_counts": {"batch/v1": 1},
                },
            )
            destination = root / "artifact"

            self._collect(run_root, destination)
            summary = (destination / "summary.md").read_text(encoding="utf-8")

            self.assertIn("pipeline_gap", summary)
            self.assertIn("point-0", summary)
            self.assertIn("point-15", summary)
            self.assertNotIn("point-16", summary)
            self.assertNotIn("{'not': 'text'}", summary)

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
            private_key = "private_key=-----BEGIN PRIVATE KEY-----\n" + "A" * 5000
            (run_root / "state.json").write_text(
                json.dumps(
                    {
                        "phase": f"password=hunter2 {private_key}",
                        "exit_code": 1,
                    }
                ),
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
            flow_case = (
                run_root
                / "logs/wrl02/0/flow/cases/TC4-1-0-3"
                / ("a" * 32)
                / "iteration-0.ndjson"
            )
            flow_case.parent.mkdir(parents=True)
            flow_case.write_text(
                '{"type":"validator_event","token":"secret-token"}\n',
                encoding="utf-8",
            )
            flow_progress = run_root / "logs/wrl02/0/flow/shard-0.progress.json"
            flow_progress.write_text('{"flow_status":"progress"}\n', encoding="utf-8")
            flow_events = run_root / "logs/wrl02/0/flow/shard-0.events.ndjson"
            flow_events.write_text('{"type":"health"}\n', encoding="utf-8")
            (run_root / "logs/wrl02/0/flow/unapproved.json").write_text(
                "SHOULD_NOT_BE_COPIED\n", encoding="utf-8"
            )
            (run_root / "logs/wrl02/0/flow/shard-1.progress.json").symlink_to(
                flow_progress
            )

            destination = root / "artifact"
            self._collect(run_root, destination, logs=True)

            copied = destination / "logs/wrl02/0/services/cranectld.log"
            self.assertTrue(copied.is_file())
            self.assertEqual(copied.read_text(encoding="utf-8").count("[REDACTED]"), 2)
            self.assertFalse((destination / "logs/wrl02/0/env.log").exists())
            self.assertFalse((destination / "logs/wrl02/0/link.log").exists())
            copied_flow_case = destination / flow_case.relative_to(run_root)
            self.assertTrue(copied_flow_case.is_file())
            copied_flow_payload = json.loads(
                copied_flow_case.read_text(encoding="utf-8")
            )
            self.assertEqual(copied_flow_payload["token"], "[REDACTED]")
            self.assertNotIn(
                "secret-token", copied_flow_case.read_text(encoding="utf-8")
            )
            self.assertTrue(
                (destination / flow_progress.relative_to(run_root)).is_file()
            )
            self.assertTrue(
                (destination / flow_events.relative_to(run_root)).is_file()
            )
            self.assertFalse(
                (destination / "logs/wrl02/0/flow/unapproved.json").exists()
            )
            self.assertFalse(
                (destination / "logs/wrl02/0/flow/shard-1.progress.json").exists()
            )
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
            self.assertNotIn("BEGIN PRIVATE KEY", summary)
            self.assertNotIn("A" * 100, summary)
            self.assertNotIn("<b>", summary)
            self.assertIn("build&#96;\\|&lt;b&gt;[REDACTED]", summary)

    def test_log_and_flow_copy_respect_exact_aggregate_byte_limits(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            case = self._case("1.0.0.1", "failed", 1.0)
            run_root = self._run(
                root,
                [case],
                exit_code=1,
                shards=[[case]],
                workers={"wrl02": [0]},
            )
            service_logs = run_root / "logs/wrl02/0/services"
            service_logs.mkdir(parents=True)
            (service_logs / "a.log").write_text("abcdefgh", encoding="utf-8")
            (service_logs / "b.log").write_text("ijklmnop", encoding="utf-8")
            flow_logs = run_root / "logs/wrl02/0/flow"
            flow_logs.mkdir(parents=True)
            (flow_logs / "shard-0.commands.ndjson").write_text(
                "abcdefgh", encoding="utf-8"
            )
            (flow_logs / "shard-1.events.ndjson").write_text(
                "ijklmnop", encoding="utf-8"
            )
            destination = root / "artifact"

            with (
                mock.patch.object(
                    sys,
                    "argv",
                    [
                        str(SCRIPT),
                        "--run-root",
                        str(run_root),
                        "--destination",
                        str(destination),
                        "--run-id",
                        "gh-123-1",
                        "--include-logs",
                    ],
                ),
                mock.patch.object(collection, "MAX_LOG_BYTES", 8),
                mock.patch.object(collection, "MAX_TOTAL_LOG_BYTES", 10),
                mock.patch.object(collection, "MAX_FLOW_FILE_BYTES", 8),
                mock.patch.object(collection, "MAX_TOTAL_FLOW_BYTES", 10),
            ):
                self.assertEqual(collection.main(), 0)

            copied_service_logs = tuple(
                (destination / "logs/wrl02/0/services").glob("*.log")
            )
            copied_flow_logs = tuple((destination / "logs/wrl02/0/flow").glob("*"))
            self.assertEqual(sum(path.stat().st_size for path in copied_service_logs), 10)
            self.assertEqual(sum(path.stat().st_size for path in copied_flow_logs), 10)
            self.assertEqual(
                (destination / "logs/wrl02/0/services/b.log").read_text(
                    encoding="utf-8"
                ),
                "op",
            )
            self.assertEqual(
                (destination / "logs/wrl02/0/flow/shard-1.events.ndjson").read_text(
                    encoding="utf-8"
                ),
                "op",
            )

    def test_pass_summary_shows_shards_timing_and_top_ten_slowest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cases = [
                self._case(
                    f"1.0.0.{index}", "passed", float(index), name=f"slow-{index}"
                )
                for index in range(1, 12)
            ]
            run_root = self._run(
                root,
                cases,
                exit_code=0,
                shards=[cases[:6], cases[6:]],
                workers={"wrl02": [0, 1]},
            )
            self._checkpoint(
                run_root, "wrl02", 0, 0, cases[:6], exit_code=0, duration=30
            )
            self._checkpoint(
                run_root, "wrl02", 1, 1, cases[6:], exit_code=0, duration=40
            )

            destination = root / "artifact"
            self._collect(
                run_root,
                destination,
                cache_hit="true",
                build_duration=59.999,
            )
            summary = (destination / "summary.md").read_text(encoding="utf-8")

            self.assertIn("system test: PASSED", summary)
            self.assertIn("`11`", summary)
            self.assertIn("`2/2`", summary)
            self.assertIn("`wrl02 / 0`", summary)
            self.assertIn("`6 (6/0/0/0)`", summary)
            self.assertIn("`1m 00.00s`", summary)
            self.assertIn("Build cache: **hit**", summary)
            self.assertLess(summary.index("slow-11"), summary.index("slow-10"))
            self.assertNotIn("slow-1`", summary)
            manifest = json.loads(
                (destination / "artifact-manifest.json").read_text(encoding="utf-8")
            )
            self.assertEqual(manifest["check_exit_code"], 0)
            self.assertFalse(manifest["failure_logs_included"])

    def test_revision_routing_is_preserved_in_summary_and_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cases = [self._case("1.0.0.1", "passed", 1.0)]
            run_root = self._run(
                root,
                cases,
                exit_code=0,
                shards=[cases],
                workers={"wrl02": [0]},
            )
            self._checkpoint(run_root, "wrl02", 0, 0, cases, exit_code=0)
            destination = root / "artifact"
            routing_sha = "d" * 40
            base_sha = "e" * 40
            head_sha = "f" * 40

            self._collect(
                run_root,
                destination,
                routing_sha=routing_sha,
                pr_base_sha=base_sha,
                pr_head_sha=head_sha,
            )

            summary = (destination / "summary.md").read_text(encoding="utf-8")
            self.assertIn(f"`{routing_sha}`", summary)
            self.assertIn(f"`{base_sha}`", summary)
            self.assertIn(f"`{head_sha}`", summary)
            manifest = json.loads(
                (destination / "artifact-manifest.json").read_text(encoding="utf-8")
            )
            self.assertEqual(
                manifest["revision_routing"],
                {
                    "routing_sha": routing_sha,
                    "pr_base_sha": base_sha,
                    "pr_head_sha": head_sha,
                },
            )

    def test_exit_one_summary_reports_failed_and_error_cases(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cases = [
                self._case("1.0.0.1", "passed", 1.0),
                self._case(
                    "1.0.0.2",
                    "failed",
                    2.25,
                    name="failed|<case>",
                    message="expected | value\npassword=hunter2",
                ),
                self._case(
                    "1.0.0.3",
                    "error",
                    3.5,
                    message="github_pat_abcdefghijklmnopqrstuvwxyz",
                ),
            ]
            run_root = self._run(
                root,
                cases,
                exit_code=1,
                shards=[cases],
                workers={"wrl03": [0]},
            )
            self._checkpoint(run_root, "wrl03", 0, 0, cases, exit_code=1)

            destination = root / "artifact"
            self._collect(run_root, destination)
            summary = (destination / "summary.md").read_text(encoding="utf-8")

            self.assertIn("system test: TEST FAILURE", summary)
            self.assertIn("`1.0.0.2`", summary)
            self.assertIn("`failed\\|&lt;case&gt;`", summary)
            self.assertIn("`wrl03`", summary)
            self.assertIn("`2.25s`", summary)
            self.assertIn("[REDACTED]", summary)
            self.assertNotIn("hunter2", summary)
            self.assertNotIn("github_pat_", summary)

    def test_exit_two_summary_prioritizes_infrastructure_and_fatal(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cases = [
                self._case("1.0.0.1", "failed", 2.0, message="assertion failed"),
                self._case("1.0.0.2", "not_run", 0.0),
            ]
            run_root = self._run(
                root,
                cases,
                exit_code=2,
                shards=[cases],
                workers={"wrl04": [0]},
                infrastructure_errors=[
                    "<script>Authorization: Bearer secret-token</script>",
                    ("shard 0 fatal during teardown: password=hunter2"),
                ],
                missing_case_ids=["1.0.0.2"],
            )
            self._checkpoint(
                run_root,
                "wrl04",
                0,
                0,
                cases,
                exit_code=2,
                fatal={"phase": "teardown", "message": "password=hunter2"},
            )

            destination = root / "artifact"
            self._collect(run_root, destination)
            summary = (destination / "summary.md").read_text(encoding="utf-8")

            self.assertIn("system test: INFRASTRUCTURE ERROR", summary)
            self.assertIn("### Infrastructure errors", summary)
            self.assertIn("&lt;script&gt;", summary)
            self.assertIn("teardown", summary)
            self.assertIn("Missing cases", summary)
            self.assertIn("`1`", summary)
            self.assertIn("`INFRASTRUCTURE ERROR`", summary)
            self.assertEqual(summary.count("fatal during teardown"), 1)
            self.assertNotIn("secret-token", summary)
            self.assertNotIn("hunter2", summary)

    def test_workflow_exit_mismatch_is_an_infrastructure_error(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cases = [self._case("1.0.0.1", "passed", 1.0)]
            run_root = self._run(
                root,
                cases,
                exit_code=0,
                shards=[cases],
                workers={"wrl02": [0]},
            )
            self._checkpoint(run_root, "wrl02", 0, 0, cases, exit_code=0)

            destination = root / "artifact"
            self._collect(run_root, destination, execute_exit_code=2)
            summary = (destination / "summary.md").read_text(encoding="utf-8")

            self.assertIn("system test: INFRASTRUCTURE ERROR", summary)
            self.assertIn("Workflow exit code 2 disagrees with", summary)
            self.assertIn("aggregate result exit code 0", summary)
            self.assertIn("run state exit code 0", summary)
            manifest = json.loads(
                (destination / "artifact-manifest.json").read_text(encoding="utf-8")
            )
            self.assertEqual(manifest["check_exit_code"], 2)

    def test_workflow_success_without_aggregate_is_infrastructure_error(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run_root = root / "runs/gh-123-1"
            run_root.mkdir(parents=True)
            destination = root / "artifact"

            self._collect(run_root, destination, execute_exit_code=0)
            summary = (destination / "summary.md").read_text(encoding="utf-8")

            self.assertIn("system test: INFRASTRUCTURE ERROR", summary)
            self.assertIn("aggregate result is missing", summary)

    def test_infrastructure_summary_folds_not_run_and_prioritizes_root_cause(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cases = [
                self._case(f"3.0.0.{index}", "not_run", 0.0) for index in range(1, 26)
            ]
            infrastructure = [
                (
                    f"case was not run: 3.0.0.{index}"
                    if index % 2
                    else f"missing case result: 3.0.0.{index}"
                )
                for index in range(1, 26)
            ]
            infrastructure.extend(
                f"shard {index} checkpoint is unfinished" for index in range(25)
            )
            infrastructure.append(
                "lease: LeaseBusyError: cluster capacity is held by another run"
            )
            run_root = self._run(
                root,
                cases,
                exit_code=2,
                shards=[cases],
                workers={"wrl02": [0]},
                infrastructure_errors=infrastructure,
            )
            self._checkpoint(run_root, "wrl02", 0, 0, cases, exit_code=2)

            destination = root / "artifact"
            self._collect(run_root, destination, execute_exit_code=2)
            summary = (destination / "summary.md").read_text(encoding="utf-8")

            self.assertIn("lease: LeaseBusyError", summary)
            self.assertNotIn("case was not run:", summary)
            self.assertNotIn("missing case result:", summary)
            self.assertIn("Folded 25 case-level not-run or missing-result", summary)
            self.assertIn("Showing 20 of 26 errors", summary)

    def test_incomplete_summary_uses_partial_checkpoint_not_run_count(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run_root = root / "runs/gh-123-1"
            cases = [
                self._case("1.0.0.1", "passed", 1.0),
                self._case("1.0.0.2", "not_run", 0.0),
            ]
            self._write_json(
                run_root / "plan/manifest.json",
                {
                    "sharding": {
                        "shards": [
                            {"index": 0, "cases": []},
                            {"index": 1, "cases": []},
                        ]
                    }
                },
            )
            self._write_json(
                run_root / "plan/allocation.json",
                {"workers": {"wrl02": [0, 1]}},
            )
            self._write_json(run_root / "state.json", {"phase": "stopped"})
            self._checkpoint(run_root, "wrl02", 0, 0, cases, exit_code=None)

            destination = root / "artifact"
            self._collect(run_root, destination)
            summary = (destination / "summary.md").read_text(encoding="utf-8")

            self.assertIn("system test: INCOMPLETE", summary)
            self.assertIn("`1 (partial)`", summary)
            self.assertIn("`0/2`", summary)

    def test_success_with_missing_shard_is_an_infrastructure_error(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cases = [
                self._case("1.0.0.1", "passed", 1.0),
                self._case("2.0.0.1", "passed", 1.0),
            ]
            run_root = self._run(
                root,
                cases,
                exit_code=0,
                shards=[[cases[0]], [cases[1]]],
                workers={"wrl02": [0, 1]},
            )
            self._checkpoint(run_root, "wrl02", 0, 0, [cases[0]], exit_code=0)
            failure_log = run_root / "logs/wrl02/0/services/cranectld.log"
            failure_log.parent.mkdir(parents=True)
            failure_log.write_text("missing shard diagnostic\n", encoding="utf-8")

            destination = root / "artifact"
            self._collect(run_root, destination, execute_exit_code=0)
            summary = (destination / "summary.md").read_text(encoding="utf-8")
            manifest = json.loads(
                (destination / "artifact-manifest.json").read_text(encoding="utf-8")
            )

            self.assertIn("system test: INFRASTRUCTURE ERROR", summary)
            self.assertIn("shard 1 is missing", summary)
            self.assertEqual(manifest["check_exit_code"], 2)
            self.assertTrue(manifest["failure_logs_included"])
            self.assertTrue(
                (destination / "logs/wrl02/0/services/cranectld.log").is_file()
            )

    def test_success_conflicting_with_terminal_shard_is_infrastructure_error(
        self,
    ) -> None:
        for shard_exit_code in (1, 2):
            with self.subTest(shard_exit_code=shard_exit_code):
                with tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    cases = [self._case("1.0.0.1", "passed", 1.0)]
                    run_root = self._run(
                        root,
                        cases,
                        exit_code=0,
                        shards=[cases],
                        workers={"wrl02": [0]},
                    )
                    self._checkpoint(
                        run_root,
                        "wrl02",
                        0,
                        0,
                        cases,
                        exit_code=shard_exit_code,
                    )

                    destination = root / "artifact"
                    self._collect(run_root, destination, execute_exit_code=0)
                    summary = (destination / "summary.md").read_text(encoding="utf-8")
                    manifest = json.loads(
                        (destination / "artifact-manifest.json").read_text(
                            encoding="utf-8"
                        )
                    )

                    self.assertIn("system test: INFRASTRUCTURE ERROR", summary)
                    self.assertIn("conflicts with aggregate exit code 0", summary)
                    self.assertEqual(manifest["check_exit_code"], 2)

    def test_test_failure_conflicting_with_shard_infrastructure_is_exit_two(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cases = [self._case("1.0.0.1", "failed", 1.0)]
            run_root = self._run(
                root,
                cases,
                exit_code=1,
                shards=[cases],
                workers={"wrl02": [0]},
            )
            self._checkpoint(run_root, "wrl02", 0, 0, cases, exit_code=2)

            destination = root / "artifact"
            self._collect(run_root, destination, execute_exit_code=1)
            summary = (destination / "summary.md").read_text(encoding="utf-8")
            manifest = json.loads(
                (destination / "artifact-manifest.json").read_text(encoding="utf-8")
            )

            self.assertIn("system test: INFRASTRUCTURE ERROR", summary)
            self.assertIn("conflicts with aggregate exit code 1", summary)
            self.assertEqual(manifest["check_exit_code"], 2)

    def test_test_failure_with_incomplete_shard_is_exit_two(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            failed_case = self._case("1.0.0.1", "failed", 1.0)
            not_run_case = self._case("2.0.0.1", "not_run", 0.0)
            cases = [failed_case, not_run_case]
            run_root = self._run(
                root,
                cases,
                exit_code=1,
                shards=[[failed_case], [not_run_case]],
                workers={"wrl02": [0, 1]},
            )
            self._checkpoint(run_root, "wrl02", 0, 0, [failed_case], exit_code=1)
            self._checkpoint(run_root, "wrl02", 1, 1, [not_run_case], exit_code=0)

            destination = root / "artifact"
            self._collect(run_root, destination, execute_exit_code=1)
            summary = (destination / "summary.md").read_text(encoding="utf-8")
            manifest = json.loads(
                (destination / "artifact-manifest.json").read_text(encoding="utf-8")
            )

            self.assertIn("system test: INFRASTRUCTURE ERROR", summary)
            self.assertIn("incomplete coverage", summary)
            self.assertEqual(manifest["check_exit_code"], 2)

    def test_affected_cases_use_numeric_dotted_id_order(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cases = [
                self._case("10.0.0.1", "not_run", 0.0),
                self._case("2.0.0.1", "not_run", 0.0),
                self._case("1.0.0.1", "not_run", 0.0),
            ]
            run_root = self._run(
                root,
                cases,
                exit_code=2,
                shards=[cases],
                workers={"wrl02": [0]},
                infrastructure_errors=["infrastructure failure"],
            )
            self._checkpoint(run_root, "wrl02", 0, 0, cases, exit_code=2)

            destination = root / "artifact"
            self._collect(run_root, destination)
            summary = (destination / "summary.md").read_text(encoding="utf-8")

            self.assertLess(summary.index("`1.0.0.1`"), summary.index("`2.0.0.1`"))
            self.assertLess(summary.index("`2.0.0.1`"), summary.index("`10.0.0.1`"))

    def test_plan_suite_digest_fallback_without_aggregate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run_root = root / "runs/gh-123-1"
            self._write_json(
                run_root / "plan/manifest.json",
                {
                    "suite": {"digest": "suite-primary"},
                    "sharding": {
                        "suite_digest": "suite-secondary",
                        "shards": [],
                    },
                },
            )

            destination = root / "artifact"
            self._collect(run_root, destination)
            summary = (destination / "summary.md").read_text(encoding="utf-8")
            self.assertIn("system test: INCOMPLETE", summary)
            self.assertIn("suite-primary", summary)
            self.assertNotIn("suite-secondary", summary)

            self._write_json(
                run_root / "plan/manifest.json",
                {"sharding": {"suite_digest": "suite-secondary", "shards": []}},
            )
            fallback_destination = root / "fallback-artifact"
            self._collect(run_root, fallback_destination)
            fallback_summary = (fallback_destination / "summary.md").read_text(
                encoding="utf-8"
            )
            self.assertIn("suite-secondary", fallback_summary)

    def test_shard_metadata_scan_is_bounded_before_sorting(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            run_root = Path(temporary)
            results_root = run_root / "results"
            results_root.mkdir()

            def shard_paths(_path: Path, _pattern: str):
                for index in range(collection.MAX_METADATA_FILES):
                    yield results_root / f"shard-{index}.json"
                raise AssertionError("metadata iterator was consumed past its limit")

            with mock.patch.object(Path, "rglob", shard_paths):
                self.assertEqual(collection._shard_results(run_root), [])

    def test_shard_metadata_has_a_total_byte_limit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            run_root = Path(temporary)
            first = run_root / "results/wrl02/0/shard-0.json"
            second = run_root / "results/wrl02/1/shard-1.json"
            self._write_json(first, {"shard_index": 0, "cases": []})
            self._write_json(second, {"shard_index": 1, "cases": []})

            with mock.patch.object(
                collection, "MAX_TOTAL_CHECKPOINT_BYTES", first.stat().st_size
            ):
                records = collection._shard_results(run_root)

            self.assertEqual([record[1]["shard_index"] for record in records], [0])

    def test_symlinked_run_inputs_do_not_leak_into_summary(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run_root = root / "runs/gh-123-1"
            (run_root / "results/wrl02/0").mkdir(parents=True)
            outside_result = root / "outside-result.json"
            outside_checkpoint = root / "outside-checkpoint.json"
            secret = "password=outside-secret"
            self._write_json(
                outside_result,
                {"exit_code": 0, "total": 1, "passed": 1, "secret": secret},
            )
            self._write_json(
                outside_checkpoint,
                {
                    "shard_index": 0,
                    "worker_exit_code": 0,
                    "finished_at": "2026-07-16T00:00:01Z",
                    "cases": [self._case("1.0.0.1", "passed", 1.0, message=secret)],
                },
            )
            (run_root / "result.json").symlink_to(outside_result)
            (run_root / "results/wrl02/0/shard-0.json").symlink_to(outside_checkpoint)
            self._write_json(
                run_root / "plan/manifest.json",
                {
                    "sharding": {
                        "shards": [
                            {
                                "index": 0,
                                "estimated_duration_seconds": 1,
                                "cases": [],
                            }
                        ]
                    }
                },
            )

            destination = root / "artifact"
            self._collect(run_root, destination)
            summary = (destination / "summary.md").read_text(encoding="utf-8")

            self.assertIn("system test: INCOMPLETE", summary)
            self.assertIn("`MISSING`", summary)
            self.assertNotIn("outside-secret", summary)
            self.assertFalse((destination / "result.json").exists())
            self.assertFalse((destination / "results/wrl02/0/shard-0.json").exists())

            linked_run = root / "linked-run"
            linked_run.symlink_to(run_root, target_is_directory=True)
            linked_destination = root / "linked-artifact"
            self._collect(linked_run, linked_destination)
            linked_summary = (linked_destination / "summary.md").read_text(
                encoding="utf-8"
            )
            self.assertNotIn("outside-secret", linked_summary)
            self.assertIn("system test: INCOMPLETE", linked_summary)

    def test_destination_symlink_is_rejected_without_deleting_target(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run_root = root / "run"
            run_root.mkdir()
            target = root / "sentinel"
            target.mkdir()
            sentinel = target / "keep.txt"
            sentinel.write_text("keep", encoding="utf-8")
            destination = root / "artifact"
            destination.symlink_to(target, target_is_directory=True)
            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--run-root",
                    str(run_root),
                    "--destination",
                    str(destination),
                    "--run-id",
                    "gh-123-1",
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "keep")

    def test_duration_formatting(self) -> None:
        cases = (
            (0, "0.00s"),
            (1.234, "1.23s"),
            (59.999, "1m 00.00s"),
            (65.2, "1m 05.20s"),
            (3661.75, "1h 01m 01.75s"),
        )
        for value, expected in cases:
            with self.subTest(value=value):
                self.assertEqual(collection._format_duration(value), expected)
        for value in (None, -1, True, math.nan, math.inf, 1e308, 10**309):
            with self.subTest(invalid=value):
                self.assertEqual(collection._format_duration(value), "unavailable")

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
