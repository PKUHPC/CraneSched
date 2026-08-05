#!/usr/bin/env python3
"""Focused tests for the execution-flow schema compiler."""

from __future__ import annotations

import copy
import hashlib
import importlib.util
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest

import yaml


SCRIPT = Path(__file__).with_name("generate_execution_flow_schema.py")
SPEC = importlib.util.spec_from_file_location("execution_flow_schema_generator", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
GENERATOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GENERATOR)

REPOSITORY_ROOT = SCRIPT.parent.parent
DIRECT_EMITTER_CALL = re.compile(r"\bFlowEmitter::([A-Z][A-Za-z0-9_]*)\s*\(")
DIRECT_HELPERS = {
    "ChildRuntimeConfig",
    "CorrelationRequired",
    "CorrelationValue",
}


class ExecutionFlowSchemaValidationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.schema = GENERATOR._load_schema(GENERATOR.DEFAULT_SCHEMA)

    def test_canonical_schema_is_valid(self) -> None:
        points = GENERATOR._validate(copy.deepcopy(self.schema))
        self.assertIn("pipeline/fault", {point["id"] for point in points})

    def test_product_point_emissions_use_the_gated_macro(self) -> None:
        offenders: list[str] = []
        implementation = (
            REPOSITORY_ROOT / "src/Utilities/OpenTelemetry/ExecutionFlow.cpp"
        )
        for path in sorted((REPOSITORY_ROOT / "src").rglob("*")):
            if path.suffix not in {".cc", ".cpp", ".h", ".hpp"} or path == implementation:
                continue
            source = path.read_text(encoding="utf-8")
            for line_number, line in enumerate(source.splitlines(), start=1):
                for match in DIRECT_EMITTER_CALL.finditer(line):
                    if match.group(1) not in DIRECT_HELPERS:
                        offenders.append(
                            f"{path.relative_to(REPOSITORY_ROOT)}:{line_number}:"
                            f" FlowEmitter::{match.group(1)}"
                        )
        self.assertEqual(
            offenders,
            [],
            "semantic point emissions must use CRANE_FLOW_EMIT so disabled "
            "instrumentation cannot evaluate arguments:\n" + "\n".join(offenders),
        )

    def test_pipeline_fault_must_reference_a_point(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["metadata"]["pipelineFaultPoint"] = "flow/v1/pipeline/missing"
        with self.assertRaisesRegex(ValueError, "canonical point"):
            GENERATOR._validate(schema)

    def test_pipeline_fault_must_require_reason_code(self) -> None:
        schema = copy.deepcopy(self.schema)
        fault = next(
            point for point in schema["points"] if point["id"] == "pipeline/fault"
        )
        fault["requiredAttributes"] = ["job_id"]
        with self.assertRaisesRegex(ValueError, "reason_code"):
            GENERATOR._validate(schema)

    def test_enum_values_must_not_collide_after_symbol_normalization(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["enums"]["reason_code"].extend(["schema-collision", "schema_collision"])
        with self.assertRaisesRegex(
            ValueError,
            "same target identifier 'SchemaCollision'",
        ):
            GENERATOR._validate(schema)

    def test_attributes_must_not_collide_after_symbol_normalization(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["attributes"]["job__id"] = {"type": "string"}
        with self.assertRaisesRegex(
            ValueError,
            "same target identifier 'JobId'",
        ):
            GENERATOR._validate(schema)

    def test_envelope_and_point_attributes_must_be_disjoint(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["attributes"]["flow_id"] = {"type": "string"}
        with self.assertRaisesRegex(
            ValueError,
            r"envelope and point attributes must be disjoint: \['flow_id'\]",
        ):
            GENERATOR._validate(schema)

    def test_envelope_attributes_must_have_supported_types(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["envelopeAttributes"]["flow_id"]["type"] = "bytes"
        with self.assertRaisesRegex(
            ValueError,
            r"envelope attribute 'flow_id' has an unsupported type",
        ):
            GENERATOR._validate(schema)

    def test_envelope_attributes_must_have_supported_requirements(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["envelopeAttributes"]["flow_id"]["required"] = "sometimes"
        with self.assertRaisesRegex(
            ValueError,
            r"envelope attribute 'flow_id' has an unsupported requirement",
        ):
            GENERATOR._validate(schema)

    def test_envelope_missing_reason_must_be_canonical(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["envelopeAttributes"]["flow_id"]["missingReason"] = "not_real"
        with self.assertRaisesRegex(
            ValueError,
            r"envelope attribute 'flow_id' references unknown missingReason 'not_real'",
        ):
            GENERATOR._validate(schema)

    def test_storage_contract_must_be_non_empty(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["storageAttributes"] = {}
        with self.assertRaisesRegex(
            ValueError,
            "storageAttributes must be a non-empty mapping",
        ):
            GENERATOR._validate(schema)

    def test_storage_attribute_shape_is_strict(self) -> None:
        schema = copy.deepcopy(self.schema)
        del schema["storageAttributes"]["flow_slot"]["kind"]
        with self.assertRaisesRegex(
            ValueError,
            r"storage attribute 'flow_slot' keys do not match the contract.*kind",
        ):
            GENERATOR._validate(schema)

    def test_storage_attributes_are_frontend_owned_and_non_wire(self) -> None:
        for key, value, message in (
            ("source", "backend", "unsupported source"),
            ("wire", True, "must be non-wire"),
        ):
            with self.subTest(key=key):
                schema = copy.deepcopy(self.schema)
                schema["storageAttributes"]["flow_environment_id"][key] = value
                with self.assertRaisesRegex(ValueError, message):
                    GENERATOR._validate(schema)

    def test_storage_numeric_bounds_are_valid(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["storageAttributes"]["flow_slot"]["minimum"] = 64
        with self.assertRaisesRegex(ValueError, "minimum exceeds maximum"):
            GENERATOR._validate(schema)

        schema = copy.deepcopy(self.schema)
        schema["storageAttributes"]["flow_environment_id"]["maximum"] = 63
        with self.assertRaisesRegex(
            ValueError,
            "string storage attribute.*cannot declare numeric bounds",
        ):
            GENERATOR._validate(schema)

    def test_storage_and_wire_attributes_must_be_disjoint(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["attributes"]["flow_slot"] = {"type": "int64"}
        with self.assertRaisesRegex(
            ValueError,
            r"storage-only attributes must be disjoint.*\['flow_slot'\]",
        ):
            GENERATOR._validate(schema)

    def test_point_symbol_must_not_collide_with_count_sentinel(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["points"][0]["symbol"] = "Count"
        with self.assertRaisesRegex(
            ValueError,
            r"point symbol 'Count'.*FlowPoint::kCount sentinel",
        ):
            GENERATOR._validate(schema)

    def test_frontend_reason_code_type_name_is_reserved(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["enums"]["reason_code"].append("code")
        with self.assertRaisesRegex(
            ValueError,
            r"reserved target identifier 'Code'.*executionFlowReasonCode type",
        ):
            GENERATOR._validate(schema)

    def test_duplicate_yaml_mapping_keys_are_rejected(self) -> None:
        source = GENERATOR.DEFAULT_SCHEMA.read_text(encoding="utf-8")
        source = source.replace(
            "  job_id: {type: int64}",
            "  job_id: {type: int64, type: string}",
            1,
        )
        with tempfile.TemporaryDirectory() as directory:
            schema_path = Path(directory) / "duplicate.yaml"
            schema_path.write_text(source, encoding="utf-8")
            with self.assertRaisesRegex(
                yaml.YAMLError,
                "duplicate mapping key 'type'",
            ):
                GENERATOR._load_schema(schema_path)

    def test_attribute_count_at_mask_capacity_is_valid(self) -> None:
        schema = copy.deepcopy(self.schema)
        extra_count = GENERATOR.FLOW_ATTRIBUTE_MASK_BITS - len(schema["attributes"])
        for index in range(extra_count):
            schema["attributes"][f"boundary_{index}"] = {"type": "string"}
        self.assertEqual(
            len(schema["attributes"]),
            GENERATOR.FLOW_ATTRIBUTE_MASK_BITS,
        )
        points = GENERATOR._validate(schema)
        rendered = GENERATOR._render_cpp(
            GENERATOR.DEFAULT_SCHEMA.read_bytes(),
            schema,
            points,
        )
        self.assertIn("FlowAttributeMask{1} << 15", rendered)

    def test_attribute_count_must_fit_generated_mask(self) -> None:
        schema = copy.deepcopy(self.schema)
        extra_count = GENERATOR.FLOW_ATTRIBUTE_MASK_BITS - len(schema["attributes"]) + 1
        for index in range(extra_count):
            schema["attributes"][f"extra_{index}"] = {"type": "string"}
        self.assertEqual(
            len(schema["attributes"]),
            GENERATOR.FLOW_ATTRIBUTE_MASK_BITS + 1,
        )
        with self.assertRaisesRegex(ValueError, "FlowAttributeMask capacity"):
            GENERATOR._validate(schema)


class ExecutionFlowSchemaCliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.directory = Path(self.temporary_directory.name)
        self.schema = self.directory / "v1.yaml"
        self.schema.write_bytes(GENERATOR.DEFAULT_SCHEMA.read_bytes())
        self.outputs = {
            "cpp": self.directory / "ExecutionFlowSchema.h",
            "frontend": self.directory / "flow_catalog_generated.go",
            "autotest": self.directory / "catalog_generated.go",
            "autotest_python": self.directory / "schema_generated.py",
        }

    def _command(self, *, check: bool = False) -> list[str]:
        command = [
            sys.executable,
            str(SCRIPT),
            "--schema",
            str(self.schema),
            "--output",
            str(self.outputs["cpp"]),
            "--frontend-output",
            str(self.outputs["frontend"]),
            "--autotest-output",
            str(self.outputs["autotest"]),
            "--autotest-python-output",
            str(self.outputs["autotest_python"]),
        ]
        if check:
            command.append("--check")
        return command

    def _run(self, *, check: bool = False) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            self._command(check=check),
            text=True,
            capture_output=True,
            check=False,
        )

    def test_three_outputs_embed_digest_and_pass_check(self) -> None:
        generated = self._run()
        self.assertEqual(generated.returncode, 0, generated.stderr)
        digest = hashlib.sha256(self.schema.read_bytes()).hexdigest()
        digest_constants = {
            "cpp": rf'kExecutionFlowSchemaSha256\s*=\s*"{digest}"',
            "frontend": rf'executionFlowSchemaSHA256\s*=\s*"{digest}"',
            "autotest": rf'\bSHA256\s*=\s*"{digest}"',
            "autotest_python": rf"\bSHA256\s*=\s*['\"]{digest}['\"]",
        }
        version_constants = {
            "cpp": r'kExecutionFlowSchemaVersion\s*=\s*"v1"',
            "frontend": r'executionFlowSchemaVersion\s*=\s*"v1"',
            "autotest": r'\bVersion\s*=\s*"v1"',
            "autotest_python": r"\bVERSION\s*=\s*['\"]v1['\"]",
        }
        for name, output in self.outputs.items():
            with self.subTest(output=name):
                content = output.read_text(encoding="utf-8")
                comment_prefix = "#" if name == "autotest_python" else "//"
                self.assertIn(f"{comment_prefix} SHA256: {digest}", content)
                self.assertRegex(content, digest_constants[name])
                self.assertRegex(content, version_constants[name])

        frontend = self.outputs["frontend"].read_text(encoding="utf-8")
        self.assertIn(
            "var generatedExecutionFlowCatalog executionFlowSchemaCatalog =",
            frontend,
        )
        self.assertNotIn("func init()", frontend)

        for name, output in self.outputs.items():
            with self.subTest(envelope_output=name):
                content = output.read_text(encoding="utf-8")
                self.assertIn("flow_id", content)
                self.assertIn("event_sequence", content)
                if name == "cpp":
                    self.assertNotIn("flow_instance_slot", content)
                    self.assertNotIn("flow_slot", content)
                else:
                    self.assertIn("flow_environment_id", content)
                    self.assertIn("flow_instance_slot", content)
                    self.assertIn("flow_slot", content)

        cpp = self.outputs["cpp"].read_text(encoding="utf-8")
        self.assertIn("FlowEnvelopeAttributeName", cpp)
        self.assertIn("FlowAttributeWireType", cpp)
        self.assertIn("FlowEnvelopeAttributeWireType", cpp)
        self.assertIn("FlowEnvelopeAttributeRequirement", cpp)
        self.assertIn("FlowEnvelopeAttributeMissingReason", cpp)
        self.assertIn("FlowEnvelopeAttribute::kEventSequence", cpp)

        self.assertIn("AllowsEnvelopeAttribute", frontend)
        self.assertIn("EnvelopeAttributeType", frontend)
        self.assertIn("EnvelopeAttributes", frontend)
        self.assertIn("AllowsStorageAttribute", frontend)
        self.assertIn("StorageAttribute", frontend)
        self.assertIn("StorageAttributes", frontend)
        self.assertIn("Wire: false", frontend)
        self.assertIn("Maximum: 63", frontend)

        autotest = self.outputs["autotest"].read_text(encoding="utf-8")
        self.assertIn("func EnvelopeAttribute(", autotest)
        self.assertIn("func EnvelopeAttributeNames(", autotest)
        self.assertIn("func StorageAttribute(", autotest)
        self.assertIn("func StorageAttributeNames(", autotest)
        self.assertIn("StorageFlowEnvironmentID", autotest)
        self.assertIn("HasMaximum: true", autotest)

        autotest_python = self.outputs["autotest_python"].read_text(
            encoding="utf-8"
        )
        self.assertIn("# fmt: off\nAPI_VERSION", autotest_python)
        self.assertIn("\n# fmt: on\n\n__all__", autotest_python)
        self.assertIn("ENVELOPE_ATTRIBUTES", autotest_python)
        self.assertIn("'required': 'business'", autotest_python)
        self.assertIn("STORAGE_ATTRIBUTES", autotest_python)
        self.assertIn("'source': 'frontend'", autotest_python)
        self.assertIn("'wire': False", autotest_python)
        self.assertIn("'maximum': None", autotest_python)

        generated_spec = importlib.util.spec_from_file_location(
            "generated_execution_flow_schema",
            self.outputs["autotest_python"],
        )
        assert generated_spec is not None and generated_spec.loader is not None
        generated_module = importlib.util.module_from_spec(generated_spec)
        generated_spec.loader.exec_module(generated_module)
        environment = generated_module.STORAGE_ATTRIBUTES["flow_environment_id"]
        self.assertIs(environment["wire"], False)
        self.assertIsNone(environment["minimum"])
        self.assertIsNone(environment["maximum"])

        checked = self._run(check=True)
        self.assertEqual(checked.returncode, 0, checked.stderr)

    def test_check_rejects_each_stale_output(self) -> None:
        generated = self._run()
        self.assertEqual(generated.returncode, 0, generated.stderr)
        for name, output in self.outputs.items():
            with self.subTest(output=name):
                original = output.read_text(encoding="utf-8")
                output.write_text(original + "// stale\n", encoding="utf-8")
                checked = self._run(check=True)
                self.assertEqual(checked.returncode, 1)
                self.assertIn(str(output), checked.stderr)
                output.write_text(original, encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
