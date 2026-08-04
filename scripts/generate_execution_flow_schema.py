#!/usr/bin/env python3
"""Generate the typed C++ execution-flow point catalog from the wire schema."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

import yaml
from yaml.constructor import ConstructorError
from yaml.nodes import MappingNode
from yaml.resolver import BaseResolver


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCHEMA = ROOT / "schemas/execution-flow/v1.yaml"
DEFAULT_OUTPUT = (
    ROOT / "src/Utilities/OpenTelemetry/include/crane/ExecutionFlowSchema.h"
)
ATTRIBUTE_TYPES = {"int64", "string", "enum"}
FLOW_ATTRIBUTE_MASK_BITS = 16
PRODUCERS = {"cranectld", "craned", "frontend", "supervisor"}
PRODUCER_POINT_PREFIXES = {
    "cranectld": "ctld/",
    "craned": "craned/",
    "frontend": "pipeline/",
    "supervisor": "supervisor/",
}
TOP_LEVEL_KEYS = {
    "apiVersion",
    "kind",
    "metadata",
    "envelopeAttributes",
    "storageAttributes",
    "attributes",
    "enums",
    "points",
}
METADATA_KEYS = {"name", "wirePrefix", "heartbeatPoint", "pipelineFaultPoint"}
POINT_KEYS = {"symbol", "id", "producer", "requiredAttributes"}
ENVELOPE_ATTRIBUTE_KEYS = {"type", "required", "missingReason"}
ENVELOPE_REQUIREMENTS = {"always", "business", "optional"}
STORAGE_ATTRIBUTE_KEYS = {
    "type",
    "kind",
    "source",
    "wire",
    "minimum",
    "maximum",
}
STORAGE_KINDS = {"field", "tag"}
STORAGE_SOURCES = {"frontend"}
ATTRIBUTE_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
ENUM_VALUE_PATTERN = re.compile(r"^[a-z][a-z0-9_-]*$")
POINT_ID_PATTERN = re.compile(
    r"^(?:ctld|craned|pipeline|supervisor)/[a-z0-9_]+(?:/[a-z0-9_]+)*$"
)
SYMBOL_PATTERN = re.compile(r"^[A-Z][A-Za-z0-9]*$")
POINT_SYMBOL_RESERVED = {"Count": "FlowPoint::kCount sentinel"}
FRONTEND_REASON_SYMBOL_RESERVED = {
    "Code": "executionFlowReasonCode type",
}


class _UniqueKeyLoader(yaml.SafeLoader):
    """Safe YAML loader that rejects duplicate keys at every mapping level."""


def _construct_unique_mapping(
    loader: _UniqueKeyLoader, node: MappingNode, deep: bool = False
) -> dict[object, object]:
    loader.flatten_mapping(node)
    mapping: dict[object, object] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in mapping
        except TypeError as exc:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                "found an unhashable mapping key",
                key_node.start_mark,
            ) from exc
        if duplicate:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"found duplicate mapping key {key!r}",
                key_node.start_mark,
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeyLoader.add_constructor(
    BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def _enum_symbol(value: str) -> str:
    return "".join(part.capitalize() for part in value.replace("_", "-").split("-"))


def _go_symbol(value: str) -> str:
    """Return a Go identifier while preserving the conventional ID initialism."""
    return _enum_symbol(value).replace("Id", "ID")


def _validate_generated_symbols(owner: str, values: list[str]) -> None:
    generated: dict[str, str] = {}
    for value in values:
        symbol = _enum_symbol(value)
        previous = generated.setdefault(symbol, value)
        if previous != value:
            raise ValueError(
                f"{owner} values {previous!r} and {value!r} generate the "
                f"same target identifier {symbol!r}"
            )


def _validate_reserved_symbols(
    owner: str, values: list[str], reserved: dict[str, str]
) -> None:
    for value in values:
        symbol = _enum_symbol(value)
        if symbol in reserved:
            raise ValueError(
                f"{owner} value {value!r} generates reserved target identifier "
                f"{symbol!r} ({reserved[symbol]})"
            )


def _load_schema(path: Path) -> dict[str, object]:
    loader = _UniqueKeyLoader(path.read_text(encoding="utf-8"))
    try:
        data = loader.get_single_data()
    finally:
        loader.dispose()
    if not isinstance(data, dict):
        raise ValueError("schema root must be a mapping")
    if data.get("apiVersion") != "cranesched.io/execution-flow-schema/v1":
        raise ValueError("unsupported execution-flow schema apiVersion")
    if data.get("kind") != "ExecutionFlowPointSchema":
        raise ValueError("schema kind must be ExecutionFlowPointSchema")
    return data


def _validate(data: dict[str, object]) -> list[dict[str, object]]:
    unknown_top_level = set(data) - TOP_LEVEL_KEYS
    missing_top_level = TOP_LEVEL_KEYS - set(data)
    if unknown_top_level or missing_top_level:
        raise ValueError(
            "schema top-level keys do not match the contract: "
            f"missing={sorted(missing_top_level)}, "
            f"unknown={sorted(unknown_top_level)}"
        )

    metadata = data.get("metadata")
    if not isinstance(metadata, dict) or set(metadata) != METADATA_KEYS:
        actual = set(metadata) if isinstance(metadata, dict) else set()
        raise ValueError(
            "metadata keys do not match the contract: "
            f"missing={sorted(METADATA_KEYS - actual)}, "
            f"unknown={sorted(actual - METADATA_KEYS)}"
        )
    if metadata.get("name") != "flow/v1":
        raise ValueError("metadata.name must be 'flow/v1'")
    if metadata.get("wirePrefix") != "flow/v1/":
        raise ValueError("metadata.wirePrefix must be 'flow/v1/'")
    for field in ("heartbeatPoint", "pipelineFaultPoint"):
        point = metadata.get(field)
        if (
            not isinstance(point, str)
            or not point.startswith(str(metadata["wirePrefix"]))
            or point == metadata["wirePrefix"]
        ):
            raise ValueError(f"metadata.{field} must be within wirePrefix")
    if metadata["heartbeatPoint"] == metadata["pipelineFaultPoint"]:
        raise ValueError("pipeline heartbeat and fault points must differ")

    envelope_attributes = data.get("envelopeAttributes")
    if not isinstance(envelope_attributes, dict) or not envelope_attributes:
        raise ValueError("envelopeAttributes must be a non-empty mapping")
    for name, definition in envelope_attributes.items():
        if (
            not isinstance(name, str)
            or ATTRIBUTE_NAME_PATTERN.fullmatch(name) is None
            or not isinstance(definition, dict)
        ):
            raise ValueError("envelope attribute definitions must be mappings")
        if set(definition) != ENVELOPE_ATTRIBUTE_KEYS:
            raise ValueError(
                f"envelope attribute {name!r} keys do not match the contract: "
                f"missing={sorted(ENVELOPE_ATTRIBUTE_KEYS - set(definition))}, "
                f"unknown={sorted(set(definition) - ENVELOPE_ATTRIBUTE_KEYS)}"
            )
        if definition.get("type") not in ATTRIBUTE_TYPES:
            raise ValueError(f"envelope attribute {name!r} has an unsupported type")
        if definition.get("required") not in ENVELOPE_REQUIREMENTS:
            raise ValueError(
                f"envelope attribute {name!r} has an unsupported requirement"
            )
        missing_reason = definition.get("missingReason")
        if (
            not isinstance(missing_reason, str)
            or ENUM_VALUE_PATTERN.fullmatch(missing_reason) is None
        ):
            raise ValueError(
                f"envelope attribute {name!r} has an invalid missingReason"
            )
    _validate_generated_symbols("envelope attribute", list(envelope_attributes))

    storage_attributes = data.get("storageAttributes")
    if not isinstance(storage_attributes, dict) or not storage_attributes:
        raise ValueError("storageAttributes must be a non-empty mapping")
    for name, definition in storage_attributes.items():
        if (
            not isinstance(name, str)
            or ATTRIBUTE_NAME_PATTERN.fullmatch(name) is None
            or not isinstance(definition, dict)
        ):
            raise ValueError("storage attribute definitions must be mappings")
        if set(definition) != STORAGE_ATTRIBUTE_KEYS:
            raise ValueError(
                f"storage attribute {name!r} keys do not match the contract: "
                f"missing={sorted(STORAGE_ATTRIBUTE_KEYS - set(definition))}, "
                f"unknown={sorted(set(definition) - STORAGE_ATTRIBUTE_KEYS)}"
            )
        if definition.get("type") not in {"int64", "string"}:
            raise ValueError(f"storage attribute {name!r} has an unsupported type")
        if definition.get("kind") not in STORAGE_KINDS:
            raise ValueError(f"storage attribute {name!r} has an unsupported kind")
        if definition.get("source") not in STORAGE_SOURCES:
            raise ValueError(f"storage attribute {name!r} has an unsupported source")
        if definition.get("wire") is not False:
            raise ValueError(f"storage attribute {name!r} must be non-wire")
        minimum = definition.get("minimum")
        maximum = definition.get("maximum")
        for boundary_name, boundary in (("minimum", minimum), ("maximum", maximum)):
            if boundary is not None and (
                isinstance(boundary, bool) or not isinstance(boundary, int)
            ):
                raise ValueError(
                    f"storage attribute {name!r} {boundary_name} must be an integer or null"
                )
        if definition.get("type") == "string" and (
            minimum is not None or maximum is not None
        ):
            raise ValueError(
                f"string storage attribute {name!r} cannot declare numeric bounds"
            )
        if minimum is not None and maximum is not None and minimum > maximum:
            raise ValueError(
                f"storage attribute {name!r} minimum exceeds maximum"
            )
    _validate_generated_symbols("storage attribute", list(storage_attributes))

    attributes = data.get("attributes")
    if not isinstance(attributes, dict) or not attributes:
        raise ValueError("attributes must be a non-empty mapping")
    if len(attributes) > FLOW_ATTRIBUTE_MASK_BITS:
        raise ValueError(
            "attributes exceed the 16-bit FlowAttributeMask capacity: "
            f"{len(attributes)} > {FLOW_ATTRIBUTE_MASK_BITS}"
        )
    for name, definition in attributes.items():
        if (
            not isinstance(name, str)
            or ATTRIBUTE_NAME_PATTERN.fullmatch(name) is None
            or not isinstance(definition, dict)
        ):
            raise ValueError("attribute definitions must be mappings")
        if set(definition) != {"type"}:
            raise ValueError(f"attribute {name!r} must contain exactly the 'type' key")
        if definition.get("type") not in ATTRIBUTE_TYPES:
            raise ValueError(f"attribute {name!r} has an unsupported type")
    _validate_generated_symbols("attribute", list(attributes))
    overlap = set(envelope_attributes) & set(attributes)
    if overlap:
        raise ValueError(
            "envelope and point attributes must be disjoint: "
            f"{sorted(overlap)}"
        )
    storage_overlap = set(storage_attributes) & (
        set(envelope_attributes) | set(attributes)
    )
    if storage_overlap:
        raise ValueError(
            "storage-only attributes must be disjoint from wire attributes: "
            f"{sorted(storage_overlap)}"
        )

    enums = data.get("enums")
    if not isinstance(enums, dict):
        raise ValueError("enums must be a mapping")
    enum_attributes = {
        name
        for name, definition in attributes.items()
        if definition.get("type") == "enum"
    }
    if set(enums) != enum_attributes:
        raise ValueError(
            "enum tables must exactly match enum attributes: "
            f"missing={sorted(enum_attributes - set(enums))}, "
            f"unknown={sorted(set(enums) - enum_attributes)}"
        )
    for name, values in enums.items():
        if (
            not isinstance(values, list)
            or not values
            or not all(
                isinstance(value, str)
                and ENUM_VALUE_PATTERN.fullmatch(value) is not None
                for value in values
            )
            or len(values) != len(set(values))
        ):
            raise ValueError(f"enum {name!r} must contain unique string values")
        _validate_generated_symbols(f"enum {name!r}", values)
        if name == "reason_code":
            _validate_reserved_symbols(
                "enum 'reason_code'",
                values,
                FRONTEND_REASON_SYMBOL_RESERVED,
            )
    reason_codes = set(enums.get("reason_code", []))
    for name, definition in envelope_attributes.items():
        if definition["missingReason"] not in reason_codes:
            raise ValueError(
                f"envelope attribute {name!r} references unknown missingReason "
                f"{definition['missingReason']!r}"
            )

    points = data.get("points")
    if not isinstance(points, list) or not points:
        raise ValueError("points must be a non-empty sequence")
    symbols: set[str] = set()
    ids: set[str] = set()
    for point in points:
        if not isinstance(point, dict):
            raise ValueError("point definitions must be mappings")
        if set(point) != POINT_KEYS:
            raise ValueError(
                "point keys do not match the contract: "
                f"missing={sorted(POINT_KEYS - set(point))}, "
                f"unknown={sorted(set(point) - POINT_KEYS)}"
            )
        symbol = point.get("symbol")
        point_id = point.get("id")
        producer = point.get("producer")
        required = point.get("requiredAttributes")
        if not isinstance(symbol, str) or SYMBOL_PATTERN.fullmatch(symbol) is None:
            raise ValueError(f"invalid point symbol {symbol!r}")
        if symbol in symbols:
            raise ValueError(f"duplicate point symbol {symbol!r}")
        if symbol in POINT_SYMBOL_RESERVED:
            raise ValueError(
                f"point symbol {symbol!r} collides with reserved generated "
                f"identifier {POINT_SYMBOL_RESERVED[symbol]}"
            )
        symbols.add(symbol)
        if (
            not isinstance(point_id, str)
            or POINT_ID_PATTERN.fullmatch(point_id) is None
        ):
            raise ValueError(f"invalid point id {point_id!r}")
        if point_id in ids:
            raise ValueError(f"duplicate point id {point_id!r}")
        ids.add(point_id)
        if producer not in PRODUCERS:
            raise ValueError(f"invalid producer for {point_id!r}")
        if not point_id.startswith(PRODUCER_POINT_PREFIXES[producer]):
            raise ValueError(f"point {point_id!r} does not match producer {producer!r}")
        if not isinstance(required, list) or not required:
            raise ValueError(f"{point_id!r} must declare requiredAttributes")
        if not all(isinstance(name, str) for name in required) or len(required) != len(
            set(required)
        ):
            raise ValueError(f"{point_id!r} requiredAttributes must be unique strings")
        unknown = set(required) - set(attributes)
        if unknown:
            raise ValueError(
                f"{point_id!r} has unknown required attributes: {sorted(unknown)}"
            )

    pipeline_fault_id = str(metadata["pipelineFaultPoint"]).removeprefix(
        str(metadata["wirePrefix"])
    )
    pipeline_fault = next(
        (point for point in points if point["id"] == pipeline_fault_id),
        None,
    )
    if pipeline_fault is None:
        raise ValueError("metadata.pipelineFaultPoint must reference a canonical point")
    if pipeline_fault["producer"] != "frontend":
        raise ValueError("the pipeline fault point must be produced by frontend")
    if "reason_code" not in pipeline_fault["requiredAttributes"]:
        raise ValueError("the pipeline fault point must require reason_code")
    return sorted(points, key=lambda item: str(item["id"]))


def _render_value_enum(type_name: str, values: list[str]) -> str:
    enum_lines = "\n".join(f"  k{_enum_symbol(value)}," for value in values)
    case_lines = "\n".join(
        f'    case {type_name}::k{_enum_symbol(value)}: return "{value}";'
        for value in values
    )
    return f"""enum class {type_name} {{
{enum_lines}
}};

[[nodiscard]] constexpr std::string_view {type_name}Name({type_name} value) {{
  switch (value) {{
{case_lines}
  }}
  return {{}};
}}
"""


def _render_cpp(
    schema_bytes: bytes,
    data: dict[str, object],
    points: list[dict[str, object]],
) -> str:
    digest = hashlib.sha256(schema_bytes).hexdigest()
    metadata = data["metadata"]
    envelope_attributes = data["envelopeAttributes"]
    attributes = data["attributes"]
    assert isinstance(metadata, dict)
    assert isinstance(envelope_attributes, dict)
    assert isinstance(attributes, dict)
    schema_name = str(metadata["name"])
    schema_version = schema_name.rsplit("/", maxsplit=1)[-1]
    sorted_attributes = sorted(attributes)
    sorted_envelope_attributes = sorted(envelope_attributes)
    wire_type_symbols = {
        "int64": "FlowWireType::kInt64",
        "string": "FlowWireType::kString",
        "enum": "FlowWireType::kEnum",
    }
    envelope_requirement_symbols = {
        "always": "FlowEnvelopeRequirement::kAlways",
        "business": "FlowEnvelopeRequirement::kBusiness",
        "optional": "FlowEnvelopeRequirement::kOptional",
    }
    envelope_attribute_enum_lines = "\n".join(
        f"  k{_enum_symbol(name)}," for name in sorted_envelope_attributes
    )
    envelope_attribute_name_lines = "\n".join(
        '    case FlowEnvelopeAttribute::k{}: return "{}";'.format(
            _enum_symbol(name), name
        )
        for name in sorted_envelope_attributes
    )
    envelope_attribute_type_lines = "\n".join(
        "    case FlowEnvelopeAttribute::k{}: return {};".format(
            _enum_symbol(name), wire_type_symbols[envelope_attributes[name]["type"]]
        )
        for name in sorted_envelope_attributes
    )
    envelope_attribute_requirement_lines = "\n".join(
        "    case FlowEnvelopeAttribute::k{}: return {};".format(
            _enum_symbol(name),
            envelope_requirement_symbols[envelope_attributes[name]["required"]],
        )
        for name in sorted_envelope_attributes
    )
    envelope_attribute_missing_reason_lines = "\n".join(
        '    case FlowEnvelopeAttribute::k{}: return "{}";'.format(
            _enum_symbol(name), envelope_attributes[name]["missingReason"]
        )
        for name in sorted_envelope_attributes
    )
    attribute_enum_lines = "\n".join(
        f"  k{_enum_symbol(name)} = FlowAttributeMask{{1}} << {index},"
        for index, name in enumerate(sorted_attributes)
    )
    attribute_name_lines = "\n".join(
        '    case FlowAttribute::k{}: return "{}";'.format(
            _enum_symbol(name), name
        )
        for name in sorted_attributes
    )
    attribute_type_lines = "\n".join(
        "    case FlowAttribute::k{}: return {};".format(
            _enum_symbol(name), wire_type_symbols[attributes[name]["type"]]
        )
        for name in sorted_attributes
    )
    enum_lines = "\n".join(f"  k{point['symbol']}," for point in points)
    case_lines = "\n".join(
        '    case FlowPoint::k{}: return "{}";'.format(point["symbol"], point["id"])
        for point in points
    )
    producer_lines = "\n".join(
        '    case FlowPoint::k{}: return "{}";'.format(
            point["symbol"], point["producer"]
        )
        for point in points
    )
    required_attribute_lines = "\n".join(
        "    case FlowPoint::k{}: return {};".format(
            point["symbol"],
            " | ".join(
                "FlowAttributeBit(FlowAttribute::k{})".format(_enum_symbol(attribute))
                for attribute in point["requiredAttributes"]
            ),
        )
        for point in points
    )
    enums = data["enums"]
    assert isinstance(enums, dict)
    value_enums = "\n".join(
        _render_value_enum(type_name, list(enums[schema_name]))
        for schema_name, type_name in (
            ("operation", "FlowOperation"),
            ("outcome", "FlowOutcome"),
            ("reason_code", "FlowReasonCode"),
        )
    ).rstrip()
    return f"""// Generated by scripts/generate_execution_flow_schema.py. DO NOT EDIT.
// Source: schemas/execution-flow/v1.yaml
// SHA256: {digest}
#pragma once

#include <cstdint>
#include <string_view>

namespace crane {{

// clang-format off

inline constexpr std::string_view kExecutionFlowSchemaSha256 = "{digest}";
inline constexpr std::string_view kExecutionFlowSchemaName = "{schema_name}";
inline constexpr std::string_view kExecutionFlowSchemaVersion = "{schema_version}";
inline constexpr std::string_view kExecutionFlowWirePrefix = "{metadata["wirePrefix"]}";
inline constexpr std::string_view kExecutionFlowHeartbeatPoint = "{metadata["heartbeatPoint"]}";
inline constexpr std::string_view kExecutionFlowPipelineFaultPoint = "{metadata["pipelineFaultPoint"]}";

enum class FlowWireType {{
  kInt64,
  kString,
  kEnum,
}};

enum class FlowEnvelopeRequirement {{
  kAlways,
  kBusiness,
  kOptional,
}};

enum class FlowEnvelopeAttribute {{
{envelope_attribute_enum_lines}
  kCount,
}};

[[nodiscard]] constexpr std::string_view FlowEnvelopeAttributeName(
    FlowEnvelopeAttribute attribute) {{
  switch (attribute) {{
{envelope_attribute_name_lines}
    case FlowEnvelopeAttribute::kCount: break;
  }}
  return {{}};
}}

[[nodiscard]] constexpr FlowWireType FlowEnvelopeAttributeWireType(
    FlowEnvelopeAttribute attribute) {{
  switch (attribute) {{
{envelope_attribute_type_lines}
    case FlowEnvelopeAttribute::kCount: break;
  }}
  return FlowWireType::kString;
}}

[[nodiscard]] constexpr FlowEnvelopeRequirement
FlowEnvelopeAttributeRequirement(FlowEnvelopeAttribute attribute) {{
  switch (attribute) {{
{envelope_attribute_requirement_lines}
    case FlowEnvelopeAttribute::kCount: break;
  }}
  return FlowEnvelopeRequirement::kOptional;
}}

[[nodiscard]] constexpr std::string_view FlowEnvelopeAttributeMissingReason(
    FlowEnvelopeAttribute attribute) {{
  switch (attribute) {{
{envelope_attribute_missing_reason_lines}
    case FlowEnvelopeAttribute::kCount: break;
  }}
  return {{}};
}}

using FlowAttributeMask = std::uint16_t;

enum class FlowAttribute : FlowAttributeMask {{
{attribute_enum_lines}
}};

[[nodiscard]] constexpr std::string_view FlowAttributeName(
    FlowAttribute attribute) {{
  switch (attribute) {{
{attribute_name_lines}
  }}
  return {{}};
}}

[[nodiscard]] constexpr FlowWireType FlowAttributeWireType(
    FlowAttribute attribute) {{
  switch (attribute) {{
{attribute_type_lines}
  }}
  return FlowWireType::kString;
}}

[[nodiscard]] constexpr FlowAttributeMask FlowAttributeBit(
    FlowAttribute attribute) {{
  return static_cast<FlowAttributeMask>(attribute);
}}

[[nodiscard]] constexpr bool FlowAttributesContain(
    FlowAttributeMask present, FlowAttributeMask required) {{
  return (present & required) == required;
}}

enum class FlowPoint {{
{enum_lines}
  kCount,
}};

[[nodiscard]] constexpr std::string_view FlowPointName(FlowPoint point) {{
  switch (point) {{
{case_lines}
    case FlowPoint::kCount: break;
  }}
  return {{}};
}}

[[nodiscard]] constexpr std::string_view FlowPointProducer(FlowPoint point) {{
  switch (point) {{
{producer_lines}
    case FlowPoint::kCount: break;
  }}
  return {{}};
}}

[[nodiscard]] constexpr FlowAttributeMask FlowPointRequiredAttributes(
    FlowPoint point) {{
  switch (point) {{
{required_attribute_lines}
    case FlowPoint::kCount: break;
  }}
  return {{}};
}}

[[nodiscard]] constexpr bool FlowPointRequires(FlowPoint point,
                                                FlowAttribute attribute) {{
  return FlowAttributesContain(FlowPointRequiredAttributes(point),
                               FlowAttributeBit(attribute));
}}

{value_enums}

// clang-format on

}}  // namespace crane
"""


def _go_string(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError(f"Go string value must be text, got {type(value).__name__}")
    return json.dumps(value, ensure_ascii=True)


def _gofmt(source: str) -> str:
    try:
        result = subprocess.run(
            ["gofmt"],
            input=source,
            text=True,
            capture_output=True,
            check=False,
        )
    except OSError as exc:
        raise ValueError(f"cannot execute gofmt: {exc}") from exc
    if result.returncode != 0:
        diagnostic = result.stderr.strip() or "unknown formatting error"
        raise ValueError(f"gofmt rejected generated source: {diagnostic}")
    return result.stdout


def _render_frontend_go(
    schema_bytes: bytes,
    data: dict[str, object],
    points: list[dict[str, object]],
) -> str:
    digest = hashlib.sha256(schema_bytes).hexdigest()
    envelope_attributes = data["envelopeAttributes"]
    storage_attributes = data["storageAttributes"]
    attributes = data["attributes"]
    enums = data["enums"]
    metadata = data["metadata"]
    assert isinstance(envelope_attributes, dict)
    assert isinstance(storage_attributes, dict)
    assert isinstance(attributes, dict)
    assert isinstance(enums, dict)
    assert isinstance(metadata, dict)
    schema_name = str(metadata["name"])
    schema_version = schema_name.rsplit("/", maxsplit=1)[-1]

    envelope_constant_lines = "\n".join(
        "\texecutionFlowEnvelope{} = {}".format(
            _go_symbol(name), _go_string(name)
        )
        for name in sorted(envelope_attributes)
    )
    envelope_attribute_lines = "\n".join(
        "\t{}: {{Name: {}, Type: {}, Requirement: "
        "executionFlowEnvelopeRequired{}, MissingReason: "
        "executionFlowReason{}}},".format(
            _go_string(name),
            _go_string(name),
            _go_string(definition["type"]),
            _enum_symbol(definition["required"]),
            _enum_symbol(definition["missingReason"]),
        )
        for name, definition in sorted(envelope_attributes.items())
    )
    envelope_attribute_list_lines = "\n".join(
        f"\t\tgeneratedExecutionFlowEnvelopeAttributes[{_go_string(name)}],"
        for name in sorted(envelope_attributes)
    )
    storage_constant_lines = "\n".join(
        "\texecutionFlowStorage{} = {}".format(
            _go_symbol(name), _go_string(name)
        )
        for name in sorted(storage_attributes)
    )
    storage_attribute_lines = "\n".join(
        "\t{}: {{Name: {}, Type: {}, Kind: executionFlowStorage{}, "
        "Source: {}, Wire: {}, Minimum: {}, Maximum: {}, "
        "HasMinimum: {}, HasMaximum: {}}},".format(
            _go_string(name),
            _go_string(name),
            _go_string(definition["type"]),
            _enum_symbol(definition["kind"]),
            _go_string(definition["source"]),
            json.dumps(definition["wire"]),
            definition["minimum"] if definition["minimum"] is not None else 0,
            definition["maximum"] if definition["maximum"] is not None else 0,
            json.dumps(definition["minimum"] is not None),
            json.dumps(definition["maximum"] is not None),
        )
        for name, definition in sorted(storage_attributes.items())
    )
    storage_attribute_list_lines = "\n".join(
        f"\t\tgeneratedExecutionFlowStorageAttributes[{_go_string(name)}],"
        for name in sorted(storage_attributes)
    )
    attribute_lines = "\n".join(
        f"\t{_go_string(name)}: {_go_string(definition['type'])},"
        for name, definition in sorted(attributes.items())
    )
    enum_groups = []
    for name, values in sorted(enums.items()):
        enum_lines = "\n".join(
            f"\t\t{_go_string(value)}: {{}}," for value in sorted(values)
        )
        enum_groups.append(f"\t{_go_string(name)}: {{\n{enum_lines}\n\t}},")
    reason_code_lines = "\n".join(
        "\texecutionFlowReason{} executionFlowReasonCode = {}".format(
            _enum_symbol(value), _go_string(value)
        )
        for value in enums["reason_code"]
    )
    point_lines = []
    for point in points:
        required = ", ".join(_go_string(value) for value in point["requiredAttributes"])
        point_lines.append(
            f"\t{_go_string(point['id'])}: {{\n"
            f"\t\tProducer: {_go_string(point['producer'])},\n"
            f"\t\tRequiredAttributes: []string{{{required}}},\n"
            "\t},"
        )
    return _gofmt(f"""// Code generated by CraneSched/scripts/generate_execution_flow_schema.py. DO NOT EDIT.
// Source: CraneSched/schemas/execution-flow/v1.yaml
// SHA256: {digest}

package main

const (
\texecutionFlowSchemaSHA256 = {_go_string(digest)}
\texecutionFlowSchemaName = {_go_string(schema_name)}
\texecutionFlowSchemaVersion = {_go_string(schema_version)}
\texecutionFlowWirePrefix = {_go_string(metadata["wirePrefix"])}
\texecutionFlowHeartbeatPoint = {_go_string(metadata["heartbeatPoint"])}
\texecutionFlowPipelineFaultPoint = {_go_string(metadata["pipelineFaultPoint"])}
{envelope_constant_lines}
{storage_constant_lines}
)

type executionFlowReasonCode string

const (
{reason_code_lines}
)

var generatedExecutionFlowEnvelopeAttributes = map[string]executionFlowEnvelopeAttributeSpec{{
{envelope_attribute_lines}
}}

var generatedExecutionFlowStorageAttributes = map[string]executionFlowStorageAttributeSpec{{
{storage_attribute_lines}
}}

var generatedExecutionFlowAttributeTypes = map[string]string{{
{attribute_lines}
}}

var generatedExecutionFlowEnumValues = map[string]map[string]struct{{}}{{
{chr(10).join(enum_groups)}
}}

var generatedExecutionFlowPoints = map[string]executionFlowPointSpec{{
{chr(10).join(point_lines)}
}}

type generatedExecutionFlowCatalogData struct{{}}

var generatedExecutionFlowCatalog executionFlowSchemaCatalog =
\tgeneratedExecutionFlowCatalogData{{}}

func (generatedExecutionFlowCatalogData) SchemaSHA256() string {{
\treturn executionFlowSchemaSHA256
}}

func (generatedExecutionFlowCatalogData) SchemaName() string {{
\treturn executionFlowSchemaName
}}

func (generatedExecutionFlowCatalogData) SchemaVersion() string {{
\treturn executionFlowSchemaVersion
}}

func (generatedExecutionFlowCatalogData) WirePrefix() string {{
\treturn executionFlowWirePrefix
}}

func (generatedExecutionFlowCatalogData) HeartbeatPoint() string {{
\treturn executionFlowHeartbeatPoint
}}

func (generatedExecutionFlowCatalogData) PipelineFaultPoint() string {{
\treturn executionFlowPipelineFaultPoint
}}

func (generatedExecutionFlowCatalogData) Point(name string) (executionFlowPointSpec, bool) {{
\tpoint, ok := generatedExecutionFlowPoints[name]
\tif !ok {{
\t\treturn executionFlowPointSpec{{}}, false
\t}}
\tpoint.RequiredAttributes = append([]string(nil), point.RequiredAttributes...)
\treturn point, ok
}}

func (generatedExecutionFlowCatalogData) EnvelopeAttributeType(name string) (string, bool) {{
\tattribute, ok := generatedExecutionFlowEnvelopeAttributes[name]
\treturn attribute.Type, ok
}}

func (generatedExecutionFlowCatalogData) AllowsEnvelopeAttribute(name string) bool {{
\t_, ok := generatedExecutionFlowEnvelopeAttributes[name]
\treturn ok
}}

func (generatedExecutionFlowCatalogData) EnvelopeAttributes() []executionFlowEnvelopeAttributeSpec {{
\treturn []executionFlowEnvelopeAttributeSpec{{
{envelope_attribute_list_lines}
\t}}
}}

func (generatedExecutionFlowCatalogData) StorageAttribute(name string) (executionFlowStorageAttributeSpec, bool) {{
\tattribute, ok := generatedExecutionFlowStorageAttributes[name]
\treturn attribute, ok
}}

func (generatedExecutionFlowCatalogData) AllowsStorageAttribute(name string) bool {{
\t_, ok := generatedExecutionFlowStorageAttributes[name]
\treturn ok
}}

func (generatedExecutionFlowCatalogData) StorageAttributes() []executionFlowStorageAttributeSpec {{
\treturn []executionFlowStorageAttributeSpec{{
{storage_attribute_list_lines}
\t}}
}}

func (generatedExecutionFlowCatalogData) AttributeType(name string) (string, bool) {{
\tattributeType, ok := generatedExecutionFlowAttributeTypes[name]
\treturn attributeType, ok
}}

func (generatedExecutionFlowCatalogData) AllowsAttribute(name string) bool {{
\t_, ok := generatedExecutionFlowAttributeTypes[name]
\treturn ok
}}

func (generatedExecutionFlowCatalogData) AllowsEnumValue(attribute string, value string) bool {{
\tvalues, ok := generatedExecutionFlowEnumValues[attribute]
\tif !ok {{
\t\treturn false
\t}}
\t_, ok = values[value]
\treturn ok
}}

""")


def _render_autotest_go(
    schema_bytes: bytes,
    data: dict[str, object],
    points: list[dict[str, object]],
) -> str:
    digest = hashlib.sha256(schema_bytes).hexdigest()
    metadata = data["metadata"]
    envelope_attributes = data["envelopeAttributes"]
    storage_attributes = data["storageAttributes"]
    attributes = data["attributes"]
    enums = data["enums"]
    assert isinstance(metadata, dict)
    assert isinstance(envelope_attributes, dict)
    assert isinstance(storage_attributes, dict)
    assert isinstance(attributes, dict)
    assert isinstance(enums, dict)
    wire_prefix = str(metadata["wirePrefix"])
    schema_name = str(metadata["name"])
    schema_version = schema_name.rsplit("/", maxsplit=1)[-1]

    attribute_type_symbols = {
        "int64": "AttributeInt64",
        "string": "AttributeString",
        "enum": "AttributeEnum",
    }
    envelope_constant_lines = "\n".join(
        "\tEnvelope{} = {}".format(_go_symbol(name), _go_string(name))
        for name in sorted(envelope_attributes)
    )
    envelope_requirement_symbols = {
        "always": "EnvelopeRequiredAlways",
        "business": "EnvelopeRequiredBusiness",
        "optional": "EnvelopeOptional",
    }
    envelope_attribute_lines = "\n".join(
        "\t{}: {{Type: {}, Requirement: {}, MissingReason: {}}},".format(
            _go_string(name),
            attribute_type_symbols[definition["type"]],
            envelope_requirement_symbols[definition["required"]],
            _go_string(definition["missingReason"]),
        )
        for name, definition in sorted(envelope_attributes.items())
    )
    envelope_name_lines = "\n".join(
        f"\t\t{_go_string(name)}," for name in sorted(envelope_attributes)
    )
    storage_constant_lines = "\n".join(
        "\tStorage{} = {}".format(_go_symbol(name), _go_string(name))
        for name in sorted(storage_attributes)
    )
    storage_attribute_lines = "\n".join(
        "\t{}: {{Type: {}, Kind: Storage{}, Source: {}, Wire: {}, "
        "Minimum: {}, Maximum: {}, HasMinimum: {}, HasMaximum: {}}},".format(
            _go_string(name),
            attribute_type_symbols[definition["type"]],
            _enum_symbol(definition["kind"]),
            _go_string(definition["source"]),
            json.dumps(definition["wire"]),
            definition["minimum"] if definition["minimum"] is not None else 0,
            definition["maximum"] if definition["maximum"] is not None else 0,
            json.dumps(definition["minimum"] is not None),
            json.dumps(definition["maximum"] is not None),
        )
        for name, definition in sorted(storage_attributes.items())
    )
    storage_name_lines = "\n".join(
        f"\t\t{_go_string(name)}," for name in sorted(storage_attributes)
    )
    attribute_lines = "\n".join(
        f"\t{_go_string(name)}: {{Type: {attribute_type_symbols[definition['type']]} }},"
        for name, definition in sorted(attributes.items())
    )
    enum_groups = []
    for name, values in sorted(enums.items()):
        enum_lines = "\n".join(
            f"\t\t{_go_string(value)}: {{}}," for value in sorted(values)
        )
        enum_groups.append(f"\t{_go_string(name)}: {{\n{enum_lines}\n\t}},")
    point_lines = []
    for point in points:
        required = ", ".join(_go_string(value) for value in point["requiredAttributes"])
        point_name = wire_prefix + str(point["id"])
        point_lines.append(
            f"\t{_go_string(point_name)}: {{\n"
            f"\t\tID: {_go_string(point_name)},\n"
            f"\t\tProducer: {_go_string(point['producer'])},\n"
            f"\t\tRequiredAttributes: []string{{{required}}},\n"
            "\t},"
        )
    point_name_lines = "\n".join(
        f"\t\t{_go_string(wire_prefix + str(point['id']))}," for point in points
    )

    return _gofmt(f"""// Code generated by CraneSched/scripts/generate_execution_flow_schema.py. DO NOT EDIT.
// Source: CraneSched/schemas/execution-flow/v1.yaml
// SHA256: {digest}

package schema

const (
\tAPIVersion = {_go_string(data["apiVersion"])}
\tName = {_go_string(schema_name)}
\tVersion = {_go_string(schema_version)}
\tWirePrefix = {_go_string(wire_prefix)}
\tHeartbeatPoint = {_go_string(metadata["heartbeatPoint"])}
\tPipelineFaultPoint = {_go_string(metadata["pipelineFaultPoint"])}
\tSHA256 = {_go_string(digest)}
{envelope_constant_lines}
{storage_constant_lines}
)

type AttributeType string

const (
\tAttributeInt64 AttributeType = "int64"
\tAttributeString AttributeType = "string"
\tAttributeEnum AttributeType = "enum"
)

type AttributeDefinition struct {{
\tType AttributeType
}}

type EnvelopeRequirement string

const (
\tEnvelopeRequiredAlways EnvelopeRequirement = "always"
\tEnvelopeRequiredBusiness EnvelopeRequirement = "business"
\tEnvelopeOptional EnvelopeRequirement = "optional"
)

type EnvelopeAttributeDefinition struct {{
\tType AttributeType
\tRequirement EnvelopeRequirement
\tMissingReason string
}}

type StorageKind string

const (
\tStorageField StorageKind = "field"
\tStorageTag StorageKind = "tag"
)

type StorageAttributeDefinition struct {{
\tType AttributeType
\tKind StorageKind
\tSource string
\tWire bool
\tMinimum int64
\tMaximum int64
\tHasMinimum bool
\tHasMaximum bool
}}

type PointDefinition struct {{
\tID string
\tProducer string
\tRequiredAttributes []string
}}

var envelopeAttributes = map[string]EnvelopeAttributeDefinition{{
{envelope_attribute_lines}
}}

var storageAttributes = map[string]StorageAttributeDefinition{{
{storage_attribute_lines}
}}

var attributes = map[string]AttributeDefinition{{
{attribute_lines}
}}

var enumValues = map[string]map[string]struct{{}}{{
{chr(10).join(enum_groups)}
}}

var points = map[string]PointDefinition{{
{chr(10).join(point_lines)}
}}

func Point(name string) (PointDefinition, bool) {{
\tpoint, ok := points[name]
\tif !ok {{
\t\treturn PointDefinition{{}}, false
\t}}
\tpoint.RequiredAttributes = append([]string(nil), point.RequiredAttributes...)
\treturn point, ok
}}

func PointNames() []string {{
\treturn []string{{
{point_name_lines}
\t}}
}}

func EnvelopeAttribute(name string) (EnvelopeAttributeDefinition, bool) {{
\tattribute, ok := envelopeAttributes[name]
\treturn attribute, ok
}}

func EnvelopeAttributeNames() []string {{
\treturn []string{{
{envelope_name_lines}
\t}}
}}

func StorageAttribute(name string) (StorageAttributeDefinition, bool) {{
\tattribute, ok := storageAttributes[name]
\treturn attribute, ok
}}

func StorageAttributeNames() []string {{
\treturn []string{{
{storage_name_lines}
\t}}
}}

func Attribute(name string) (AttributeDefinition, bool) {{
\tattribute, ok := attributes[name]
\treturn attribute, ok
}}

func ValidEnum(attribute string, value string) bool {{
\tvalues, ok := enumValues[attribute]
\tif !ok {{
\t\treturn false
\t}}
\t_, ok = values[value]
\treturn ok
}}
""")


def _render_autotest_python(
    schema_bytes: bytes,
    data: dict[str, object],
) -> str:
    digest = hashlib.sha256(schema_bytes).hexdigest()
    metadata = data["metadata"]
    envelope_attributes = data["envelopeAttributes"]
    storage_attributes = data["storageAttributes"]
    assert isinstance(metadata, dict)
    assert isinstance(envelope_attributes, dict)
    assert isinstance(storage_attributes, dict)
    schema_name = str(metadata["name"])
    schema_version = schema_name.rsplit("/", maxsplit=1)[-1]
    values = {
        "API_VERSION": data["apiVersion"],
        "NAME": schema_name,
        "VERSION": schema_version,
        "WIRE_PREFIX": metadata["wirePrefix"],
        "HEARTBEAT_POINT": metadata["heartbeatPoint"],
        "PIPELINE_FAULT_POINT": metadata["pipelineFaultPoint"],
        "SHA256": digest,
        "ENVELOPE_ATTRIBUTES": {
            name: {
                "type": definition["type"],
                "required": definition["required"],
                "missing_reason": definition["missingReason"],
            }
            for name, definition in sorted(envelope_attributes.items())
        },
        "STORAGE_ATTRIBUTES": {
            name: {
                "type": definition["type"],
                "kind": definition["kind"],
                "source": definition["source"],
                "wire": definition["wire"],
                "minimum": definition["minimum"],
                "maximum": definition["maximum"],
            }
            for name, definition in sorted(storage_attributes.items())
        },
    }
    # This target is Python source, not JSON. repr() keeps booleans and nulls
    # importable as False/True/None. The sorted nested inputs above make the
    # generated dictionaries deterministic.
    assignments = "\n".join(f"{name} = {value!r}" for name, value in values.items())
    exports = ",\n    ".join(json.dumps(name) for name in sorted(values))
    return f'''# Code generated by CraneSched/scripts/generate_execution_flow_schema.py. DO NOT EDIT.
# Source: CraneSched/schemas/execution-flow/v1.yaml
# SHA256: {digest}

# fmt: off
{assignments}
# fmt: on

__all__ = (
    {exports},
)
'''


def _write_or_check(path: Path, generated: str, *, check: bool) -> bool:
    if check:
        try:
            current = path.read_text(encoding="utf-8")
        except OSError as exc:
            print(f"cannot read generated catalog {path}: {exc}", file=sys.stderr)
            return False
        if current != generated:
            print(
                f"generated execution-flow catalog is stale: {path}",
                file=sys.stderr,
            )
            return False
        return True

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(generated, encoding="utf-8")
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--frontend-output", type=Path)
    parser.add_argument("--autotest-output", type=Path)
    parser.add_argument("--autotest-python-output", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    try:
        schema_bytes = args.schema.read_bytes()
        data = _load_schema(args.schema)
        points = _validate(data)
        outputs = [(args.output, _render_cpp(schema_bytes, data, points))]
        if args.frontend_output is not None:
            outputs.append(
                (
                    args.frontend_output,
                    _render_frontend_go(schema_bytes, data, points),
                )
            )
        if args.autotest_output is not None:
            outputs.append(
                (
                    args.autotest_output,
                    _render_autotest_go(schema_bytes, data, points),
                )
            )
        if args.autotest_python_output is not None:
            outputs.append(
                (
                    args.autotest_python_output,
                    _render_autotest_python(schema_bytes, data),
                )
            )
    except (OSError, ValueError, yaml.YAMLError) as exc:
        print(f"execution-flow schema error: {exc}", file=sys.stderr)
        return 2

    return (
        0
        if all(
            _write_or_check(path, generated, check=args.check)
            for path, generated in outputs
        )
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
