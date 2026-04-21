#!/usr/bin/env python3
"""Query and visualize CraneSched distributed traces from InfluxDB.

Supports flat table output, tree-based flame graph visualization,
and Chrome Trace Event Format export for chrome://tracing and Perfetto UI.
"""

import os
import argparse
import sys
import re
import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Optional

try:
    from influxdb_client import InfluxDBClient
except ImportError:
    print("Error: 'influxdb-client' is not installed.")
    print("Install: pip install influxdb-client")
    sys.exit(1)


# ── Data Model ──────────────────────────────────────────────────────────────

@dataclass
class Span:
    trace_id: str = ""
    span_id: str = ""
    parent_span_id: str = ""
    name: str = ""
    service: str = ""
    duration_us: int = 0
    start_time: Optional[object] = None
    attrs: dict = field(default_factory=dict)

    @property
    def duration_ms(self) -> float:
        return self.duration_us / 1000.0

    @property
    def label(self) -> str:
        svc = f"[{self.service}] " if self.service and self.service != "-" else ""
        return f"{svc}{self.name}"

    @property
    def is_root(self) -> bool:
        return (not self.parent_span_id or
                self.parent_span_id == "0000000000000000")


# ── CLI Args ────────────────────────────────────────────────────────────────

def positive_int(value):
    iv = int(value)
    if iv <= 0:
        raise argparse.ArgumentTypeError(f"Expected positive int, got: {value}")
    return iv


def setup_args():
    p = argparse.ArgumentParser(
        description="Query and visualize CraneSched traces from InfluxDB")

    # Connection
    p.add_argument("--url",    default=os.getenv("INFLUX_URL",   "http://localhost:8086"))
    p.add_argument("--token",  default=os.getenv("INFLUX_TOKEN", "your_token"))
    p.add_argument("--org",    default=os.getenv("INFLUX_ORG",   "your_organization"))
    p.add_argument("--bucket", default=os.getenv("TRACE_BUCKET", "your_trace_bucket_name"))

    # Filters
    p.add_argument("--job-id",   type=int,  help="Filter by Job ID")
    p.add_argument("--step-id",  type=int,  help="Filter by Step ID")
    p.add_argument("--task-id",  type=int,  help="Filter by Task ID")
    p.add_argument("--trace-id", type=str,  help="Filter by Trace ID")
    p.add_argument("--service",  type=str,  help="Filter by service (cranectld, craned, supervisor)")

    # Time and limits
    p.add_argument("--minutes", type=positive_int, default=60,
                   help="Look back N minutes (default: 60)")
    p.add_argument("--limit",   type=positive_int, default=200,
                   help="Max spans to return (default: 200)")

    # Output
    p.add_argument("--verbose", "-v", action="store_true", help="Show all attributes")
    p.add_argument("--tree",    "-t", action="store_true", help="Tree view (flame graph style)")
    p.add_argument("--chrome",  type=str, metavar="FILE",
                   help="Export to Chrome Trace Event Format JSON (for chrome://tracing or ui.perfetto.dev)")
    p.add_argument("--perfetto", type=str, metavar="FILE",
                   help="Export to Perfetto native proto format (.pftrace) with nested slices and flow arrows")
    p.add_argument("--system",  "-s", action="store_true",
                   help="System-level aggregate analysis (no job filter needed)")

    return p.parse_args()


# ── InfluxDB Query ──────────────────────────────────────────────────────────

def validate_input(val, pattern, name):
    if val is not None and not re.fullmatch(pattern, val):
        print(f"Error: Invalid format for {name}: {val}")
        sys.exit(1)


def build_flux_query(args):
    validate_input(args.trace_id, r'^[a-fA-F0-9]+$', "trace-id")
    validate_input(args.service,  r'^[a-zA-Z0-9_\-\.]+$', "service")
    validate_input(args.bucket,   r'^[a-zA-Z0-9][a-zA-Z0-9_\-\.]{0,127}$', "bucket")
    validate_input(args.org,      r'^[a-zA-Z0-9][a-zA-Z0-9_\-\.]{0,127}$', "org")

    query = f'''from(bucket: "{args.bucket}")
    |> range(start: -{args.minutes}m)
    |> filter(fn: (r) => r["_measurement"] == "spans")
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    if args.job_id is not None:
        query += f'|> filter(fn: (r) => exists r["job_id"] and string(v: r["job_id"]) == "{args.job_id}")\n'
    if args.step_id is not None:
        query += f'|> filter(fn: (r) => exists r["step_id"] and string(v: r["step_id"]) == "{args.step_id}")\n'
    if args.task_id is not None:
        query += f'|> filter(fn: (r) => exists r["task_id"] and string(v: r["task_id"]) == "{args.task_id}")\n'
    if args.trace_id is not None:
        query += f'|> filter(fn: (r) => r["trace_id"] == "{args.trace_id}")\n'
    if args.service is not None:
        query += f'|> filter(fn: (r) => r["service"] == "{args.service}")\n'

    query += f'''
    |> group(columns: [])
    |> sort(columns: ["_time"], desc: false)
    |> limit(n: {args.limit})
    '''
    return query


def query_spans(client, args) -> list[Span]:
    """Query InfluxDB and return a list of Span objects."""
    query_api = client.query_api()
    flux_query = build_flux_query(args)
    tables = query_api.query(flux_query)

    # Internal fields not to include in attrs
    INTERNAL = {
        "result", "table", "_start", "_stop", "_time", "_value",
        "_field", "_measurement",
        "trace_id", "span_id", "parent_span_id", "name",
        "duration_us", "service",
    }

    spans = []
    for table in tables:
        for record in table.records:
            v = record.values
            s = Span(
                trace_id=str(v.get("trace_id", "")),
                span_id=str(v.get("span_id", "")),
                parent_span_id=str(v.get("parent_span_id", "")),
                name=str(v.get("name", "")),
                service=str(v.get("service", "-")),
                duration_us=int(v.get("duration_us", 0) or 0),
                start_time=record.get_time(),
            )
            # InfluxDB _time is the span's end/ingestion time, not start.
            # Compute actual start_time by subtracting duration.
            if s.start_time and s.duration_us > 0:
                s.start_time = s.start_time - timedelta(microseconds=s.duration_us)
            s.attrs = {k: v2 for k, v2 in v.items()
                       if k not in INTERNAL and v2 is not None}
            spans.append(s)

    return spans


# ── Flat Table Output ───────────────────────────────────────────────────────

def print_flat(spans: list[Span], verbose: bool):
    if not spans:
        print("No traces found matching the criteria.")
        return

    print(f"\nFound {len(spans)} span(s):")
    print("-" * 105)
    print(f"{'Time':<25} | {'Service':<12} | {'Operation':<25} | {'Duration':<12} | {'Details'}")
    print("-" * 105)

    for s in spans:
        time_str = s.start_time.strftime("%Y-%m-%d %H:%M:%S.%f") if s.start_time else "-"
        details_parts = []
        for key in ("job_id", "step_id", "task_id"):
            val = s.attrs.get(key)
            if val is not None:
                details_parts.append(f"{key}={val}")
        details = ", ".join(details_parts)

        dur_str = format_duration(s.duration_us)
        print(f"{time_str:<25} | {s.service:<12} | {s.name:<25} | {dur_str:<12} | {details}")

        if verbose:
            exclude = {"job_id", "step_id", "task_id"}
            for k, v in s.attrs.items():
                if k not in exclude:
                    print(f"    {k}: {v}")
            print()


# ── Tree Output ─────────────────────────────────────────────────────────────

def build_tree(spans: list[Span]) -> dict[str, list[Span]]:
    """Group spans by trace_id, build parent→children map."""
    by_trace: dict[str, list[Span]] = defaultdict(list)
    for s in spans:
        by_trace[s.trace_id].append(s)
    return by_trace


def format_duration(us: int) -> str:
    if us < 1000:
        return f"{us}us"
    elif us < 1_000_000:
        return f"{us / 1000:.2f}ms"
    else:
        return f"{us / 1_000_000:.2f}s"


def make_bar(duration_us: int, max_duration_us: int, width: int = 20) -> str:
    if max_duration_us <= 0:
        return ""
    ratio = min(duration_us / max_duration_us, 1.0)
    filled = max(1, int(ratio * width))
    return "\u2588" * filled


def print_tree(spans: list[Span], verbose: bool):
    if not spans:
        print("No traces found matching the criteria.")
        return

    traces = build_tree(spans)
    for trace_id, trace_spans in traces.items():
        # Build lookup
        by_id: dict[str, Span] = {s.span_id: s for s in trace_spans}
        children: dict[str, list[Span]] = defaultdict(list)
        roots: list[Span] = []

        for s in trace_spans:
            if s.is_root:
                roots.append(s)
            elif s.parent_span_id in by_id:
                children[s.parent_span_id].append(s)
            else:
                # Orphan (parent not in this query result) — treat as root
                roots.append(s)

        # Sort children by start_time
        for k in children:
            children[k].sort(key=lambda x: x.start_time or x.name)
        roots.sort(key=lambda x: x.start_time or x.name)

        # Find max duration for bar scaling
        max_dur = max((s.duration_us for s in trace_spans), default=1) or 1

        # Header
        job_ids = {s.attrs.get("job_id") for s in trace_spans
                   if s.attrs.get("job_id") is not None}
        job_str = ", ".join(f"Job #{j}" for j in sorted(job_ids)) if job_ids else ""
        print(f"\nTrace {trace_id}" + (f"  ({job_str})" if job_str else ""))
        print("=" * 90)

        # Recursive render
        def render(span: Span, prefix: str, connector: str, is_last: bool):
            bar = make_bar(span.duration_us, max_dur)
            dur = format_duration(span.duration_us)
            label = span.label

            # Status indicator
            status = ""
            span_status = span.attrs.get("otel.status_code", "")
            if not span_status:
                # Check for error indicators
                exit_code = span.attrs.get("exit_code")
                if exit_code is not None and str(exit_code) != "0":
                    status = " [!]"

            line = f"{prefix}{connector}{label:<40} {bar:<22} {dur:>10}{status}"
            print(line)

            if verbose:
                v_prefix = prefix + ("    " if is_last else "\u2502   ")
                for k, v in span.attrs.items():
                    if k not in ("job_id", "step_id", "task_id"):
                        print(f"{v_prefix}  {k}: {v}")

            kids = children.get(span.span_id, [])
            child_prefix = prefix + ("    " if is_last else "\u2502   ")
            for i, child in enumerate(kids):
                last = (i == len(kids) - 1)
                conn = "\u2514\u2500\u2500 " if last else "\u251c\u2500\u2500 "
                render(child, child_prefix, conn, last)

        for i, root in enumerate(roots):
            if i > 0:
                print()  # Gap between root spans
            render(root, "", "", True)

        print()


# ── Chrome Trace Event Format Export ────────────────────────────────────────

def export_chrome(spans: list[Span], output_path: str):
    """Export spans to Chrome Trace Event Format JSON.

    Open the output file in:
      - chrome://tracing (Load button)
      - ui.perfetto.dev (drag & drop)
    """
    if not spans:
        print("No spans to export.")
        return

    # Assign stable pid per unique service
    services = sorted({s.service for s in spans})
    svc_to_pid = {svc: i + 1 for i, svc in enumerate(services)}

    # Assign tid per (pid, step_id) so related spans share a lane.
    # Spans without step_id use greedy lane allocation to avoid overlaps.
    span_tid: dict[str, int] = {}

    # Phase 1: assign tids by step_id within each pid
    pid_step_tid: dict[tuple[int, str], int] = {}  # (pid, step_id) → tid
    pid_next_tid: dict[int, int] = defaultdict(lambda: 1)  # pid → next available tid

    for s in spans:
        pid = svc_to_pid.get(s.service, 0)
        step_id = s.attrs.get("step_id")
        if step_id is not None:
            key = (pid, str(step_id))
            if key not in pid_step_tid:
                pid_step_tid[key] = pid_next_tid[pid]
                pid_next_tid[pid] += 1
            span_tid[s.span_id] = pid_step_tid[key]

    # Phase 2: greedy allocation for spans without step_id
    pid_lanes: dict[int, list[int]] = defaultdict(list)  # pid → [end_ts, ...]

    # Pre-fill lanes with step_id-assigned spans
    for s in sorted(spans, key=lambda x: int(x.start_time.timestamp() * 1_000_000) if x.start_time else 0):
        if s.span_id in span_tid:
            pid = svc_to_pid.get(s.service, 0)
            tid = span_tid[s.span_id]
            ts = int(s.start_time.timestamp() * 1_000_000) if s.start_time else 0
            end = ts + max(s.duration_us, 1)
            lanes = pid_lanes[pid]
            while len(lanes) < tid:
                lanes.append(0)
            lanes[tid - 1] = max(lanes[tid - 1], end)

    for s in sorted(spans, key=lambda x: int(x.start_time.timestamp() * 1_000_000) if x.start_time else 0):
        if s.span_id in span_tid:
            continue
        pid = svc_to_pid.get(s.service, 0)
        ts = int(s.start_time.timestamp() * 1_000_000) if s.start_time else 0
        end = ts + max(s.duration_us, 1)
        lanes = pid_lanes[pid]

        assigned = False
        for i, lane_end in enumerate(lanes):
            if ts >= lane_end:
                lanes[i] = end
                span_tid[s.span_id] = i + 1
                assigned = True
                break
        if not assigned:
            lanes.append(end)
            span_tid[s.span_id] = len(lanes)

    # Build step_type lookup: step_id → type name
    # step_type values: 1=daemon, 2=primary, 3=common
    STEP_TYPE_NAMES = {1: "daemon", 2: "primary", 3: "common"}
    step_type_map: dict[str, str] = {}  # step_id → type name
    for s in spans:
        st = s.attrs.get("step_type")
        sid = s.attrs.get("step_id")
        if st is not None and sid is not None:
            step_type_map[str(sid)] = STEP_TYPE_NAMES.get(int(st), "unknown")

    events = []

    # Metadata events: process names
    for svc, pid in svc_to_pid.items():
        events.append({
            "name": "process_name", "ph": "M", "pid": pid, "tid": 0,
            "args": {"name": svc if svc != "-" else "unknown"}
        })

    # Metadata events: thread names (Step N with type label)
    for (pid, step_id_str), tid in pid_step_tid.items():
        stype = step_type_map.get(step_id_str, "")
        label = f"Step {step_id_str} ({stype})" if stype else f"Step {step_id_str}"
        events.append({
            "name": "thread_name", "ph": "M", "pid": pid, "tid": tid,
            "args": {"name": label}
        })

    # Span events
    for s in spans:
        ts_us = int(s.start_time.timestamp() * 1_000_000) if s.start_time else 0
        pid = svc_to_pid.get(s.service, 0)
        tid = span_tid.get(s.span_id, 0)

        args = dict(s.attrs)
        args["trace_id"] = s.trace_id
        args["span_id"] = s.span_id
        if s.parent_span_id and s.parent_span_id != "0000000000000000":
            args["parent_span_id"] = s.parent_span_id

        # Use step_type as category for color grouping in Perfetto
        step_id_str = str(s.attrs.get("step_id", ""))
        cat = step_type_map.get(step_id_str, s.service)

        events.append({
            "name": s.name,
            "ph": "X",
            "ts": ts_us,
            "dur": max(s.duration_us, 1),  # Perfetto needs dur >= 1
            "pid": pid,
            "tid": tid,
            "cat": cat,
            "args": args,
        })

    # Flow events: connect step/supervisor_spawn → step/execute
    # Match by (job_id, step_id, node), where node is extracted from service name
    def extract_node(service: str) -> str:
        """Extract node from 'Craned@wrl04' or 'Supervisor@wrl04' → 'wrl04'."""
        if "@" in service:
            return service.split("@", 1)[1]
        return ""

    spawns: dict[tuple, Span] = {}  # (job_id, step_id, node) → spawn span
    for s in spans:
        if s.name == "step/supervisor_spawn":
            key = (s.attrs.get("job_id"), s.attrs.get("step_id"), extract_node(s.service))
            spawns[key] = s

    flow_id = 0
    for s in spans:
        if s.name == "step/execute":
            key = (s.attrs.get("job_id"), s.attrs.get("step_id"), extract_node(s.service))
            spawn = spawns.get(key)
            if spawn:
                flow_id += 1
                spawn_pid = svc_to_pid.get(spawn.service, 0)
                spawn_tid = span_tid.get(spawn.span_id, 0)
                spawn_end = int(spawn.start_time.timestamp() * 1_000_000) + spawn.duration_us if spawn.start_time else 0
                exec_ts = int(s.start_time.timestamp() * 1_000_000) if s.start_time else 0
                exec_pid = svc_to_pid.get(s.service, 0)
                exec_tid = span_tid.get(s.span_id, 0)
                # Flow start (at end of spawn)
                events.append({
                    "name": "spawn→exec", "ph": "s", "id": flow_id,
                    "pid": spawn_pid, "tid": spawn_tid, "ts": spawn_end, "cat": "flow",
                })
                # Flow end (at start of execute)
                events.append({
                    "name": "spawn→exec", "ph": "f", "id": flow_id, "bp": "e",
                    "pid": exec_pid, "tid": exec_tid, "ts": exec_ts, "cat": "flow",
                })

    with open(output_path, "w") as f:
        json.dump(events, f, indent=2, default=str)

    print(f"Exported {len(spans)} spans to {output_path}")
    print(f"Open in browser: chrome://tracing or https://ui.perfetto.dev")


# ── Perfetto Native Proto Export ───────────────────────────────────────────

def export_perfetto(spans: list[Span], output_path: str, system_mode: bool = False):
    """Export spans to Perfetto native protobuf format (.pftrace).

    Produces nested slices (proper flame graph stacking), flow arrows,
    and debug annotations. Open in https://ui.perfetto.dev
    """
    try:
        from perfetto.protos.perfetto.trace.perfetto_trace_pb2 import (
            Trace, TracePacket,
        )
    except ImportError:
        print("Error: 'perfetto' package not installed.")
        print("Install: pip install perfetto")
        return

    if not spans:
        print("No spans to export.")
        return

    STEP_TYPE_NAMES = {1: "daemon", 2: "primary", 3: "common"}
    SEQUENCE_ID = 1
    trace = Trace()

    # ── Track UUID allocation ──
    # One track per (service, job_id). All spans from the same service+job
    # share a track, and Perfetto auto-nests by temporal containment.
    # System mode: one track per (service, span_name) for overview.
    services = sorted({s.service for s in spans})
    svc_to_pid = {svc: i + 1 for i, svc in enumerate(services)}
    next_uuid = 100

    by_id = {s.span_id: s for s in spans}

    thread_uuid_map: dict[tuple[int, str], int] = {}
    thread_labels: dict[int, str] = {}

    def alloc_track(pid: int, group_key: str, label: str) -> int:
        nonlocal next_uuid
        key = (pid, group_key)
        if key not in thread_uuid_map:
            next_uuid += 1
            thread_uuid_map[key] = next_uuid
            thread_labels[next_uuid] = label
        return thread_uuid_map[key]

    span_track_uuid: dict[str, int] = {}
    for s in spans:
        pid = svc_to_pid.get(s.service, 0)
        if system_mode:
            uuid = alloc_track(pid, s.name, s.name)
        else:
            # Per-job mode: group by trace_id so different traces
            # (submit vs lifecycle) don't mix on the same track.
            uuid = alloc_track(pid, s.trace_id, s.trace_id[:12])
        span_track_uuid[s.span_id] = uuid

    # ── Emit TrackDescriptors ──

    # Process descriptors
    for svc, pid in svc_to_pid.items():
        pkt = trace.packet.add()
        pkt.trusted_packet_sequence_id = SEQUENCE_ID
        td = pkt.track_descriptor
        td.uuid = pid
        td.process.pid = pid
        td.process.process_name = svc if svc != "-" else "unknown"

    # Thread descriptors
    for (pid, _group_key), uuid in thread_uuid_map.items():
        pkt = trace.packet.add()
        pkt.trusted_packet_sequence_id = SEQUENCE_ID
        td = pkt.track_descriptor
        td.uuid = uuid
        td.parent_uuid = pid
        td.name = thread_labels.get(uuid, "")

    # ── Build event list sorted by timestamp ──
    # Perfetto requires packets in monotonic timestamp order within a sequence.
    # For proper nesting: at same timestamp, BEGINs of longer spans come first,
    # ENDs of shorter spans come first.

    # Build child lookup for flow events
    children_of: dict[str, list[Span]] = defaultdict(list)
    for s in spans:
        if s.parent_span_id and s.parent_span_id in by_id:
            children_of[s.parent_span_id].append(s)

    # Collect (sort_key, event_type, span) tuples
    # sort_key: (timestamp_ns, order) where order=0 for BEGIN, order=1 for END
    # At same timestamp: BEGIN with longer duration first (parent before child)
    # At same timestamp: END with shorter duration first (child before parent)
    events = []
    for s in spans:
        ts_us = int(s.start_time.timestamp() * 1_000_000) if s.start_time else 0
        dur = max(s.duration_us, 1)
        end_us = ts_us + dur
        # BEGIN: sort by (ts, 0, -duration) so longer (parent) spans come first
        events.append((ts_us * 1000, 0, -dur, s, "begin"))
        # END: sort by (end_ts, 1, duration) so shorter (child) spans end first
        events.append((end_us * 1000, 1, dur, s, "end"))

    events.sort(key=lambda e: (e[0], e[1], e[2]))

    # ── Emit sorted events as TracePackets ──
    for ts_ns, _ord, _dur, s, etype in events:
        track_uuid = span_track_uuid.get(s.span_id, 0)
        pkt = trace.packet.add()
        pkt.trusted_packet_sequence_id = SEQUENCE_ID
        pkt.timestamp = ts_ns
        te = pkt.track_event
        te.track_uuid = track_uuid

        if etype == "begin":
            te.type = te.TYPE_SLICE_BEGIN
            te.name = s.name

            if s.service:
                te.categories.append(s.service)
            step_type_val = s.attrs.get("step_type")
            if step_type_val is not None:
                stype = STEP_TYPE_NAMES.get(int(step_type_val), "")
                if stype:
                    te.categories.append(stype)

            # Debug annotations
            for k, v in s.attrs.items():
                da = te.debug_annotations.add()
                da.name = str(k)
                if isinstance(v, (int, float)):
                    if isinstance(v, float):
                        da.double_value = v
                    else:
                        da.int_value = int(v)
                else:
                    da.string_value = str(v)
            da = te.debug_annotations.add()
            da.name = "trace_id"
            da.string_value = s.trace_id
            da = te.debug_annotations.add()
            da.name = "span_id"
            da.string_value = s.span_id
            if s.parent_span_id and s.parent_span_id != "0000000000000000":
                da = te.debug_annotations.add()
                da.name = "parent_span_id"
                da.string_value = s.parent_span_id

            # Flow events
            if s.name == "step/supervisor_spawn":
                for child in children_of.get(s.span_id, []):
                    if child.name == "step/execute":
                        flow_id = int(child.span_id[:12], 16) & 0x7FFFFFFFFFFFFFFF
                        te.flow_ids.append(flow_id)
            if s.name == "step/execute" and s.parent_span_id in by_id:
                parent = by_id[s.parent_span_id]
                if parent.name == "step/supervisor_spawn":
                    flow_id = int(s.span_id[:12], 16) & 0x7FFFFFFFFFFFFFFF
                    te.flow_ids.append(flow_id)
        else:
            te.type = te.TYPE_SLICE_END

    # ── Set sequence flags on first event packet ──
    # Find first packet with track_event (skip descriptors)
    for pkt in trace.packet:
        if pkt.HasField("track_event"):
            pkt.sequence_flags = TracePacket.SEQ_INCREMENTAL_STATE_CLEARED
            break

    # ── Write binary proto ──
    with open(output_path, "wb") as f:
        f.write(trace.SerializeToString())

    print(f"Exported {len(spans)} spans to {output_path} (Perfetto native format)")
    print(f"Open: https://ui.perfetto.dev (drag in the .pftrace file)")


# ── System-Level Analysis ──────────────────────────────────────────────────

def query_all_spans(client, args) -> list[Span]:
    """Query ALL spans in the time window (no job filter)."""
    validate_input(args.bucket, r'^[a-zA-Z0-9][a-zA-Z0-9_\-\.]{0,127}$', "bucket")
    validate_input(args.org, r'^[a-zA-Z0-9][a-zA-Z0-9_\-\.]{0,127}$', "org")

    query = f'''from(bucket: "{args.bucket}")
    |> range(start: -{args.minutes}m)
    |> filter(fn: (r) => r["_measurement"] == "spans")
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    |> group(columns: [])
    |> sort(columns: ["_time"], desc: false)
    |> limit(n: {args.limit})
    '''

    query_api = client.query_api()
    tables = query_api.query(query)

    INTERNAL = {
        "result", "table", "_start", "_stop", "_time", "_value",
        "_field", "_measurement",
        "trace_id", "span_id", "parent_span_id", "name",
        "duration_us", "service",
    }

    spans = []
    for table in tables:
        for record in table.records:
            v = record.values
            s = Span(
                trace_id=str(v.get("trace_id", "")),
                span_id=str(v.get("span_id", "")),
                parent_span_id=str(v.get("parent_span_id", "")),
                name=str(v.get("name", "")),
                service=str(v.get("service", "-")),
                duration_us=int(v.get("duration_us", 0) or 0),
                start_time=record.get_time(),
            )
            if s.start_time and s.duration_us > 0:
                s.start_time = s.start_time - timedelta(microseconds=s.duration_us)
            s.attrs = {k: v2 for k, v2 in v.items()
                       if k not in INTERNAL and v2 is not None}
            spans.append(s)
    return spans


def percentile(values: list, q: float) -> float:
    """Calculate percentile from sorted values."""
    if not values:
        return 0.0
    sorted_v = sorted(values)
    idx = int(len(sorted_v) * q)
    idx = min(idx, len(sorted_v) - 1)
    return sorted_v[idx]


def print_system_analysis(spans: list[Span]):
    """Print system-level aggregate performance analysis."""
    if not spans:
        print("No spans found in the time window.")
        return

    # Group spans by name
    by_name: dict[str, list[Span]] = defaultdict(list)
    for s in spans:
        by_name[s.name].append(s)

    # Group by service
    by_service: dict[str, list[Span]] = defaultdict(list)
    for s in spans:
        by_service[s.service].append(s)

    print(f"\n{'='*70}")
    print(f" CraneSched System Performance ({len(spans)} spans)")
    print(f"{'='*70}")

    # --- Throughput ---
    job_ids = set()
    completed = set()
    failed = set()
    for s in spans:
        jid = s.attrs.get("job_id")
        if jid is not None:
            job_ids.add(jid)
        if s.name == "job/end":
            status = s.attrs.get("status")
            exit_code = s.attrs.get("exit_code")
            if jid:
                if str(status) == "2" or str(exit_code) == "0":
                    completed.add(jid)
                else:
                    failed.add(jid)

    if job_ids:
        print(f"\nThroughput:")
        print(f"  Jobs seen:       {len(job_ids)}")
        if completed or failed:
            total_done = len(completed) + len(failed)
            print(f"  Completed:       {len(completed)} ({100*len(completed)/total_done:.1f}%)" if total_done else "")
            print(f"  Failed:          {len(failed)} ({100*len(failed)/total_done:.1f}%)" if total_done else "")

    # --- Duration stats by span name ---
    # Key span categories for performance analysis
    PERF_SPANS = [
        ("Scheduling Cycle", "scheduling/cycle", [
            "scheduling/node_select", "scheduling/resource_validate",
            "scheduling/db_persist", "scheduling/rpc_alloc_jobs",
            "scheduling/rpc_alloc_steps",
        ]),
        ("Submit-to-Running", "job/pending", []),
        ("RPC Execute (CraneCtld→Craned)", "job/rpc_execute", []),
        ("Step Spawn", "step/supervisor_spawn", []),
        ("Step Execute", "step/execute", []),
        ("Queue Dispatch (Craned)", "step/queue_dispatch", []),
    ]

    for section_name, main_span, sub_spans in PERF_SPANS:
        spans_list = by_name.get(main_span, [])
        if not spans_list:
            continue
        durations = [s.duration_us for s in spans_list]
        avg = sum(durations) / len(durations)
        p50 = percentile(durations, 0.50)
        p95 = percentile(durations, 0.95)
        p99 = percentile(durations, 0.99)
        mx = max(durations)

        print(f"\n{section_name} ({main_span}):  n={len(durations)}")
        print(f"  avg={format_duration(int(avg))}  "
              f"P50={format_duration(int(p50))}  "
              f"P95={format_duration(int(p95))}  "
              f"P99={format_duration(int(p99))}  "
              f"max={format_duration(mx)}")

        for sub_name in sub_spans:
            sub_list = by_name.get(sub_name, [])
            if not sub_list:
                continue
            sub_durs = [s.duration_us for s in sub_list]
            sub_avg = sum(sub_durs) / len(sub_durs)
            sub_p99 = percentile(sub_durs, 0.99)
            # Mark potential bottleneck
            marker = "  <--" if sub_p99 > p99 * 0.5 else ""
            short = sub_name.split("/")[-1]
            print(f"  |-- {short:25s} avg={format_duration(int(sub_avg)):>10s}  "
                  f"P99={format_duration(int(sub_p99)):>10s}  (n={len(sub_durs)}){marker}")

    # --- Per-node RPC latency ---
    rpc_spans = by_name.get("job/rpc_execute", [])
    if rpc_spans:
        node_rpcs: dict[str, list[int]] = defaultdict(list)
        for s in rpc_spans:
            node = s.attrs.get("craned_id", "?")
            node_rpcs[node].append(s.duration_us)

        if len(node_rpcs) > 1:
            print(f"\nRPC Latency by Node (job/rpc_execute):")
            for node in sorted(node_rpcs.keys()):
                durs = node_rpcs[node]
                avg = sum(durs) / len(durs)
                p99 = percentile(durs, 0.99)
                print(f"  {node:12s} avg={format_duration(int(avg)):>10s}  "
                      f"P99={format_duration(int(p99)):>10s}  (n={len(durs)})")

    # --- Step type breakdown ---
    STEP_TYPE_NAMES = {1: "daemon", 2: "primary", 3: "common"}
    exec_spans = by_name.get("step/execute", [])
    if exec_spans:
        type_durs: dict[str, list[int]] = defaultdict(list)
        for s in exec_spans:
            st = s.attrs.get("step_type")
            tname = STEP_TYPE_NAMES.get(int(st), "unknown") if st is not None else "unknown"
            type_durs[tname].append(s.duration_us)

        if type_durs:
            print(f"\nStep Execute by Type:")
            for tname in ["daemon", "primary", "common", "unknown"]:
                durs = type_durs.get(tname)
                if not durs:
                    continue
                avg = sum(durs) / len(durs)
                p99 = percentile(durs, 0.99)
                print(f"  {tname:12s} avg={format_duration(int(avg)):>10s}  "
                      f"P99={format_duration(int(p99)):>10s}  (n={len(durs)})")

    print()


def export_system_chrome(spans: list[Span], output_path: str):
    """Export system-level Chrome Trace — grouped by span name, not step_id."""
    if not spans:
        print("No spans to export.")
        return

    # pid = service, tid = span name category (within service)
    services = sorted({s.service for s in spans})
    svc_to_pid = {svc: i + 1 for i, svc in enumerate(services)}

    # Assign tids by span name within each pid (greedy, no overlaps)
    span_tid: dict[str, int] = {}
    pid_name_tid: dict[tuple[int, str], int] = {}
    pid_lanes: dict[int, list[int]] = defaultdict(list)
    pid_next_tid: dict[int, int] = defaultdict(lambda: 1)

    # Phase 1: assign tids by span name
    for s in spans:
        pid = svc_to_pid.get(s.service, 0)
        key = (pid, s.name)
        if key not in pid_name_tid:
            pid_name_tid[key] = pid_next_tid[pid]
            pid_next_tid[pid] += 1

    # Phase 2: greedy within each name-lane to avoid overlaps
    # For each (pid, name) lane, track end times of sub-lanes
    lane_sub_ends: dict[tuple[int, str], list[int]] = defaultdict(list)

    for s in sorted(spans, key=lambda x: int(x.start_time.timestamp() * 1_000_000) if x.start_time else 0):
        pid = svc_to_pid.get(s.service, 0)
        ts = int(s.start_time.timestamp() * 1_000_000) if s.start_time else 0
        end = ts + max(s.duration_us, 1)
        key = (pid, s.name)
        base_tid = pid_name_tid[key]

        sub_ends = lane_sub_ends[key]
        assigned = False
        for i, se in enumerate(sub_ends):
            if ts >= se:
                sub_ends[i] = end
                span_tid[s.span_id] = base_tid + i
                assigned = True
                break
        if not assigned:
            sub_ends.append(end)
            if len(sub_ends) > 1:
                # Need extra tid slots — shift subsequent name lanes
                span_tid[s.span_id] = base_tid + len(sub_ends) - 1
            else:
                span_tid[s.span_id] = base_tid

    events = []

    # Process names
    for svc, pid in svc_to_pid.items():
        events.append({
            "name": "process_name", "ph": "M", "pid": pid, "tid": 0,
            "args": {"name": svc if svc != "-" else "unknown"}
        })

    # Thread names
    for (pid, name), tid in pid_name_tid.items():
        events.append({
            "name": "thread_name", "ph": "M", "pid": pid, "tid": tid,
            "args": {"name": name}
        })

    # Span events
    for s in spans:
        ts_us = int(s.start_time.timestamp() * 1_000_000) if s.start_time else 0
        pid = svc_to_pid.get(s.service, 0)
        tid = span_tid.get(s.span_id, 0)

        args = dict(s.attrs)
        args["trace_id"] = s.trace_id
        args["span_id"] = s.span_id

        events.append({
            "name": s.name,
            "ph": "X",
            "ts": ts_us,
            "dur": max(s.duration_us, 1),
            "pid": pid,
            "tid": tid,
            "cat": s.name.split("/")[0],  # job, step, scheduling, submit
            "args": args,
        })

    with open(output_path, "w") as f:
        json.dump(events, f, indent=2, default=str)

    print(f"Exported {len(spans)} spans (system view) to {output_path}")
    print(f"Open in browser: chrome://tracing or https://ui.perfetto.dev")


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    args = setup_args()

    print(f"Connecting to {args.url}, Org: {args.org}, Bucket: {args.bucket}")

    try:
        with InfluxDBClient(url=args.url, token=args.token, org=args.org) as client:
            if args.system:
                spans = query_all_spans(client, args)
                if args.perfetto:
                    export_perfetto(spans, args.perfetto, system_mode=True)
                elif args.chrome:
                    export_system_chrome(spans, args.chrome)
                else:
                    print_system_analysis(spans)
            else:
                spans = query_spans(client, args)
                if args.perfetto:
                    export_perfetto(spans, args.perfetto, system_mode=False)
                elif args.chrome:
                    export_chrome(spans, args.chrome)
                elif args.tree:
                    print_tree(spans, args.verbose)
                else:
                    print_flat(spans, args.verbose)

    except Exception as e:
        print(f"\nError: {e}")
        print("Please check your InfluxDB connection settings and ensure the service is running.")
        sys.exit(1)


if __name__ == "__main__":
    main()
