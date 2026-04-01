#!/usr/bin/env python3
"""Query and visualize CraneSched distributed traces from InfluxDB.

Supports flat table output and tree-based flame graph visualization.
"""

import os
import argparse
import sys
import re
from collections import defaultdict
from dataclasses import dataclass, field
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


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    args = setup_args()

    print(f"Connecting to {args.url}, Org: {args.org}, Bucket: {args.bucket}")

    try:
        with InfluxDBClient(url=args.url, token=args.token, org=args.org) as client:
            spans = query_spans(client, args)

            if args.tree:
                print_tree(spans, args.verbose)
            else:
                print_flat(spans, args.verbose)

    except Exception as e:
        print(f"\nError: {e}")
        print("Please check your InfluxDB connection settings and ensure the service is running.")
        sys.exit(1)


if __name__ == "__main__":
    main()
