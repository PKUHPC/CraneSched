#!/usr/bin/env python3
"""统计 job_id 区间的任务耗时分布信息。"""

import argparse
import math
import sys

import yaml
from pymongo import MongoClient

DEFAULT_DB_CONFIG = "/etc/crane/database.yaml"


def load_db_config(config_path):
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)
    return {
        "host": cfg.get("DbHost", "localhost"),
        "port": cfg.get("DbPort", 27017),
        "user": cfg.get("DbUser", ""),
        "password": cfg.get("DbPassword", ""),
        "db": cfg.get("DbName", "crane_db"),
        "replica_set": cfg.get("DbReplSetName", ""),
    }


def parse_args():
    p = argparse.ArgumentParser(description="Job duration distribution stats")
    p.add_argument("--config", default=DEFAULT_DB_CONFIG,
                   help="Path to database.yaml (default: /etc/crane/database.yaml)")
    p.add_argument("--collection", default="job_table")
    p.add_argument("--uri", default=None, help="MongoDB URI (overrides config file)")
    return p.parse_args()


BUCKETS = [
    (0, 1, "<1s"),
    (1, 5, "1-5s"),
    (5, 10, "5-10s"),
    (10, 30, "10-30s"),
    (30, 60, "30-60s"),
    (60, 300, "1-5min"),
    (300, 600, "5-10min"),
    (600, float("inf"), ">10min"),
]

RANGES = [
    (1, 10000, "1-10000"),
    (10001, 20000, "10001-20000"),
]

JOB_STATUS = {
    0: "Pending",
    1: "Running",
    2: "Completed",
    3: "Failed",
    4: "ExceedTimeLimit",
    5: "Cancelled",
    6: "OutOfMemory",
    7: "Configuring",
    8: "Starting",
    9: "Completing",
    10: "Suspended",
    11: "Deadline",
    15: "Invalid",
}


def percentile(sorted_data, p):
    if not sorted_data:
        return 0
    k = (len(sorted_data) - 1) * p / 100.0
    f = int(k)
    c = f + 1
    if c >= len(sorted_data):
        return sorted_data[-1]
    return sorted_data[f] + (sorted_data[c] - sorted_data[f]) * (k - f)


def compute_stats(durations):
    if not durations:
        return None
    durations.sort()
    n = len(durations)
    avg = sum(durations) / n
    variance = sum((d - avg) ** 2 for d in durations) / n
    stddev = math.sqrt(variance)
    return {
        "count": n,
        "min": durations[0],
        "max": durations[-1],
        "avg": avg,
        "stddev": stddev,
        "p50": percentile(durations, 50),
        "p90": percentile(durations, 90),
        "p95": percentile(durations, 95),
        "p99": percentile(durations, 99),
    }


def compute_histogram(durations):
    hist = {label: 0 for _, _, label in BUCKETS}
    for d in durations:
        for lo, hi, label in BUCKETS:
            if lo <= d < hi:
                hist[label] += 1
                break
    return hist


def fmt_duration(seconds):
    if seconds < 60:
        return f"{seconds:.3f}s"
    elif seconds < 3600:
        return f"{seconds/60:.2f}min"
    else:
        return f"{seconds/3600:.2f}h"


def print_stats(label, stats, hist, total, status_dist):
    print(f"\n{'='*60}")
    print(f"  Job ID Range: {label}")
    print(f"{'='*60}")
    if stats is None:
        print("  No data found.")
        return
    print(f"  Count:  {stats['count']}")
    print(f"  Min:    {fmt_duration(stats['min'])}")
    print(f"  Max:    {fmt_duration(stats['max'])}")
    print(f"  Avg:    {fmt_duration(stats['avg'])}")
    print(f"  Stddev: {fmt_duration(stats['stddev'])}")
    print(f"  P50:    {fmt_duration(stats['p50'])}")
    print(f"  P90:    {fmt_duration(stats['p90'])}")
    print(f"  P95:    {fmt_duration(stats['p95'])}")
    print(f"  P99:    {fmt_duration(stats['p99'])}")

    print(f"\n  Status Distribution:")
    print(f"  {'Status':<16} {'Count':>8} {'Percent':>8}  Bar")
    print(f"  {'-'*16} {'-'*8} {'-'*8}  {'-'*30}")
    status_total = sum(status_dist.values())
    for code in sorted(status_dist.keys()):
        name = JOB_STATUS.get(code, f"Unknown({code})")
        count = status_dist[code]
        pct = count / status_total * 100 if status_total > 0 else 0
        bar_len = int(pct / 2)
        bar = "#" * bar_len
        print(f"  {name:<16} {count:>8} {pct:>7.2f}%  {bar}")

    print(f"\n  Duration Distribution:")
    print(f"  {'Bucket':<10} {'Count':>8} {'Percent':>8}  Bar")
    print(f"  {'-'*10} {'-'*8} {'-'*8}  {'-'*30}")
    for _, _, label in BUCKETS:
        count = hist[label]
        pct = count / total * 100 if total > 0 else 0
        bar_len = int(pct / 2)
        bar = "#" * bar_len
        print(f"  {label:<10} {count:>8} {pct:>7.2f}%  {bar}")


def main():
    args = parse_args()
    if args.uri:
        client = MongoClient(args.uri)
        db_name = args.uri.rsplit("/", 1)[-1].split("?")[0] or "crane_db"
    else:
        cfg = load_db_config(args.config)
        if cfg["user"] and cfg["password"]:
            uri = (f"mongodb://{cfg['user']}:{cfg['password']}"
                   f"@{cfg['host']}:{cfg['port']}/{cfg['db']}"
                   f"?authSource=admin")
        else:
            uri = f"mongodb://{cfg['host']}:{cfg['port']}/{cfg['db']}"
        if cfg["replica_set"]:
            sep = "&" if "?" in uri else "?"
            uri += f"{sep}replicaSet={cfg['replica_set']}"
        client = MongoClient(uri)
        db_name = cfg["db"]

    db = client[db_name]
    coll = db[args.collection]

    for lo, hi, label in RANGES:
        cursor = coll.find(
            {"job_id": {"$gte": lo, "$lte": hi}},
            {"time_start": 1, "time_end": 1, "state": 1},
        )
        durations = []
        status_dist = {}
        for doc in cursor:
            state = doc.get("state")
            if state is not None:
                status_dist[state] = status_dist.get(state, 0) + 1
            ts = doc.get("time_start", 0) or 0
            te = doc.get("time_end", 0) or 0
            if ts > 0 and te > 0:
                d = te - ts
                if d >= 0:
                    durations.append(d)

        stats = compute_stats(durations)
        hist = compute_histogram(durations)
        print_stats(label, stats, hist, len(durations), status_dist)

    print()


if __name__ == "__main__":
    main()
