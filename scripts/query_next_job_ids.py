#!/usr/bin/env python3

"""Print safe next job IDs from a migrated CraneSched MongoDB database."""

import argparse
import json
import shutil
import subprocess
import sys


MAX_JOB_ID = 2**32 - 1
MAX_JOB_DB_ID = 2**63 - 1

MONGO_QUERY = r"""
(() => {
  if (!db.getCollectionNames().includes("job_table")) {
    throw new Error("job_table does not exist; migration may not be complete");
  }

  const rows = db.job_table.aggregate([{
    $group: {
      _id: null,
      max_job_id: {$max: "$job_id"},
      max_job_db_id: {$max: "$job_db_id"}
    }
  }]).toArray();
  const row = rows.length === 0 ? {} : rows[0];
  const asString = value => value == null ? null : value.toString();

  print(JSON.stringify({
    max_job_id: asString(row.max_job_id),
    max_job_db_id: asString(row.max_job_db_id)
  }));
})()
"""


def _port(value):
    port = int(value)
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("port must be between 1 and 65535")
    return port


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(
        description=(
            "Read max job IDs from a migrated job_table and print the "
            "ccontrol reset commands. This tool does not modify any data."
        )
    )
    parser.add_argument("--database", required=True, help="MongoDB database name")
    parser.add_argument("--host", default="localhost", help="MongoDB host")
    parser.add_argument("--port", type=_port, default=27017, help="MongoDB port")
    parser.add_argument("--username", help="MongoDB username")
    parser.add_argument(
        "--authentication-database",
        default="admin",
        help="MongoDB authentication database (default: admin)",
    )
    parser.add_argument("--tls", action="store_true", help="Enable MongoDB TLS")
    parser.add_argument("--tls-ca-file", help="MongoDB TLS CA file")
    parser.add_argument("--mongosh", default="mongosh", help=argparse.SUPPRESS)
    parser.add_argument(
        "--json", action="store_true", help="Print machine-readable JSON"
    )
    return parser.parse_args(argv)


def _build_mongosh_command(args, executable):
    command = [
        executable,
        "--quiet",
        "--norc",
        "--host",
        args.host,
        "--port",
        str(args.port),
    ]
    if args.username:
        command.extend(
            [
                "--username",
                args.username,
                "--authenticationDatabase",
                args.authentication_database,
            ]
        )
    if args.tls:
        command.append("--tls")
    if args.tls_ca_file:
        command.extend(["--tlsCAFile", args.tls_ca_file])
    command.extend([args.database, "--eval", MONGO_QUERY])
    return command


def _parse_id(value, field):
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field} is not an integer")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    raise ValueError(f"{field} is not a non-negative integer: {value!r}")


def _parse_mongosh_output(output):
    for line in reversed(output.splitlines()):
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(value, dict):
            continue
        if "max_job_id" not in value or "max_job_db_id" not in value:
            continue
        return (
            _parse_id(value["max_job_id"], "max_job_id"),
            _parse_id(value["max_job_db_id"], "max_job_db_id"),
        )
    raise ValueError("mongosh did not return the expected job ID result")


def _next_id(maximum, limit, field):
    if maximum is None:
        return 1
    if maximum < 0:
        raise ValueError(f"{field} must not be negative")
    if maximum >= limit:
        raise ValueError(f"{field} cannot be incremented safely: {maximum}")
    return maximum + 1


def _query_ids(args):
    executable = shutil.which(args.mongosh)
    if executable is None:
        raise RuntimeError(f"mongosh executable not found: {args.mongosh}")

    result = subprocess.run(
        _build_mongosh_command(args, executable),
        check=False,
        stdout=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"mongosh exited with status {result.returncode}")
    return _parse_mongosh_output(result.stdout)


def main(argv=None):
    args = parse_arguments(argv)
    try:
        max_job_id, max_job_db_id = _query_ids(args)
        next_job_id = _next_id(max_job_id, MAX_JOB_ID, "max_job_id")
        next_job_db_id = _next_id(max_job_db_id, MAX_JOB_DB_ID, "max_job_db_id")
    except (OSError, RuntimeError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    values = {
        "database": args.database,
        "max_job_id": max_job_id,
        "max_job_db_id": max_job_db_id,
        "next_job_id": next_job_id,
        "next_job_db_id": next_job_db_id,
    }
    if args.json:
        print(json.dumps(values, separators=(",", ":")))
        return 0

    print(f"Database: {args.database}")
    print(f"max_job_id: {max_job_id if max_job_id is not None else '<none>'}")
    print(f"max_job_db_id: {max_job_db_id if max_job_db_id is not None else '<none>'}")
    print(f"next_job_id: {next_job_id}")
    print(f"next_job_db_id: {next_job_db_id}")
    print("\nRun these commands before reopening submissions:")
    print(f"ccontrol reset next-job-id {next_job_id}")
    print(f"ccontrol reset next-job-db-id {next_job_db_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
