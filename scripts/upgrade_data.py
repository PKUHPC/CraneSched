#!/usr/bin/env python3

"""Perform the data operations required for an upgrade from CraneSched v1.1.3."""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover - exercised by deployment environments
    yaml = None


MAX_JOB_ID = 2**32 - 1
MAX_JOB_DB_ID = 2**63 - 1
CACCT_MAX_ATTEMPTS = 10
QUERY_MARKER = "__CRANE_MAX_JOB_IDS__"
MONGO_QUERY = rf"""
(() => {{
  const names = db.getCollectionNames();
  const hasJobTable = names.includes("job_table");
  const hasTaskTable = names.includes("task_table");
  if (hasJobTable === hasTaskTable) {{
    throw new Error(
      hasJobTable
        ? "both job_table and task_table exist; database state is ambiguous"
        : "neither job_table nor task_table exists"
    );
  }}

  const collection = hasJobTable ? "job_table" : "task_table";
  const jobIdField = hasJobTable ? "job_id" : "task_id";
  const jobDbIdField = hasJobTable ? "job_db_id" : "task_db_id";
  const rows = db.getCollection(collection).aggregate([{{
    $group: {{
      _id: null,
      max_job_id: {{$max: `$${{jobIdField}}`}},
      max_job_db_id: {{$max: `$${{jobDbIdField}}`}}
    }}
  }}]).toArray();
  const row = rows.length === 0 ? {{}} : rows[0];
  const asString = value => value == null ? null : value.toString();

  print("{QUERY_MARKER}" + JSON.stringify({{
    collection: collection,
    job_id_field: jobIdField,
    job_db_id_field: jobDbIdField,
    max_job_id: asString(row.max_job_id),
    max_job_db_id: asString(row.max_job_db_id)
  }}));
}})()
"""


class BackupError(RuntimeError):
    """An expected backup or validation failure."""


def _status(message):
    print(f"[INFO] {message}", flush=True)


@dataclass(frozen=True)
class CraneSettings:
    config_path: str
    database_config_path: str
    config_dir: str
    database_path: str
    runtime_dir: str
    mongo_host: str
    mongo_port: int
    mongo_username: str
    mongo_database: str


def _load_yaml(path):
    if yaml is None:
        raise BackupError(
            "PyYAML is required; install python3-yaml (Debian/Ubuntu) or "
            "python3-pyyaml (RHEL-compatible distributions)"
        )

    try:
        with Path(path).open(encoding="utf-8") as config_file:
            value = yaml.safe_load(config_file)
    except (OSError, yaml.YAMLError) as error:
        raise BackupError(f"failed to read YAML config {path}: {error}") from error

    if not isinstance(value, dict):
        raise BackupError(f"YAML config is not a mapping: {path}")
    return value


def _required_string(config, key, default=None):
    value = config.get(key, default)
    if value is None or not str(value).strip():
        raise BackupError(f"missing or empty configuration key: {key}")
    return str(value).strip()


def _resolve_settings(config_path, database_config_path=None, config_dir=None):
    config_path = Path(config_path).resolve()
    config = _load_yaml(config_path)

    if database_config_path is None:
        database_config_path = config.get("DbConfigPath", "/etc/crane/database.yaml")
    database_config_path = Path(str(database_config_path))
    if not database_config_path.is_absolute():
        database_config_path = config_path.parent / database_config_path
    database_config_path = database_config_path.resolve()
    database_config = _load_yaml(database_config_path)

    crane_base_dir = Path(_required_string(config, "CraneBaseDir", "/var/crane/"))
    keepalived = config.get("Keepalived")
    if isinstance(keepalived, dict) and keepalived.get("CraneSharedBaseDir"):
        crane_base_dir = Path(str(keepalived["CraneSharedBaseDir"]))
    crane_base_dir = crane_base_dir.resolve()

    database_path = Path(
        _required_string(database_config, "CraneCtldDbPath", "cranectld/embedded.db")
    )
    if not database_path.is_absolute():
        database_path = crane_base_dir / database_path
    database_path = database_path.resolve()
    runtime_dir = database_path.parent
    if runtime_dir == Path("/") or runtime_dir == crane_base_dir:
        raise BackupError(
            "CraneCtldDbPath must be inside a dedicated subdirectory; refusing "
            f"to back up broad runtime directory {runtime_dir}"
        )

    try:
        mongo_port = int(database_config.get("DbPort", 27017))
    except (TypeError, ValueError) as error:
        raise BackupError("DbPort must be an integer") from error
    if not 1 <= mongo_port <= 65535:
        raise BackupError("DbPort must be between 1 and 65535")

    if config_dir is None:
        config_dir = "/etc/crane"

    return CraneSettings(
        config_path=str(config_path),
        database_config_path=str(database_config_path),
        config_dir=str(Path(config_dir).resolve()),
        database_path=str(database_path),
        runtime_dir=str(runtime_dir),
        mongo_host=_required_string(database_config, "DbHost", "localhost"),
        mongo_port=mongo_port,
        mongo_username=str(database_config.get("DbUser") or "").strip(),
        mongo_database=_required_string(database_config, "DbName", "crane_db"),
    )


def _find_executable(name, override=None):
    executable = override or shutil.which(name)
    if executable is None:
        raise BackupError(f"required executable not found in PATH: {name}")
    return executable


def _mongo_connection_args(settings, authentication_database, tls, tls_ca_file):
    args = [
        "--host",
        settings.mongo_host,
        "--port",
        str(settings.mongo_port),
    ]
    if settings.mongo_username:
        args.extend(
            [
                "--username",
                settings.mongo_username,
                "--authenticationDatabase",
                authentication_database,
            ]
        )
    if tls:
        args.append("--tls")
    if tls_ca_file:
        args.extend(["--tlsCAFile", tls_ca_file])
    return args


def _build_mongosh_command(
    executable, settings, authentication_database="admin", tls=False, tls_ca_file=None
):
    return [
        executable,
        "--quiet",
        "--norc",
        *_mongo_connection_args(settings, authentication_database, tls, tls_ca_file),
        settings.mongo_database,
        "--eval",
        MONGO_QUERY,
    ]


def _build_mongodump_command(
    executable,
    settings,
    output_dir,
    authentication_database="admin",
    tls=False,
    tls_ca_file=None,
):
    return [
        executable,
        *_mongo_connection_args(settings, authentication_database, tls, tls_ca_file),
        "--db",
        settings.mongo_database,
        "--out",
        str(output_dir),
    ]


def _build_mongorestore_command(
    executable,
    settings,
    dump_dir,
    authentication_database="admin",
    tls=False,
    tls_ca_file=None,
):
    return [
        executable,
        *_mongo_connection_args(settings, authentication_database, tls, tls_ca_file),
        "--db",
        settings.mongo_database,
        str(dump_dir),
    ]


def _parse_non_negative_id(value, field):
    if value is None:
        return None
    if isinstance(value, bool):
        raise BackupError(f"{field} is not an integer")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and value.isdigit():
        parsed = int(value)
    else:
        raise BackupError(f"{field} is not a non-negative integer: {value!r}")
    if parsed < 0:
        raise BackupError(f"{field} must not be negative")
    return parsed


def _next_id(maximum, limit, field):
    if maximum is None:
        return 1
    if maximum >= limit:
        raise BackupError(f"{field} cannot be incremented safely: {maximum}")
    return maximum + 1


def _parse_query_output(output):
    for line in reversed(output.splitlines()):
        if not line.startswith(QUERY_MARKER):
            continue
        try:
            result = json.loads(line[len(QUERY_MARKER) :])
        except json.JSONDecodeError as error:
            raise BackupError(f"invalid mongosh result: {error}") from error
        required = {
            "collection",
            "job_id_field",
            "job_db_id_field",
            "max_job_id",
            "max_job_db_id",
        }
        if not isinstance(result, dict) or not required.issubset(result):
            break
        result["max_job_id"] = _parse_non_negative_id(
            result["max_job_id"], "max_job_id"
        )
        result["max_job_db_id"] = _parse_non_negative_id(
            result["max_job_db_id"], "max_job_db_id"
        )
        result["next_job_id"] = _next_id(result["max_job_id"], MAX_JOB_ID, "max_job_id")
        result["next_job_db_id"] = _next_id(
            result["max_job_db_id"], MAX_JOB_DB_ID, "max_job_db_id"
        )
        return result
    raise BackupError("mongosh did not return the expected job ID result")


def _query_job_ids(command):
    _status("Querying the maximum job ID and job database ID from MongoDB")
    try:
        result = subprocess.run(
            command,
            check=False,
            stdout=subprocess.PIPE,
            text=True,
        )
    except OSError as error:
        raise BackupError(f"failed to run mongosh: {error}") from error
    if result.returncode != 0:
        raise BackupError(f"mongosh exited with status {result.returncode}")
    ids = _parse_query_output(result.stdout)
    _status(
        "MongoDB IDs read: "
        f"max_job_id={ids['max_job_id']}, "
        f"max_job_db_id={ids['max_job_db_id']}, "
        f"next_job_id={ids['next_job_id']}, "
        f"next_job_db_id={ids['next_job_db_id']}"
    )
    return ids


def _run_checked(command, operation):
    _status(f"Starting: {operation}")
    try:
        result = subprocess.run(command, check=False)
    except OSError as error:
        raise BackupError(f"failed to {operation}: {error}") from error
    if result.returncode != 0:
        raise BackupError(f"failed to {operation}: exit status {result.returncode}")
    _status(f"Completed: {operation}")


def _run_capture_checked(command, operation):
    _status(f"Starting: {operation}")
    try:
        result = subprocess.run(
            command,
            check=False,
            stdout=subprocess.PIPE,
            text=True,
        )
    except OSError as error:
        raise BackupError(f"failed to {operation}: {error}") from error
    if result.returncode != 0:
        raise BackupError(f"failed to {operation}: exit status {result.returncode}")
    _status(f"Completed: {operation}")
    return result.stdout


def _check_cranectld_stopped(pgrep):
    try:
        result = subprocess.run(
            [pgrep, "-x", "cranectld"],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except OSError as error:
        raise BackupError(f"failed to check cranectld process: {error}") from error
    if result.returncode == 0:
        pids = " ".join(result.stdout.split())
        raise BackupError(f"cranectld is still running (PID: {pids})")
    if result.returncode != 1:
        raise BackupError(
            f"pgrep failed with status {result.returncode}: {result.stderr.strip()}"
        )
    _status("Confirmed that cranectld is stopped")


def _is_relative_to(path, parent):
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _validate_source_paths(settings, output_dir):
    config_dir = Path(settings.config_dir)
    runtime_dir = Path(settings.runtime_dir)
    for label, path in (
        ("configuration directory", config_dir),
        ("CraneCtld runtime directory", runtime_dir),
    ):
        if not path.is_dir():
            raise BackupError(f"{label} does not exist: {path}")

    output_dir = output_dir.resolve()
    for source in (config_dir.resolve(), runtime_dir.resolve()):
        if _is_relative_to(output_dir, source):
            raise BackupError(
                f"backup output must be outside source directory: {source}"
            )


def _copy_archive(cp, source, destination, operation):
    _run_checked([cp, "-a", "--", str(source), str(destination)], operation)


def _write_manifest(output_dir, settings, ids):
    manifest = {
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "settings": asdict(settings),
        "job_ids": ids,
    }
    temporary = output_dir / "manifest.json.tmp"
    final = output_dir / "manifest.json"
    temporary.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.chmod(temporary, 0o600)
    temporary.replace(final)
    return manifest


def _perform_backup(args):
    if os.geteuid() != 0:
        raise BackupError("run this command as root so file ownership is preserved")

    settings = _resolve_settings(args.config, args.database_config, args.config_dir)
    _status(f"Loaded Crane configuration: {settings.config_path}")
    _status(f"Loaded database configuration: {settings.database_config_path}")
    _status(
        f"Resolved MongoDB database: {settings.mongo_database} at "
        f"{settings.mongo_host}:{settings.mongo_port}"
    )
    _status(f"Resolved CraneCtld runtime directory: {settings.runtime_dir}")
    output_dir = Path(args.output).resolve()
    if output_dir.exists():
        raise BackupError(f"backup output already exists: {output_dir}")
    _validate_source_paths(settings, output_dir)

    mongosh = _find_executable("mongosh", args.mongosh)
    mongodump = _find_executable("mongodump", args.mongodump)
    cp = _find_executable("cp", args.cp)
    pgrep = _find_executable("pgrep", args.pgrep)
    _check_cranectld_stopped(pgrep)

    query_command = _build_mongosh_command(
        mongosh,
        settings,
        args.authentication_database,
        args.tls,
        args.tls_ca_file,
    )
    ids = _query_job_ids(query_command)

    output_dir.mkdir(mode=0o700, parents=True)
    incomplete_marker = output_dir / "BACKUP_INCOMPLETE"
    incomplete_marker.write_text(
        "This directory is incomplete and must not be used for rollback.\n",
        encoding="utf-8",
    )
    os.chmod(incomplete_marker, 0o600)

    _copy_archive(
        cp,
        settings.config_dir,
        output_dir / "etc-crane",
        "back up the Crane configuration directory",
    )
    _copy_archive(
        cp,
        settings.database_config_path,
        output_dir / "database.yaml.active",
        "back up the active database configuration",
    )

    dump_dir = output_dir / "mongodb"
    dump_command = _build_mongodump_command(
        mongodump,
        settings,
        dump_dir,
        args.authentication_database,
        args.tls,
        args.tls_ca_file,
    )
    _run_checked(dump_command, f"dump MongoDB database {settings.mongo_database}")
    expected_dump = dump_dir / settings.mongo_database
    if not expected_dump.is_dir():
        raise BackupError(f"mongodump output is missing: {expected_dump}")

    _copy_archive(
        cp,
        settings.runtime_dir,
        output_dir / "cranectld-runtime",
        "back up the CraneCtld runtime directory",
    )

    manifest = _write_manifest(output_dir, settings, ids)
    _status(f"Wrote backup manifest: {output_dir / 'manifest.json'}")
    incomplete_marker.unlink()
    _status("Removed BACKUP_INCOMPLETE after all backup operations completed")
    return output_dir, manifest


def _load_manifest(backup_dir):
    backup_dir = Path(backup_dir).resolve()
    if (backup_dir / "BACKUP_INCOMPLETE").exists():
        raise BackupError(f"backup is incomplete: {backup_dir}")

    manifest_path = backup_dir / "manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise BackupError(
            f"failed to read backup manifest {manifest_path}: {error}"
        ) from error
    if not isinstance(manifest, dict):
        raise BackupError(f"backup manifest is not a JSON object: {manifest_path}")
    return manifest


def _settings_from_manifest(manifest):
    value = manifest.get("settings")
    if not isinstance(value, dict):
        raise BackupError("backup manifest has no settings object")

    try:
        mongo_port = int(value.get("mongo_port"))
    except (TypeError, ValueError) as error:
        raise BackupError("backup manifest contains an invalid MongoDB port") from error
    if not 1 <= mongo_port <= 65535:
        raise BackupError("backup manifest contains an invalid MongoDB port")

    settings = CraneSettings(
        config_path=_required_string(value, "config_path"),
        database_config_path=_required_string(value, "database_config_path"),
        config_dir=_required_string(value, "config_dir"),
        database_path=_required_string(value, "database_path"),
        runtime_dir=_required_string(value, "runtime_dir"),
        mongo_host=_required_string(value, "mongo_host"),
        mongo_port=mongo_port,
        mongo_username=str(value.get("mongo_username") or "").strip(),
        mongo_database=_required_string(value, "mongo_database"),
    )

    for field in (
        "config_path",
        "database_config_path",
        "config_dir",
        "database_path",
        "runtime_dir",
    ):
        if not Path(getattr(settings, field)).is_absolute():
            raise BackupError(f"backup manifest contains a non-absolute {field}")

    config_path = Path(settings.config_path).resolve()
    config_dir = Path(settings.config_dir).resolve()
    runtime_dir = Path(settings.runtime_dir).resolve()
    database_path = Path(settings.database_path).resolve()
    for label, path in (
        ("configuration", config_dir),
        ("CraneCtld runtime", runtime_dir),
    ):
        if len(path.parts) < 3:
            raise BackupError(f"backup manifest contains an unsafe {label} directory")
    if not _is_relative_to(config_path, config_dir):
        raise BackupError("Crane config path is outside the configuration directory")
    if database_path.parent != runtime_dir:
        raise BackupError("backup manifest contains an unsafe CraneCtld runtime path")
    if _is_relative_to(runtime_dir, config_dir) or _is_relative_to(
        config_dir, runtime_dir
    ):
        raise BackupError("configuration and runtime directories must not overlap")
    return settings


def _require_backup_path(backup_dir, relative_path, path_type):
    path = backup_dir / relative_path
    predicate = path.is_dir if path_type == "directory" else path.is_file
    if not predicate():
        raise BackupError(f"backup is missing {path_type}: {path}")
    return path


def _validated_next_ids(manifest):
    ids = manifest.get("job_ids")
    if not isinstance(ids, dict):
        raise BackupError("backup manifest has no job_ids object")

    max_job_id = _parse_non_negative_id(ids.get("max_job_id"), "max_job_id")
    max_job_db_id = _parse_non_negative_id(ids.get("max_job_db_id"), "max_job_db_id")
    next_job_id = _parse_non_negative_id(ids.get("next_job_id"), "next_job_id")
    next_job_db_id = _parse_non_negative_id(ids.get("next_job_db_id"), "next_job_db_id")
    expected_job_id = _next_id(max_job_id, MAX_JOB_ID, "max_job_id")
    expected_job_db_id = _next_id(max_job_db_id, MAX_JOB_DB_ID, "max_job_db_id")
    if next_job_id != expected_job_id:
        raise BackupError(
            f"next_job_id {next_job_id} does not follow max_job_id {max_job_id}"
        )
    if next_job_db_id != expected_job_db_id:
        raise BackupError(
            "next_job_db_id "
            f"{next_job_db_id} does not follow max_job_db_id {max_job_db_id}"
        )
    return next_job_id, next_job_db_id


def _validate_backup(backup_dir):
    backup_dir = Path(backup_dir).resolve()
    manifest = _load_manifest(backup_dir)
    settings = _settings_from_manifest(manifest)
    next_job_id, next_job_db_id = _validated_next_ids(manifest)
    _require_backup_path(backup_dir, "etc-crane", "directory")
    _require_backup_path(backup_dir, "database.yaml.active", "file")
    _require_backup_path(backup_dir / "mongodb", settings.mongo_database, "directory")
    _require_backup_path(backup_dir, "cranectld-runtime", "directory")
    _status(f"Validated all required backup data in {backup_dir}")
    return {
        "backup_dir": str(backup_dir),
        "mongo_database": settings.mongo_database,
        "runtime_dir": settings.runtime_dir,
        "next_job_id": next_job_id,
        "next_job_db_id": next_job_db_id,
    }


def _clear_runtime(backup_dir, pgrep_override=None):
    if os.geteuid() != 0:
        raise BackupError("run this command as root to clear the runtime directory")

    backup_dir = Path(backup_dir).resolve()
    manifest = _load_manifest(backup_dir)
    settings = _settings_from_manifest(manifest)
    _require_backup_path(backup_dir, "cranectld-runtime", "directory")

    pgrep = _find_executable("pgrep", pgrep_override)
    _check_cranectld_stopped(pgrep)
    runtime_dir = Path(settings.runtime_dir).resolve()
    if not runtime_dir.is_dir():
        raise BackupError(f"CraneCtld runtime directory does not exist: {runtime_dir}")
    if _is_relative_to(backup_dir, runtime_dir):
        raise BackupError("backup directory must be outside the runtime directory")

    entries = list(runtime_dir.iterdir())
    _status(
        f"Clearing {len(entries)} entries from CraneCtld runtime directory: "
        f"{runtime_dir}"
    )
    try:
        for child in entries:
            if child.is_symlink() or not child.is_dir():
                child.unlink()
            else:
                shutil.rmtree(child)
    except OSError as error:
        raise BackupError(
            f"failed to clear runtime directory {runtime_dir}: {error}"
        ) from error
    if any(runtime_dir.iterdir()):
        raise BackupError(
            f"runtime directory is not empty after cleanup: {runtime_dir}"
        )
    _status(f"Confirmed empty CraneCtld runtime directory: {runtime_dir}")
    return runtime_dir


def _restore_job_ids(backup_dir, ccontrol_override=None):
    manifest = _load_manifest(backup_dir)
    next_job_id, next_job_db_id = _validated_next_ids(manifest)
    ccontrol = _find_executable("ccontrol", ccontrol_override)
    _status(
        f"Validated ID counters: next_job_id={next_job_id}, "
        f"next_job_db_id={next_job_db_id}"
    )

    _run_checked(
        [ccontrol, "reset", "next-job-id", str(next_job_id)],
        "restore next job ID",
    )
    _run_checked(
        [ccontrol, "reset", "next-job-db-id", str(next_job_db_id)],
        "restore next job database ID",
    )
    return next_job_id, next_job_db_id


def _verify_job_id(backup_dir, job_id):
    manifest = _load_manifest(backup_dir)
    expected_job_id, _ = _validated_next_ids(manifest)
    actual_job_id = _parse_non_negative_id(job_id, "job_id")
    if actual_job_id != expected_job_id:
        raise BackupError(
            f"canary job ID {actual_job_id} does not match expected ID "
            f"{expected_job_id}"
        )
    return actual_job_id


def _parse_cbatch_job_id(output):
    for line in reversed(output.splitlines()):
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and "job_id" in value:
            job_id = _parse_non_negative_id(value["job_id"], "job_id")
            if job_id == 0:
                raise BackupError("cbatch returned invalid job ID 0")
            return job_id
    raise BackupError("cbatch did not return a job_id in its JSON response")


def _account_output_contains_job_id(value, job_id):
    if isinstance(value, dict):
        if str(value.get("job_id")) == str(job_id):
            return True
        return any(
            _account_output_contains_job_id(item, job_id) for item in value.values()
        )
    if isinstance(value, list):
        return any(_account_output_contains_job_id(item, job_id) for item in value)
    return False


def _validate_account_output(output, job_id):
    try:
        value = json.loads(output)
    except json.JSONDecodeError as error:
        raise BackupError(f"cacct returned invalid JSON: {error}") from error
    if not _account_output_contains_job_id(value, job_id):
        raise BackupError(
            f"cacct output does not contain cancelled canary job {job_id}"
        )


def _run_canary(
    backup_dir,
    cbatch_override=None,
    ccancel_override=None,
    cacct_override=None,
):
    cbatch = _find_executable("cbatch", cbatch_override)
    ccancel = _find_executable("ccancel", ccancel_override)
    cacct = _find_executable("cacct", cacct_override)
    reply = _run_capture_checked(
        [cbatch, "--hold", "--json", "-J", "upgrade-canary", "--wrap", "true"],
        "submit upgrade canary",
    )
    job_id = _parse_cbatch_job_id(reply)
    _status(f"Submitted held upgrade canary with job ID {job_id}")
    try:
        _verify_job_id(backup_dir, job_id)
        _status(f"Canary job ID {job_id} matches the backup manifest")
    finally:
        _run_checked([ccancel, str(job_id)], "cancel upgrade canary")
    for attempt in range(1, CACCT_MAX_ATTEMPTS + 1):
        account_output = _run_capture_checked(
            [cacct, "-j", str(job_id), "--json"],
            f"read cancelled upgrade canary with cacct (attempt {attempt})",
        )
        try:
            _validate_account_output(account_output, job_id)
        except BackupError as error:
            if attempt == CACCT_MAX_ATTEMPTS:
                raise
            _status(f"Canary is not visible in cacct yet: {error}; retrying")
            time.sleep(1)
            continue
        _status(f"Cancelled canary job {job_id} is readable through cacct")
        return job_id, account_output
    raise AssertionError("unreachable")


def _failed_path(path, timestamp):
    return path.with_name(f"{path.name}.failed-upgrade-{timestamp}")


def _rollback_from_backup(args):
    if os.geteuid() != 0:
        raise BackupError("run this command as root to restore backup ownership")

    backup_dir = Path(args.rollback_from).resolve()
    manifest = _load_manifest(backup_dir)
    settings = _settings_from_manifest(manifest)
    config_backup = _require_backup_path(backup_dir, "etc-crane", "directory")
    database_config_backup = _require_backup_path(
        backup_dir, "database.yaml.active", "file"
    )
    mongo_backup = _require_backup_path(
        backup_dir / "mongodb", settings.mongo_database, "directory"
    )
    runtime_backup = _require_backup_path(backup_dir, "cranectld-runtime", "directory")

    pgrep = _find_executable("pgrep", args.pgrep)
    mongosh = _find_executable("mongosh", args.mongosh)
    mongorestore = _find_executable("mongorestore", args.mongorestore)
    cp = _find_executable("cp", args.cp)
    _check_cranectld_stopped(pgrep)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    config_dir = Path(settings.config_dir).resolve()
    failed_config_dir = _failed_path(config_dir, timestamp)
    runtime_dir = Path(settings.runtime_dir).resolve()
    failed_runtime_dir = _failed_path(runtime_dir, timestamp)
    for failed_path in (failed_config_dir, failed_runtime_dir):
        if failed_path.exists():
            raise BackupError(f"rollback destination already exists: {failed_path}")

    _status(
        f"Validated rollback backup for MongoDB database {settings.mongo_database}, "
        f"configuration {config_dir}, and runtime {runtime_dir}"
    )

    drop_command = [
        mongosh,
        "--quiet",
        "--norc",
        *_mongo_connection_args(
            settings,
            args.authentication_database,
            args.tls,
            args.tls_ca_file,
        ),
        settings.mongo_database,
        "--eval",
        "db.dropDatabase()",
    ]
    _run_checked(drop_command, f"drop MongoDB database {settings.mongo_database}")
    restore_command = _build_mongorestore_command(
        mongorestore,
        settings,
        mongo_backup,
        args.authentication_database,
        args.tls,
        args.tls_ca_file,
    )
    _run_checked(restore_command, f"restore MongoDB database {settings.mongo_database}")

    try:
        if config_dir.exists():
            _status(f"Preserving replaced configuration at {failed_config_dir}")
            config_dir.rename(failed_config_dir)
            _status(f"Preserved replaced configuration at {failed_config_dir}")
        _copy_archive(cp, config_backup, config_dir, "restore Crane configuration")

        database_config_path = Path(settings.database_config_path)
        database_config_path.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
        _copy_archive(
            cp,
            database_config_backup,
            database_config_path,
            "restore active database configuration",
        )

        if runtime_dir.exists():
            _status(f"Preserving replaced runtime at {failed_runtime_dir}")
            runtime_dir.rename(failed_runtime_dir)
            _status(f"Preserved replaced runtime at {failed_runtime_dir}")
        runtime_dir.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
        _copy_archive(cp, runtime_backup, runtime_dir, "restore CraneCtld runtime")
    except OSError as error:
        raise BackupError(f"failed to restore filesystem backup: {error}") from error

    return {
        "config_dir": str(config_dir),
        "failed_config_dir": str(failed_config_dir)
        if failed_config_dir.exists()
        else None,
        "runtime_dir": str(runtime_dir),
        "failed_runtime_dir": (
            str(failed_runtime_dir) if failed_runtime_dir.exists() else None
        ),
        "mongo_database": settings.mongo_database,
    }


def _build_parser():
    parser = argparse.ArgumentParser(
        description=(
            "Run the backup, runtime cleanup, ID restoration, canary validation, "
            "and data rollback operations for a CraneSched v1.1.3 upgrade."
        )
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--output", help="create a backup in this new directory")
    mode.add_argument(
        "--validate-backup",
        metavar="BACKUP_DIR",
        help="validate a completed backup and print its operational summary",
    )
    mode.add_argument(
        "--restore-job-ids-from",
        metavar="BACKUP_DIR",
        help="read a completed backup and restore both counters with ccontrol",
    )
    mode.add_argument(
        "--clear-runtime-from",
        metavar="BACKUP_DIR",
        help="validate a completed backup and clear its CraneCtld runtime directory",
    )
    mode.add_argument(
        "--rollback-from",
        metavar="BACKUP_DIR",
        help="restore configuration, MongoDB, and runtime data from a backup",
    )
    mode.add_argument(
        "--run-canary-from",
        metavar="BACKUP_DIR",
        help="submit, verify, cancel, and account a held upgrade canary",
    )
    parser.add_argument(
        "--config", default="/etc/crane/config.yaml", help="Crane config.yaml path"
    )
    parser.add_argument(
        "--database-config",
        help="override DbConfigPath from config.yaml",
    )
    parser.add_argument(
        "--config-dir",
        default="/etc/crane",
        help="configuration directory to archive (default: /etc/crane)",
    )
    parser.add_argument(
        "--authentication-database",
        default="admin",
        help="MongoDB authentication database (default: admin)",
    )
    parser.add_argument("--tls", action="store_true", help="enable MongoDB TLS")
    parser.add_argument("--tls-ca-file", help="MongoDB TLS CA file")
    parser.add_argument("--mongosh", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--mongodump", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--mongorestore", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--cp", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--pgrep", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--ccontrol", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--cbatch", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--ccancel", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--cacct", default=None, help=argparse.SUPPRESS)
    return parser


def main(argv=None):
    args = _build_parser().parse_args(argv)
    try:
        if args.validate_backup:
            result = _validate_backup(args.validate_backup)
            print(f"SUCCESS: Backup validated: {result['backup_dir']}")
            print(f"MongoDB database: {result['mongo_database']}")
            print(f"CraneCtld runtime directory: {result['runtime_dir']}")
            print(f"next_job_id: {result['next_job_id']}")
            print(f"next_job_db_id: {result['next_job_db_id']}")
            return 0
        if args.clear_runtime_from:
            runtime_dir = _clear_runtime(args.clear_runtime_from, args.pgrep)
            print(f"SUCCESS: CraneCtld runtime directory cleared: {runtime_dir}")
            return 0
        if args.rollback_from:
            result = _rollback_from_backup(args)
            print("SUCCESS: Rollback data restored")
            print(f"MongoDB database: {result['mongo_database']}")
            print(f"Configuration directory: {result['config_dir']}")
            print(f"CraneCtld runtime directory: {result['runtime_dir']}")
            if result["failed_config_dir"]:
                print(f"Replaced configuration saved at: {result['failed_config_dir']}")
            if result["failed_runtime_dir"]:
                print(f"Replaced runtime saved at: {result['failed_runtime_dir']}")
            return 0
        if args.run_canary_from:
            job_id, account_output = _run_canary(
                args.run_canary_from,
                args.cbatch,
                args.ccancel,
                args.cacct,
            )
            print(f"SUCCESS: Upgrade canary passed: {job_id}")
            print(account_output.rstrip())
            return 0
        if args.restore_job_ids_from:
            next_job_id, next_job_db_id = _restore_job_ids(
                args.restore_job_ids_from, args.ccontrol
            )
            print("SUCCESS: Job ID counters restored")
            print(f"next_job_id: {next_job_id}")
            print(f"next_job_db_id: {next_job_db_id}")
            return 0
        output_dir, manifest = _perform_backup(args)
    except BackupError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    ids = manifest["job_ids"]
    print(f"SUCCESS: Backup completed: {output_dir}")
    print(f"MongoDB database: {manifest['settings']['mongo_database']}")
    print(f"ID source collection: {ids['collection']}")
    print(
        f"max_job_id: {ids['max_job_id'] if ids['max_job_id'] is not None else '<none>'}"
    )
    print(
        "max_job_db_id: "
        f"{ids['max_job_db_id'] if ids['max_job_db_id'] is not None else '<none>'}"
    )
    print(f"next_job_id: {ids['next_job_id']}")
    print(f"next_job_db_id: {ids['next_job_db_id']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
