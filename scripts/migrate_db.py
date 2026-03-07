#!/usr/bin/env python3

"""
CraneSched MongoDB Migration Tool

A general-purpose database migration script for CraneSched. Each migration
is a self-contained function that can be run independently or as part of
a full migration.

Usage:
    # List all available migrations
    python3 migrate_db.py list -C /etc/crane/config.yaml

    # Run all pending migrations (dry run)
    python3 migrate_db.py run -C /etc/crane/config.yaml --dry-run

    # Run all pending migrations
    python3 migrate_db.py run -C /etc/crane/config.yaml

    # Run a specific migration by name
    python3 migrate_db.py run -C /etc/crane/config.yaml -n add_has_job_info
"""

import argparse
import logging
import sys
from dataclasses import dataclass
from typing import Callable, List, Optional

import pymongo
import yaml

logger = logging.getLogger()
# Suppress overly verbose logs from pymongo
logging.getLogger("pymongo").setLevel(logging.INFO)

TASK_COLLECTION = "task_table"


# ======================== Migration Registry ========================


@dataclass
class Migration:
    """Represents a single database migration."""

    name: str
    description: str
    func: Callable[[pymongo.database.Database, bool], None]


# Global migration registry, ordered by registration.
_migrations: List[Migration] = []


def register_migration(name: str, description: str):
    """Decorator to register a migration function."""

    def decorator(func: Callable[[pymongo.database.Database, bool], None]):
        _migrations.append(Migration(name=name, description=description, func=func))
        return func

    return decorator


def get_migrations() -> List[Migration]:
    """Return all registered migrations."""
    return list(_migrations)


def get_migration_by_name(name: str) -> Optional[Migration]:
    """Return a migration by name, or None if not found."""
    for m in _migrations:
        if m.name == name:
            return m
    return None


# ======================== Migration Functions ========================


@register_migration(
    name="add_has_job_info",
    description="Add 'has_job_info: true' to legacy task records that have "
    "complete job info but are missing this field.",
)
def migrate_has_job_info(db: pymongo.database.Database, dry_run: bool = False):
    """
    Add 'has_job_info: true' to task records that have complete job information
    but are missing the 'has_job_info' field.

    A record is considered to have complete job information if it contains the
    'account' field, which is always present in records created by InsertJob
    or InsertRecoveredJob.
    """
    collection = db[TASK_COLLECTION]

    query = {
        "has_job_info": {"$exists": False},
        "account": {"$exists": True},
    }

    count = collection.count_documents(query)
    logger.info(
        f"[add_has_job_info] Found {count} task record(s) with complete job "
        f"info but missing 'has_job_info' field."
    )

    if count == 0:
        logger.info("[add_has_job_info] No migration needed.")
        return

    if dry_run:
        logger.info(
            "[add_has_job_info] [Dry run] Would update %d record(s). "
            "No changes made.",
            count,
        )
        return

    result = collection.update_many(query, {"$set": {"has_job_info": True}})
    logger.info(
        f"[add_has_job_info] Successfully updated {result.modified_count} "
        f"record(s) with 'has_job_info: true'."
    )


@register_migration(
    name="add_exclusive",
    description="Add 'exclusive: false' to legacy task records (pre-v1.1.3).",
)
def migrate_exclusive(db: pymongo.database.Database, dry_run: bool = False):
    """Add 'exclusive: false' to task records missing this field."""
    collection = db[TASK_COLLECTION]
    query = {"exclusive": {"$exists": False}}
    count = collection.count_documents(query)
    logger.info(f"[add_exclusive] Found {count} record(s) missing 'exclusive' field.")

    if count == 0:
        logger.info("[add_exclusive] No migration needed.")
        return
    if dry_run:
        logger.info(f"[add_exclusive] [Dry run] Would update {count} record(s).")
        return

    result = collection.update_many(query, {"$set": {"exclusive": False}})
    logger.info(f"[add_exclusive] Updated {result.modified_count} record(s).")


@register_migration(
    name="add_cpus_mem_alloc",
    description="Add 'cpus_alloc'/'mem_alloc' fields (copy from req values).",
)
def migrate_cpus_mem_alloc(db: pymongo.database.Database, dry_run: bool = False):
    """
    Add 'cpus_alloc' and 'mem_alloc' to task records missing these fields.
    Values are copied from cpus_req and mem_req respectively.
    """
    collection = db[TASK_COLLECTION]
    query = {
        "$or": [
            {"cpus_alloc": {"$exists": False}},
            {"mem_alloc": {"$exists": False}},
        ]
    }
    count = collection.count_documents(query)
    logger.info(f"[add_cpus_mem_alloc] Found {count} record(s) missing alloc fields.")

    if count == 0:
        logger.info("[add_cpus_mem_alloc] No migration needed.")
        return
    if dry_run:
        logger.info(f"[add_cpus_mem_alloc] [Dry run] Would update {count} record(s).")
        return

    # Use aggregation pipeline to copy cpus_req -> cpus_alloc, mem_req -> mem_alloc
    pipeline = [
        {
            "$set": {
                "cpus_alloc": {"$ifNull": ["$cpus_alloc", "$cpus_req"]},
                "mem_alloc": {"$ifNull": ["$mem_alloc", "$mem_req"]},
            }
        }
    ]
    result = collection.update_many(query, pipeline)
    logger.info(f"[add_cpus_mem_alloc] Updated {result.modified_count} record(s).")


@register_migration(
    name="add_device_map",
    description="Add 'device_map: {}' to legacy task records (pre-v1.1.3).",
)
def migrate_device_map(db: pymongo.database.Database, dry_run: bool = False):
    """Add 'device_map: {}' to task records missing this field."""
    collection = db[TASK_COLLECTION]
    query = {"device_map": {"$exists": False}}
    count = collection.count_documents(query)
    logger.info(f"[add_device_map] Found {count} record(s) missing 'device_map'.")

    if count == 0:
        logger.info("[add_device_map] No migration needed.")
        return
    if dry_run:
        logger.info(f"[add_device_map] [Dry run] Would update {count} record(s).")
        return

    result = collection.update_many(query, {"$set": {"device_map": {}}})
    logger.info(f"[add_device_map] Updated {result.modified_count} record(s).")


@register_migration(
    name="add_wckey_fields",
    description="Add 'wckey'/'using_default_wckey' fields to legacy records.",
)
def migrate_wckey_fields(db: pymongo.database.Database, dry_run: bool = False):
    """Add 'wckey: ""' and 'using_default_wckey: false' to records missing them."""
    collection = db[TASK_COLLECTION]
    query = {
        "$or": [
            {"wckey": {"$exists": False}},
            {"using_default_wckey": {"$exists": False}},
        ]
    }
    count = collection.count_documents(query)
    logger.info(f"[add_wckey_fields] Found {count} record(s) missing wckey fields.")

    if count == 0:
        logger.info("[add_wckey_fields] No migration needed.")
        return
    if dry_run:
        logger.info(f"[add_wckey_fields] [Dry run] Would update {count} record(s).")
        return

    pipeline = [
        {
            "$set": {
                "wckey": {"$ifNull": ["$wckey", ""]},
                "using_default_wckey": {"$ifNull": ["$using_default_wckey", False]},
            }
        }
    ]
    result = collection.update_many(query, pipeline)
    logger.info(f"[add_wckey_fields] Updated {result.modified_count} record(s).")


@register_migration(
    name="add_licenses_alloc",
    description="Add 'licenses_alloc: {}' to legacy task records (pre-v1.1.3).",
)
def migrate_licenses_alloc(db: pymongo.database.Database, dry_run: bool = False):
    """Add 'licenses_alloc: {}' to task records missing this field."""
    collection = db[TASK_COLLECTION]
    query = {"licenses_alloc": {"$exists": False}}
    count = collection.count_documents(query)
    logger.info(f"[add_licenses_alloc] Found {count} record(s) missing 'licenses_alloc'.")

    if count == 0:
        logger.info("[add_licenses_alloc] No migration needed.")
        return
    if dry_run:
        logger.info(f"[add_licenses_alloc] [Dry run] Would update {count} record(s).")
        return

    result = collection.update_many(query, {"$set": {"licenses_alloc": {}}})
    logger.info(f"[add_licenses_alloc] Updated {result.modified_count} record(s).")


@register_migration(
    name="add_nodename_list",
    description="Add 'nodename_list: []' to legacy task records (pre-v1.1.3).",
)
def migrate_nodename_list(db: pymongo.database.Database, dry_run: bool = False):
    """Add 'nodename_list: []' to task records missing this field."""
    collection = db[TASK_COLLECTION]
    query = {"nodename_list": {"$exists": False}}
    count = collection.count_documents(query)
    logger.info(f"[add_nodename_list] Found {count} record(s) missing 'nodename_list'.")

    if count == 0:
        logger.info("[add_nodename_list] No migration needed.")
        return
    if dry_run:
        logger.info(f"[add_nodename_list] [Dry run] Would update {count} record(s).")
        return

    result = collection.update_many(query, {"$set": {"nodename_list": []}})
    logger.info(f"[add_nodename_list] Updated {result.modified_count} record(s).")


@register_migration(
    name="add_cluster",
    description="Add 'cluster: \"\"' to legacy task records (pre-v1.1.3).",
)
def migrate_cluster(db: pymongo.database.Database, dry_run: bool = False):
    """Add 'cluster: ""' to task records missing this field."""
    collection = db[TASK_COLLECTION]
    query = {"cluster": {"$exists": False}}
    count = collection.count_documents(query)
    logger.info(f"[add_cluster] Found {count} record(s) missing 'cluster' field.")

    if count == 0:
        logger.info("[add_cluster] No migration needed.")
        return
    if dry_run:
        logger.info(f"[add_cluster] [Dry run] Would update {count} record(s).")
        return

    result = collection.update_many(query, {"$set": {"cluster": ""}})
    logger.info(f"[add_cluster] Updated {result.modified_count} record(s).")


# ======================== Config & Connection ========================


def _read_config(path):
    """Read configuration from a YAML file."""
    try:
        with open(path, "r") as file:
            config = yaml.safe_load(file)
        return config
    except FileNotFoundError as e:
        logger.error(f"Error: Configuration file {path} not found.")
        raise e
    except yaml.YAMLError as e:
        logger.error(f"Error: Failed to parse YAML file {path}: {e}")
        raise e


def load_config(crane_path: str, db_path: str = None):
    """Load and validate configurations."""
    global_config = _read_config(crane_path)

    real_db_path = db_path if db_path else global_config.get("DbConfigPath")
    if not real_db_path:
        raise ValueError("Missing key in config (DbConfigPath).")

    db_config = _read_config(real_db_path)

    username = db_config.get("DbUser")
    password = db_config.get("DbPassword")
    host = db_config.get("DbHost")
    port = db_config.get("DbPort")
    dbname = db_config.get("DbName")

    if not all([username, password, host, port, dbname]):
        raise ValueError("Missing keys in DB config parameters.")

    return username, password, host, port, dbname


def connect_to_mongo(username, password, host, port, dbname):
    """Establish a connection to MongoDB."""
    try:
        client = pymongo.MongoClient(
            host=host,
            port=int(port),
            username=username,
            password=password,
            authSource="admin",
        )
        return client[dbname]
    except Exception as e:
        logger.error(f"Error: Failed to connect to MongoDB: {e}")
        raise e


# ======================== CLI ========================


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="CraneSched MongoDB Migration Tool"
    )
    parser.add_argument(
        "-C",
        "--config",
        default="/etc/crane/config.yaml",
        help="Path to the crane config. Default: /etc/crane/config.yaml",
    )
    parser.add_argument(
        "-D",
        "--db-config",
        default=None,
        help="Path to the DB config. Default is the value in crane config.",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # 'list' sub-command
    subparsers.add_parser("list", help="List all available migrations.")

    # 'run' sub-command
    run_parser = subparsers.add_parser("run", help="Run migrations.")
    run_parser.add_argument(
        "-n",
        "--name",
        default=None,
        help="Run only the migration with this name. If omitted, all migrations are run.",
    )
    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Only show what would be changed without making modifications.",
    )

    return parser.parse_args()


def cmd_list():
    """Handle the 'list' command."""
    migrations = get_migrations()
    if not migrations:
        print("No migrations registered.")
        return
    print(f"{'Name':<30} Description")
    print(f"{'-' * 30} {'-' * 11}")
    for m in migrations:
        print(f"{m.name:<30} {m.description}")


def cmd_run(db, name=None, dry_run=False):
    """Handle the 'run' command."""
    if name:
        migration = get_migration_by_name(name)
        if migration is None:
            logger.error(f"Migration '{name}' not found.")
            logger.info(
                "Available migrations: "
                + ", ".join(m.name for m in get_migrations())
            )
            sys.exit(1)
        to_run = [migration]
    else:
        to_run = get_migrations()

    if not to_run:
        logger.info("No migrations to run.")
        return

    for migration in to_run:
        logger.info(f"Running migration: {migration.name} ...")
        migration.func(db, dry_run=dry_run)

    logger.info("All migrations completed.")


# ======================== Main ========================


def _main():
    args = parse_arguments()

    global logger
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        level=logging.DEBUG,
        format="[%(levelname)s] [%(filename)s:%(lineno)d] %(message)s",
    )

    if args.command == "list":
        cmd_list()
        return

    if args.command == "run":
        username, password, host, port, dbname = load_config(
            args.config, args.db_config
        )
        logger.debug(f"MongoDB Config: {host}, {port}, {dbname}")

        db = connect_to_mongo(username, password, host, port, dbname)
        cmd_run(db, name=args.name, dry_run=args.dry_run)
        return

    # No sub-command provided
    logger.error("No command specified. Use 'list' or 'run'.")
    sys.exit(1)


if __name__ == "__main__":
    try:
        _main()
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
