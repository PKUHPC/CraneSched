#!/usr/bin/env python3

"""
MongoDB Migration Script: Add 'has_job_info' field

This script is used to upgrade old CraneSched databases that do not have the
'has_job_info' field in the task_table collection. It sets 'has_job_info' to
true for all existing task records that contain complete job information
(identified by the presence of the 'account' field) but are missing the
'has_job_info' field.

Usage:
    python3 migrate_has_job_info.py -C /etc/crane/config.yaml
    python3 migrate_has_job_info.py -C /etc/crane/config.yaml --dry-run
"""

import argparse
import logging
import sys

import pymongo
import yaml

logger = logging.getLogger()
# Suppress overly verbose logs from pymongo
logging.getLogger("pymongo").setLevel(logging.INFO)

TASK_COLLECTION = "task_table"


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

    # db_path in param comes first
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


def migrate_has_job_info(db, dry_run=False):
    """
    Add 'has_job_info: true' to task records that have complete job information
    but are missing the 'has_job_info' field.

    A record is considered to have complete job information if it contains the
    'account' field, which is always present in records created by InsertJob
    or InsertRecoveredJob.
    """
    collection = db[TASK_COLLECTION]

    # Find documents that:
    # 1. Do not have the 'has_job_info' field
    # 2. Have the 'account' field (indicating complete job info)
    query = {
        "has_job_info": {"$exists": False},
        "account": {"$exists": True},
    }

    count = collection.count_documents(query)
    logger.info(
        f"Found {count} task record(s) with complete job info "
        f"but missing 'has_job_info' field."
    )

    if count == 0:
        logger.info("No migration needed.")
        return

    if dry_run:
        logger.info("[Dry run] Would update %d record(s). No changes made.", count)
        return

    result = collection.update_many(query, {"$set": {"has_job_info": True}})
    logger.info(
        f"Successfully updated {result.modified_count} record(s) "
        f"with 'has_job_info: true'."
    )


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="CraneSched MongoDB Migration: Add 'has_job_info' field"
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
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Only show the number of records that would be updated without making changes.",
    )
    return parser.parse_args()


def _main():
    args = parse_arguments()

    # Set logging for standalone run
    global logger
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        level=logging.DEBUG,
        format="[%(levelname)s] [%(filename)s:%(lineno)d] %(message)s",
    )

    # Load configurations
    username, password, host, port, dbname = load_config(args.config, args.db_config)

    logger.debug(f"MongoDB Config: {host}, {port}, {dbname}")

    # Connect and migrate
    db = connect_to_mongo(username, password, host, port, dbname)
    migrate_has_job_info(db, dry_run=args.dry_run)

    logger.info("Done.")


if __name__ == "__main__":
    try:
        _main()
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
