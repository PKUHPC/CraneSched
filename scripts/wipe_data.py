#!/usr/bin/env python3

import argparse
import logging
import os
import sys
from enum import Enum
from pathlib import Path

import pymongo
import yaml
from unqlite import UnQLite

logger = logging.getLogger()
# Suppress overly verbose logs from pymongo
logging.getLogger("pymongo").setLevel(logging.INFO)


class Collection(Enum):
    ACCT = "acct_table"
    QOS = "qos_table"
    TASK = "task_table"
    USER = "user_table"
    WCKEY = "wckey_table"
    RESOURCE = "license_resource_table"

    SUMMARY = "summary_time_table"
    ACC_HOUR = "acc_usage_hour_table"
    ACC_DAY = "acc_usage_day_table"
    ACC_MONTH = "acc_usage_month_table"


def load_config(crane_path: str, db_path: str = None):
    """Load and validate configurations."""
    global_config = _read_config(crane_path)

    # db_path in param comes first
    real_db_path = db_path if db_path else global_config.get("DbConfigPath")
    base_dir = global_config.get("CraneBaseDir")
    if not real_db_path or not base_dir:
        raise ValueError("Missing keys in config (DbConfigPath, CraneBaseDir).")

    db_config = _read_config(real_db_path)

    username = db_config.get("DbUser")
    password = db_config.get("DbPassword")
    host = db_config.get("DbHost")
    port = db_config.get("DbPort")
    dbname = db_config.get("DbName")
    embedded_db_path = os.path.join(base_dir, db_config.get("CraneCtldDbPath", ""))

    if not all([username, password, host, port, dbname, embedded_db_path]):
        raise ValueError("Missing keys in DB config parameters.")

    return username, password, host, port, dbname, embedded_db_path


def wipe_collection(db, collection: Collection):
    """Delete all documents in a MongoDB collection."""
    collection_name = collection.value
    try:
        logger.debug(f"Wiping collection {collection_name}...")
        db[collection_name].delete_many({})
    except Exception as e:
        logger.error(f"Error wiping collection {collection_name}: {e}")
        raise e


def wipe_embedded(embedded_db_path: str, keep_id: bool):
    """Remove embedded database files."""

    # Try to preserve task ID
    var_db_path = embedded_db_path + "var"
    next_task_id_key = "NI"
    next_task_id_val = None

    if os.path.exists(var_db_path):
        try:
            db = UnQLite(var_db_path)
            if next_task_id_key in db:
                next_task_id_val = db[next_task_id_key]
                logger.info(f"Found existing task ID data (len={len(next_task_id_val)})")
            db.close()
        except Exception as e:
            logger.warning(f"Failed to read task ID from {var_db_path}: {e}")

    db_dir = Path(embedded_db_path).parent
    db_filename = Path(embedded_db_path).name

    if db_dir.exists():
        logger.debug(f"Removing files matching {db_filename}* in {db_dir}...")
        for file in db_dir.glob(f"{db_filename}*"):
            try:
                file.unlink()
            except Exception as e:
                logger.error(f"Error removing file {file}: {e}")
                raise e

    # Restore task ID
    if keep_id and next_task_id_val is not None:
        try:
            if not db_dir.exists():
                db_dir.mkdir(parents=True, exist_ok=True)

            db = UnQLite(var_db_path)
            db[next_task_id_key] = next_task_id_val
            db.close()
            logger.info(f"Restored task ID to {var_db_path}")
        except Exception as e:
            logger.error(f"Error restoring task ID to {var_db_path}: {e}")
            raise e


def wipe_mongo(db, collections: list[Collection]):
    """Handle MongoDB collection wiping based on user input."""
    for c in collections:
        wipe_collection(db, c)


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


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="CraneSched Cleanup Script")
    parser.add_argument(
        "mode",
        choices=["mongo", "embedded", "all"],
        help="Mode of operation: 'mongo' (MongoDB only), 'embedded' (embedded DB only), 'all' (both MongoDB and embedded DB).",
    )
    parser.add_argument("--keep-id",
                        action="store_true",
                        default="Keep embedded DB task ID(default: false).")
    parser.set_defaults(keep_id=False)
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
        "-a",
        "--acct_table",
        action="store_true",
        help="Include acct_table in MongoDB wipe.",
    )
    parser.add_argument(
        "-q",
        "--qos_table",
        action="store_true",
        help="Include qos_table in MongoDB wipe.",
    )
    parser.add_argument(
        "-t",
        "--task_table",
        action="store_true",
        help="Include task_table in MongoDB wipe.",
    )
    parser.add_argument(
        "-u",
        "--user_table",
        action="store_true",
        help="Include user_table in MongoDB wipe.",
    )
    parser.add_argument(
        "-w",
        "--wckey_table",
        action="store_true",
        help="Include wckey_table in MongoDB wipe.",
    )
    parser.add_argument(
        "-r",
        "--resource_table",
        action="store_true",
        help="Include resource_table in MongoDB wipe.",
    )
    parser.add_argument(
        "-s", "--summary_tale",
        action="store_true",
        help="Include hour/day/month/summary_table in MongoDB wipe.",
    )
    return parser.parse_args()


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
    username, password, host, port, dbname, embedded_db_path = load_config(args.config, args.db_config)

    logger.debug(f"MongoDB Config: {username}, {host}, {port}, {dbname}")
    logger.debug(f"Embedded DB Config: {embedded_db_path}")

    # Handle MongoDB cleanup
    db = None
    if args.mode in ["mongo", "all"]:
        db = connect_to_mongo(username, password, host, port, dbname)
        to_wipe = []
        if not any([args.acct_table, args.qos_table, args.task_table, args.user_table, args.wckey_table, args.resource_table]):
            to_wipe = [
                Collection.ACCT,
                Collection.QOS,
                Collection.TASK,
                Collection.USER,
                Collection.WCKEY,
                Collection.RESOURCE,
                Collection.SUMMARY,
                Collection.ACC_HOUR,
                Collection.ACC_DAY,
                Collection.ACC_MONTH
            ]  # Default to all
        else:
            if args.acct_table:
                to_wipe.append(Collection.ACCT)
            if args.qos_table:
                to_wipe.append(Collection.QOS)
            if args.task_table:
                to_wipe.append(Collection.TASK)
            if args.user_table:
                to_wipe.append(Collection.USER)
            if args.wckey_table:
                to_wipe.append(Collection.WCKEY)
            if args.resource_table:
                to_wipe.append(Collection.RESOURCE)
            if args.summary_tale:
                to_wipe.append(Collection.SUMMARY)
                to_wipe.append(Collection.ACC_HOUR)
                to_wipe.append(Collection.ACC_DAY)
                to_wipe.append(Collection.ACC_MONTH)
        wipe_mongo(db, to_wipe)

    # Handle embedded database cleanup
    if args.mode in ["embedded", "all"]:
        wipe_embedded(embedded_db_path, args.keep_id)

    logger.info("Done.")


if __name__ == "__main__":
    try:
        _main()
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
