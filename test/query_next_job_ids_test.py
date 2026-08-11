#!/usr/bin/env python3

import argparse
import importlib.util
import pathlib
import unittest


SCRIPT_PATH = (
    pathlib.Path(__file__).resolve().parents[1] / "scripts" / "query_next_job_ids.py"
)
SPEC = importlib.util.spec_from_file_location("query_next_job_ids", SCRIPT_PATH)
QUERY_NEXT_JOB_IDS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(QUERY_NEXT_JOB_IDS)


class QueryNextJobIdsTest(unittest.TestCase):
    def test_parse_mongosh_output(self):
        output = 'connection notice\n{"max_job_id":"31","max_job_db_id":"47"}\n'
        self.assertEqual(QUERY_NEXT_JOB_IDS._parse_mongosh_output(output), (31, 47))

    def test_empty_collection_starts_at_one(self):
        output = '{"max_job_id":null,"max_job_db_id":null}\n'
        maximums = QUERY_NEXT_JOB_IDS._parse_mongosh_output(output)
        self.assertEqual(maximums, (None, None))
        self.assertEqual(QUERY_NEXT_JOB_IDS._next_id(None, 2**32 - 1, "max_job_id"), 1)

    def test_rejects_invalid_and_exhausted_ids(self):
        with self.assertRaises(ValueError):
            QUERY_NEXT_JOB_IDS._parse_mongosh_output(
                '{"max_job_id":"invalid","max_job_db_id":"1"}\n'
            )
        with self.assertRaises(ValueError):
            QUERY_NEXT_JOB_IDS._next_id(2**32 - 1, 2**32 - 1, "max_job_id")

    def test_builds_read_only_mongosh_command(self):
        args = argparse.Namespace(
            host="db.example.org",
            port=27018,
            username="crane",
            authentication_database="admin",
            tls=True,
            tls_ca_file="/etc/crane/mongo-ca.pem",
            database="crane_db",
        )
        command = QUERY_NEXT_JOB_IDS._build_mongosh_command(args, "/usr/bin/mongosh")
        self.assertEqual(command[0], "/usr/bin/mongosh")
        self.assertIn("db.example.org", command)
        self.assertIn("crane_db", command)
        self.assertIn("--tls", command)
        self.assertIn("--tlsCAFile", command)
        self.assertNotIn("--password", command)
        self.assertNotIn("ccontrol", " ".join(command))


if __name__ == "__main__":
    unittest.main()
