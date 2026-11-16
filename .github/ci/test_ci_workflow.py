from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml


class CIResultTest(unittest.TestCase):
    def test_real_summary_rejects_failed_cancelled_missing_or_partial_phases(self):
        workflow = yaml.safe_load((Path(__file__).parents[1] / 'workflows/ci.yaml').read_text())
        script = workflow['jobs']['summary']['steps'][0]['run']
        cases = [
            ({}, True),
            ({'publish': 'success', 'upload': True}, True),
            ({'format': 'failure'}, False),
            ({'format': 'cancelled'}, False),
            ({'format': 'skipped'}, False),
            ({'build': 'failure'}, False),
            ({'integration': 'cancelled'}, False),
            ({'integration': 'skipped'}, False),
            ({'complete': 'false'}, False),
            ({'expected': '0', 'passed': '0'}, False),
            ({'passed': '4'}, False),
            ({'publish': 'failure', 'upload': True}, False),
            ({'publish': 'skipped', 'upload': True}, False),
        ]
        with tempfile.TemporaryDirectory() as directory:
            for changes, success in cases:
                with self.subTest(changes=changes):
                    values = dict(format='success', build='success', integration='success', publish='skipped',
                                  upload=False, complete='true', expected='5', passed='5')
                    values.update(changes)
                    needs = {name: {'result': values[name]} for name in ('format', 'build', 'integration', 'publish')}
                    needs['integration']['outputs'] = {name: values[name] for name in ('complete', 'expected', 'passed')}
                    result = subprocess.run(
                        ['bash', '-c', script], capture_output=True, text=True, timeout=10,
                        env={**os.environ, 'JOB_RESULTS': json.dumps(needs),
                             'PUBLISH_REQUESTED': str(values['upload']).lower(),
                             'GITHUB_STEP_SUMMARY': str(Path(directory) / 'summary')},
                    )
                    self.assertEqual(result.returncode == 0, success, result.stdout + result.stderr)


if __name__ == '__main__':
    unittest.main()
