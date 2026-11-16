from __future__ import annotations

import json
import hashlib
import io
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import yaml


class CIResultTest(unittest.TestCase):
    def test_reuse_gate_requires_a_successful_attempt_and_unchanged_selected_rpms(self):
        workflow = yaml.safe_load((Path(__file__).parents[1] / 'workflows/ci.yaml').read_text())
        script = next(step['run'] for step in workflow['jobs']['build']['steps'] if step.get('id') == 'reuse')
        script = script.split("python3 - <<'PY'\n", 1)[1].rsplit('\nPY', 1)[0]
        names = ('cranesched-cranectld', 'cranesched-craned', 'cranesched-client',
                 'cranesched-plugin', 'cranesched-slurm-compat')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            build = root / 'attempts/123-1/build'
            packages = build / 'packages/rpm'
            packages.mkdir(parents=True)
            records = []
            for name in names:
                path = packages / (name + '.rpm')
                path.write_text(name)
                records.append({'name': name, 'path': str(path.relative_to(build)),
                                'architecture': 'x86_64', 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
            manifest = {'schema': 1, 'sourceRevision': 'a' * 40, 'nativeTestsPassed': True,
                        'baseImage': 'pinned-base', 'target': 'rhel10-amd64', 'preset': 'ci-debug',
                        'packages': records}
            (build / 'manifest.json').write_text(json.dumps(manifest))
            self.assertEqual(script.count("Path('/ci/attempts')"), 1)
            script = script.replace("Path('/ci/attempts')", f'Path({str(root / "attempts")!r})')
            environment = {'REUSE_BUILD': '123/1', 'SOURCE_REF': '', 'BASE_IMAGE': 'pinned-base',
                           'GITHUB_API_URL': 'https://api.github.invalid', 'GITHUB_REPOSITORY': 'example/product',
                           'GH_TOKEN': 'unit-test-token', 'GITHUB_OUTPUT': str(root / 'outputs')}
            def run(status='success', **changes):
                response = io.BytesIO(json.dumps({'status': 'completed', 'conclusion': status}).encode())
                with patch.dict(os.environ, {**environment, **changes}, clear=True), \
                     patch('urllib.request.urlopen', return_value=response) as api:
                    exec(compile(script, '<reuse workflow>', 'exec'), {})
                    return api
            api = run()
            self.assertEqual(api.call_args.args[0].full_url,
                             'https://api.github.invalid/repos/example/product/actions/runs/123/attempts/1')
            outputs = (root / 'outputs').read_text()
            self.assertIn('source_sha=' + 'a' * 40, outputs)
            for status in ('failure', 'cancelled', 'skipped'):
                with self.subTest(status=status), self.assertRaises(SystemExit):
                    run(status)
            with self.assertRaises(SystemExit):
                run(REUSE_BUILD='../123/1')
            with self.assertRaises(SystemExit):
                run(SOURCE_REF='different-source')
            (packages / (names[0] + '.rpm')).write_text('changed after acceptance')
            with self.assertRaisesRegex(SystemExit, 'changed'):
                run()

    def test_cpack_collection_excludes_intermediate_rpms(self):
        workflow = yaml.safe_load((Path(__file__).parents[1] / 'workflows/ci.yaml').read_text())
        build = workflow['jobs']['build']['steps'][1]['run']
        start = build.index('mapfile -t backend_rpms')
        collection = build[start:build.index("GITHUB_TOKEN=", start)]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'packages/rpm').mkdir(parents=True)
            cpack = root / 'cpack-backend'
            intermediate = cpack / '_CPack_Packages/Linux/RPM/RPMS'
            intermediate.mkdir(parents=True)
            for name in ('craned.rpm', 'cranectld.rpm'):
                (cpack / name).write_text(name)
                (intermediate / name).write_text('temporary duplicate')
            result = subprocess.run(
                ['bash', '-euo', 'pipefail', '-c', collection],
                env={**os.environ, 'artifact_dir': str(root)},
                capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            files = list((root / 'packages/rpm').rglob('*'))
            self.assertEqual({path.name for path in files}, {'craned.rpm', 'cranectld.rpm'})
            self.assertTrue(all(path.read_text() == path.name for path in files))

    def test_real_summary_rejects_failed_cancelled_missing_or_partial_phases(self):
        workflow = yaml.safe_load((Path(__file__).parents[1] / 'workflows/ci.yaml').read_text())
        script = workflow['jobs']['summary']['steps'][0]['run']
        cases = [
            ({}, True),
            ({'publish': 'success', 'upload': True}, True),
            ({'format': 'failure'}, False),
            ({'format': 'cancelled'}, False),
            ({'format': 'skipped'}, False),
            ({'format': 'skipped', 'reused': True}, True),
            ({'format': 'skipped', 'reused': True, 'build': 'failure'}, False),
            ({'format': 'skipped', 'reused': True, 'complete': 'false'}, False),
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
                                  upload=False, reused=False, complete='true', expected='5', passed='5')
                    values.update(changes)
                    needs = {name: {'result': values[name]} for name in ('format', 'build', 'integration', 'publish')}
                    needs['integration']['outputs'] = {name: values[name] for name in ('complete', 'expected', 'passed')}
                    result = subprocess.run(
                        ['bash', '-c', script], capture_output=True, text=True, timeout=10,
                        env={**os.environ, 'JOB_RESULTS': json.dumps(needs),
                             'PUBLISH_REQUESTED': str(values['upload']).lower(),
                             'BUILD_REUSED': str(values['reused']).lower(),
                             'GITHUB_STEP_SUMMARY': str(Path(directory) / 'summary')},
                    )
                    self.assertEqual(result.returncode == 0, success, result.stdout + result.stderr)


if __name__ == '__main__':
    unittest.main()
