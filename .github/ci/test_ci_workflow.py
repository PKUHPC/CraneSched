from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import yaml


class CIResultTest(unittest.TestCase):
    def test_go_rebuild_clears_removed_package_inputs_and_preserves_caches(self):
        product = Path(__file__).resolve().parents[2]
        cleanup = product / '.github/ci/prepare-go-package-inputs.sh'
        package = yaml.safe_load((product / 'go/.goreleaser.yaml').read_text())
        self.assertTrue(any(item.get('src') == 'build/plugin/mail.so'
                            for archive in package['nfpms'] for item in archive['contents']))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            go = root / 'go'
            (go / 'plugin/dummy').mkdir(parents=True)
            for relative in ('bin/removed', 'plugin/mail.so', 'tool/removed'):
                path = go / 'build' / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text('previous revision')
            sentinels = [root / name / 'keep' for name in ('go-cache', 'module-cache', 'ccache', 'cmake-build-ci-debug')]
            for path in sentinels:
                path.parent.mkdir()
                path.write_text('persistent cache')
            compiler = root / 'fake-go'
            compiler.write_text('#!/usr/bin/env python3\nimport sys\nfrom pathlib import Path\n'
                                'if "-o" in sys.argv:\n'
                                '    path=Path(sys.argv[sys.argv.index("-o")+1]); path.parent.mkdir(parents=True, exist_ok=True); path.write_text("current revision")\n'
                                'else: print("fixture compiler")\n')
            compiler.chmod(0o755)
            command = ['make', '--no-print-directory', '-f', str(product / 'go/Makefile'),
                       '-o', 'protos', 'plugin-other', 'PLUGIN_NAMES=dummy', 'GO=' + str(compiler)]
            def build():
                result = subprocess.run(command, cwd=go, capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            build()
            self.assertFalse((go / 'plugin/mail').exists())
            self.assertEqual((go / 'build/plugin/mail.so').read_text(), 'previous revision')
            environment = {key: value for key, value in os.environ.items() if key != 'BASH_ENV'}
            subprocess.run([str(cleanup), str(root)], cwd=root, env=environment, check=True, timeout=10)
            build()
            self.assertFalse((go / 'build/plugin/mail.so').exists())
            self.assertFalse((go / 'build/bin/removed').exists())
            self.assertFalse((go / 'build/tool/removed').exists())
            self.assertEqual((go / 'build/plugin/dummy.so').read_text(), 'current revision')
            self.assertTrue(all(path.read_text() == 'persistent cache' for path in sentinels))

    def test_cpack_collection_excludes_intermediate_rpms(self):
        collector = Path(__file__).resolve().parents[2] / '.github/ci/collect-rpm-packages.sh'
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
                [str(collector)],
                env={**os.environ, 'ARTIFACT_DIR': str(root)},
                capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            files = list((root / 'packages/rpm').rglob('*'))
            self.assertEqual({path.name for path in files}, {'craned.rpm', 'cranectld.rpm'})
            self.assertTrue(all(path.read_text() == path.name for path in files))

    def test_real_summary_rejects_failed_cancelled_missing_or_partial_phases(self):
        script = Path(__file__).with_name('summarize_ci.py')
        cases = [
            ({}, True),
            ({'publish': 'success', 'upload': True}, True),
            ({'format': 'failure'}, False),
            ({'format': 'cancelled'}, False),
            ({'format': 'skipped'}, False),
            ({'format': None}, False),
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
                    if values['format'] is None:
                        del needs['format']
                    needs['integration']['outputs'] = {name: values[name] for name in ('complete', 'expected', 'passed')}
                    result = subprocess.run(
                        [sys.executable, str(script)], capture_output=True, text=True, timeout=10,
                        env={**os.environ, 'JOB_RESULTS': json.dumps(needs),
                             'PUBLISH_REQUESTED': str(values['upload']).lower(),
                             'GITHUB_STEP_SUMMARY': str(Path(directory) / 'summary')},
                    )
                    self.assertEqual(result.returncode == 0, success, result.stdout + result.stderr)


if __name__ == '__main__':
    unittest.main()
