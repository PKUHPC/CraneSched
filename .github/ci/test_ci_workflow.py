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
    def test_suite_matching_uses_the_selected_product_branch_and_leaves_override_optional(self):
        product = Path(__file__).resolve().parents[2]
        jobs = yaml.safe_load((product / '.github/workflows/ci.yaml').read_text())['jobs']
        source = next(step for step in jobs['build']['steps'] if step.get('id') == 'source')
        suite = next(step for step in jobs['build']['steps'] if step.get('id') == 'suite')
        self.assertEqual(suite['env']['CI_TEST_REF'], '${{ inputs.harness_ref }}')
        self.assertEqual(suite['env']['CI_PRODUCT_BRANCH'], '${{ steps.source.outputs.source_branch }}')
        self.assertEqual(jobs['integration']['env']['CI_SUITE_REVISION'], '${{ needs.build.outputs.harness_sha }}')
        self.assertEqual(jobs['integration']['env']['CI_TEST_IMAGE'], '${{ needs.build.outputs.test_image }}')
        self.assertEqual(jobs['integration']['env']['CI_ASSET_DIR'], '${{ needs.build.outputs.asset_dir }}')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def git(*args):
                subprocess.run(['git', '-C', str(root), *args], check=True, capture_output=True, timeout=10)
            git('init', '-b', 'feature/product')
            git('-c', 'user.name=CI fixture', '-c', 'user.email=ci@example.invalid', 'commit', '--allow-empty', '-m', 'source')
            git('tag', 'release')
            git('tag', 'feature/product')
            output = root / 'outputs'
            cases = [('', 'feature/pr', 'feature/pr'),
                     ('feature/product', 'feature/workflow', 'feature/product'),
                     ('release', 'feature/workflow', ''), ('a' * 40, 'feature/workflow', '')]
            for selected, event, expected in cases:
                with self.subTest(selected=selected):
                    git('checkout', '--detach' if expected == '' else 'feature/product')
                    output.write_text('')
                    result = subprocess.run(['bash', '-euc', source['run']], cwd=root, capture_output=True,
                                            text=True, timeout=10, env={**os.environ, 'SOURCE_REF': selected,
                                                                       'EVENT_BRANCH': event, 'GITHUB_OUTPUT': str(output)})
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                    values = dict(line.split('=', 1) for line in output.read_text().splitlines())
                    self.assertEqual(values['source_branch'], expected)

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
            ({'build': 'cancelled'}, False),
            ({'build': 'skipped'}, False),
            ({'build': None}, False),
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
                    values = dict(build='success', integration='success', publish='skipped',
                                  upload=False, complete='true', expected='5', passed='5')
                    values.update(changes)
                    needs = {name: {'result': values[name]} for name in ('build', 'integration', 'publish')}
                    if values['build'] is None:
                        del needs['build']
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
