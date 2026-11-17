from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

import yaml


CI = Path(__file__).resolve().parent
PRODUCT = CI.parents[1]
WORKFLOW = CI.parent / 'workflows/ci.yaml'
FORMAT_SCRIPT = CI / 'check-format.py'


class FormatWorkflowContractTest(unittest.TestCase):
    def test_build_consumes_the_revision_checked_by_formatting(self):
        jobs = yaml.safe_load(WORKFLOW.read_text())['jobs']
        self.assertEqual(jobs['build']['needs'], 'format')
        checkout = next(step for step in jobs['build']['steps']
                        if step.get('uses', '').startswith('actions/checkout@')
                        and step.get('with', {}).get('path') != '.ci-tools')
        self.assertEqual(checkout['with']['ref'], '${{ needs.format.outputs.source_sha }}')
        self.assertIn('format', jobs['summary']['needs'])

    def test_failed_formatting_is_uploaded_before_the_job_failure_gate(self):
        steps = yaml.safe_load(WORKFLOW.read_text())['jobs']['format']['steps']
        check = next(step for step in steps if step.get('id') == 'format')
        upload = next(step for step in steps if step.get('uses', '').startswith('actions/upload-artifact@'))
        gate = next(step for step in steps if step.get('id') == 'format_gate')
        self.assertTrue(check['continue-on-error'])
        self.assertIn("steps.format.outputs.changed == 'true'", upload['if'])
        self.assertIn("steps.format.outcome == 'failure'", gate['if'])
        self.assertLess(steps.index(upload), steps.index(gate))

class FormatScriptBehaviorTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.formatter = shutil.which('clang-format-21') or shutil.which('clang-format')
        self.assertIsNotNone(self.formatter, 'run these checks in the Dev toolchain')
        for name in ('.clang-format', '.clang-format-proto'):
            shutil.copyfile(PRODUCT / name, self.root / name)
        for directory in ('src', 'test', 'protos'):
            (self.root / directory).mkdir()
        self.files = {'src/with space.cpp': 'int main() { return 0; }\n',
                      'protos/example.proto': 'syntax = "proto3";\nmessage Example { string name = 1; }\n'}
        for relative, content in self.files.items():
            path = self.root / relative
            path.write_text(content)
            style = 'file:.clang-format-proto' if path.suffix == '.proto' else 'file'
            subprocess.run([self.formatter, '-i', '--style=' + style, relative], cwd=self.root, check=True)
        subprocess.run(['git', 'init', '-q'], cwd=self.root, check=True)
        self.commit()

    def commit(self):
        subprocess.run(['git', 'add', '.'], cwd=self.root, check=True)
        subprocess.run(['git', '-c', 'user.name=CI', '-c', 'user.email=ci@example.invalid',
                        'commit', '-qm', 'Format fixture'], cwd=self.root, check=True)

    def run_check(self, formatter=None):
        return subprocess.run(
            [sys.executable, str(FORMAT_SCRIPT)], cwd=self.root,
            env={**os.environ, 'CLANG_FORMAT': formatter or self.formatter,
                 'GITHUB_OUTPUT': str(self.root / 'outputs'),
                 'GITHUB_STEP_SUMMARY': str(self.root / 'summary')},
            capture_output=True, text=True, timeout=10)

    def test_formatted_source_passes_without_a_patch_or_source_change(self):
        result = self.run_check()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual((self.root / 'outputs').read_text(), 'changed=false\n')
        self.assertFalse((self.root / 'format.patch').exists())
        subprocess.run(['git', 'diff', '--exit-code', '--', 'src', 'test', 'protos'], cwd=self.root, check=True)

    def test_format_error_fails_and_produces_an_applicable_cpp_and_proto_patch(self):
        for relative in self.files:
            path = self.root / relative
            path.write_text(path.read_text().replace(' ', '   '))
        self.commit()
        result = self.run_check()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual((self.root / 'outputs').read_text(), 'changed=true\n')
        formatted = {relative: (self.root / relative).read_bytes() for relative in self.files}
        patch = self.root / 'format.patch'
        self.assertGreater(patch.stat().st_size, 0)
        subprocess.run(['git', 'restore', '--', *self.files], cwd=self.root, check=True)
        subprocess.run(['git', 'apply', str(patch)], cwd=self.root, check=True)
        self.assertEqual({relative: (self.root / relative).read_bytes() for relative in self.files}, formatted)
        for relative in self.files:
            style = 'file:.clang-format-proto' if relative.endswith('.proto') else 'file'
            subprocess.run([self.formatter, '--dry-run', '--Werror', '--style=' + style, relative],
                           cwd=self.root, check=True)

    def test_formatter_failure_fails_without_claiming_a_patch(self):
        result = self.run_check(formatter='/bin/false')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual((self.root / 'outputs').read_text(), 'changed=false\n')
        self.assertFalse((self.root / 'format.patch').exists())


if __name__ == '__main__':
    unittest.main()
