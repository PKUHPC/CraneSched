"""Check C/C++ and protocol styles, preserving an applicable correction patch."""
import os
from pathlib import Path
import subprocess
import sys


def check_format():
    formatter = os.environ.get('CLANG_FORMAT', 'clang-format')
    extensions = {'.c', '.cc', '.cpp', '.cxx', '.h', '.hh', '.hpp', '.hxx'}
    groups = [([], 'file'), ([], 'file:.clang-format-proto')]
    for directory in ('src', 'test', 'protos'):
        root = Path(directory)
        if not root.is_dir():
            raise RuntimeError(f'missing format input directory: {directory}')
        for path in sorted(root.rglob('*')):
            if path.is_file():
                if directory == 'protos' and path.suffix == '.proto':
                    groups[1][0].append(str(path))
                elif directory != 'protos' and path.suffix in extensions:
                    groups[0][0].append(str(path))
    if not any(files for files, _style in groups):
        raise RuntimeError('no format input files found')
    patch = Path('format.patch')
    patch.unlink(missing_ok=True)
    invalid = []
    for files, style in groups:
        if files and subprocess.run([formatter, '--dry-run', '--Werror', '--style=' + style, *files]).returncode:
            invalid.append((files, style))
    if not invalid:
        print('All files are formatted.')
        return False
    for files, style in invalid:
        subprocess.run([formatter, '-i', '--style=' + style, *files], check=True)
    diff = subprocess.check_output(['git', 'diff', '--binary', '--', 'src', 'test', 'protos'])
    if not diff:
        raise RuntimeError('clang-format reported an error without producing source changes')
    patch.write_bytes(diff)
    summary = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary:
        with Path(summary).open('a') as stream:
            stream.write('### Clang format check failed\n\n'
                         'Download `format-diff` and run:\n\n'
                         '```bash\ngit apply format.patch\n```\n')
    return True


def main():
    changed = False
    try:
        changed = check_format()
        code = int(changed)
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f'::error::clang-format failed: {error}', file=sys.stderr)
        code = 2
    output = os.environ.get('GITHUB_OUTPUT')
    if output:
        with Path(output).open('a') as stream:
            stream.write(f'changed={str(changed).lower()}\n')
    return code


if __name__ == '__main__':
    sys.exit(main())
