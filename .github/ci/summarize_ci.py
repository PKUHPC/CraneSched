"""Summarize the CI jobs and enforce the required workflow result."""
import json
import os
from pathlib import Path


def main():
    jobs = json.loads(os.environ['JOB_RESULTS'])
    integration = jobs.get('integration', {}).get('outputs', {})
    upload = os.environ['PUBLISH_REQUESTED'] == 'true'
    complete = integration.get('complete') == 'true'
    expected = int(integration.get('expected') or '0')
    passed = int(integration.get('passed') or '0')
    results = {name: jobs.get(name, {}).get('result', 'missing')
               for name in ('build', 'integration', 'publish')}
    success = (all(results[name] == 'success' for name in ('build', 'integration'))
               and complete and expected > 0 and passed == expected
               and results['publish'] == ('success' if upload else 'skipped'))
    lines = ['### Build, Test and Publish', '',
             f"- Build: {results['build']}",
             f"- Integration: {results['integration']} ({passed}/{expected})",
             f"- Package upload: {results['publish']} (requested: {str(upload).lower()})"]
    with Path(os.environ['GITHUB_STEP_SUMMARY']).open('a') as summary:
        summary.write('\n'.join(lines) + '\n')
    if not success:
        raise SystemExit('A required phase failed, was cancelled, skipped or incomplete.')
    print('All requested CI phases completed successfully.')


if __name__ == '__main__':
    main()
