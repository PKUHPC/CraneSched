import logging
import argparse
import yaml
import sys
import json
import traceback

from case_handle import get_all_system_test_cases

logger = logging.getLogger()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config_path', type=str, default='conf/conf.yaml',
                        help='config file')
    parser.add_argument('--folder', type=str, default='testcases', help='cases to use')
    args = parser.parse_args()

    cases = get_all_system_test_cases(args.folder)
    for case in enumerate(cases):
        case_name = case['name']
        logger.info('execute case {} '.format(case_name))
        is_succ = True
        service = globals()[args.conf['service']['class']](**args.conf['service']['args'])
        try:
            resps = service.run(case_name, case['request'], case.get('exec_times', 1))
        except:
            is_succ = False
            logger.error('case {} execute failed, service may have crashed!'.format(case_name))
            traceback.print_exc()

        if is_succ:
            print(json.dumps(resps, indent=2))
        service.reset()


if __name__ == '__main__':
    main()
