import os
import re
import json
import logging
import collections

from utils import merge_dict


logger = logging.getLogger()

def get_all_system_test_cases(folder: str) -> list:
    """
    get all cases by path, and then merge by base case, the level of val in cur case is higher than it in base case.
    :param folder: path for all cases
    :return: get a deque with all system cases for running time
    """
    cases_dict = dict()  # json格式原始case
    for dir_path, dir_names, file_names in os.walk(folder):  #遍历folder文件夹下所有子目录和文件
        for file_name in file_names:
            match = re.search(r'^case_(?P<name>\w+)\.json$', file_name)
            if match is None:
                continue
            case_name = match.group('name')
            with open(os.path.join(dir_path, file_name), encoding='utf-8') as f:
                case_json = json.load(f)
                case['name'] = case_name
                cases_dict[case_name] = case_json

    cases_deque = collections.deque()
    for case_name, case_json in cases_dict.items():
        logger.info('get case name: {}'.format(case_name))
        case_json['request'] = merge_dict(case_json.get('request', {}), cases_dict[case_json['inherit']]['request'])
        cases_deque.append(case_json)
    return cases_deque
