import aiohttp
import asyncio
import json
import base64
import os

def merge_dict(dic1: dict, dic2: dict, path: str = ''):
    """overrride the value of dic2 by dic1
    :param dict1: dict1 in currrent case
    :param dict2: dict2 in base case
    :return: a new dict
    """
    for key in dic2:
        if key in dic1:
            if isinstance(dic1[key], dict) and isinstance(dic2[key], dict):
                merge_dict(dic1[key], dic2[key], path + '.{}'.format(key))
            elif not isinstance(dic1[key], type(dic2[key])):
                raise Exception("type of {} in not same, dic1 is {}, dic2 is {}"
                                .format(path + '.{}'.format(key), type(dic1[key]), type(dic2[key])))
        else:
            dic1[key] = dic2[key]
    return dic1