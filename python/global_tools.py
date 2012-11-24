#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: Chen Ke
@contact: chenke@MaimiaoTech.com
@date: 2012-10-18 17:10
@version: 0.0.0
@license: Copyright MaimiaoTech.com
@copyright: Copyright MaimiaoTech.com

"""

def align_two_value(value1, value2):
    if value1 == "":
        return value1
    elif type(value2) == type(0L):
        return long(value1)
    elif type(value2) == type(0):
        return int(value1)
    elif type(value2) == type('0'):
        return str(value1)
    elif type(value2) == type(0.0):
        return float(value1)
    else:
        return value1
        
def change_dict_value_type(input_dict, sample_dict):
    for key in input_dict.keys():
        if sample_dict.has_key(key):
            input_dict[key] = align_two_value(input_dict[key], sample_dict[key])


