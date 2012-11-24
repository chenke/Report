#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: Chen Ke
@contact: chenke@MaimiaoTech.com
@date: 2012-06-06 09:50
@version: 0.0.0
@license: Copyright MaimiaoTech.com
@copyright: Copyright MaimiaoTech.com

"""
import os
import sys
import logging 

curr_path = os.path.dirname(__file__)
sys.path.append(os.path.join(curr_path,'../../../TaobaoOpenPythonSDK/'))
sys.path.append(os.path.join(curr_path,'../../../Lib/'))

MONGODB_HOST = 'app.maimiaotech.com'
MONGODB_PORT = 2006
from pymongo import Connection
mongoConn = Connection(host = MONGODB_HOST, port = MONGODB_PORT)

report_log_file = '/tmp/longtail_report_log'
#report_logger = logging.getLogger('report')
report_logger = logging.getLogger()
report_hdlr = logging.FileHandler(report_log_file)
report_formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s %(message)s')
report_hdlr.setFormatter(report_formatter)
report_logger.addHandler(report_hdlr)
report_logger.setLevel(logging.DEBUG)
console = logging.StreamHandler()
console.setFormatter(report_formatter)
report_logger.addHandler(console)

SAMPLE_BASE_DICT = {"avgpos" : 46, "nick" : "chinchinstyle", "adgroupid" : 132178175, "cpm" : 0.0, "ctr" : 0.0, "campaignid" : 3367690, "cpc" : 0.0, "source" : "SUMMARY", "cost" : 0, "keywordstr" : "桑蚕丝 围巾 披肩", "searchtype" : "SEARCH", "keywordid" : 17655322592L, "date" : "2012-09-18", "impressions" : 5, "click" : 0 }

SAMPLE_EFFECT_DICT = { "nick" : "chinchinstyle", "adgroupid" : 136883507, "favshopcount" : 0, "searchtype" : "SEARCH", "directpay" : 0, "campaignid" : 3367690, "indirectpay" : 0, "source" : "SUMMARY", "favitemcount" : 0, "keywordstr" : "女士丝巾", "indirectpaycount" : 0, "keywordid" : 18569643752L, "date" : "2012-10-09", "directpaycount" : 0 }

TOPATS_SLEEP_TIME = 600
