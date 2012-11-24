#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: Chen Ke
@contact: chenke@MaimiaoTech.com
@date: 2012-09-11 20:21
@version: 0.0.0
@license: Copyright MaimiaoTech.com
@copyright: Copyright MaimiaoTech.com

"""
import sys 
sys.path.append('../')
import urllib2

from tao_open_models.topats_simba_campkeywordbase_get import TopatsSimbaCampkeywordbaseGet
from tao_open_models.topats_result_get import TopatsResultGet
from settings_report import SAMPLE_BASE_DICT
from global_tools import change_dict_value_type
import simplejson as json


class BaseRptTopatsGet():
    """
    获得店铺的异步报表
    """

    TIME_SLOT = 'DAY'
    SEARCH_TYPE = 'SEARCH,CAT'

    def __init__(self, in_shop_id, in_nick, in_access_token, in_subway_token, in_campaign_id_list):
        self.shop_id = in_shop_id
        self.nick = in_nick
        self.access_token = in_access_token
        self.subway_token = in_subway_token
        self.campaign_id_list = in_campaign_id_list
        self.download_url_list = []
        self.task_id_list = []
        self.rpt_data_list = []
        self.rpt_record_list = []

    def generate_base_task_id(self, ):
        """
        生成异步api的task_id
        """
        for campaign_id in self.campaign_id_list:
            source = '1'
            self.task_id_list.append(TopatsSimbaCampkeywordbaseGet.get_campaign_keywordbase_task(
                self.nick, campaign_id, BaseRptTopatsGet.TIME_SLOT, BaseRptTopatsGet.SEARCH_TYPE
                , source, self.access_token))
            source = '2'
            self.task_id_list.append(TopatsSimbaCampkeywordbaseGet.get_campaign_keywordbase_task(
                self.nick, campaign_id, BaseRptTopatsGet.TIME_SLOT, BaseRptTopatsGet.SEARCH_TYPE
                , source, self.access_token))

        return 
    
    def parse_rpt(self):
        """
        解析报表返回结果，生成[{rpt_record_1}, {rpt_record_2}]格式
        """
        for rpt_data in self.rpt_data_list:
            rpt_data_normal = rpt_data[rpt_data.find('['):rpt_data.find(']')+1]
            if len(rpt_data_normal) <= 10:
                continue
            one_rpt_record_list = json.loads(rpt_data_normal.lower())
            one_rpt_record_list_filter = []
            for element in one_rpt_record_list:
                if len(element.keys()) >= 13:
                    one_rpt_record_list_filter.append(element)
            self.rpt_record_list.extend(one_rpt_record_list_filter)
        return True
            
    def parse_rpt_old(self):
        """
        解析报表返回结果，生成[{rpt_record_1}, {rpt_record_2}]格式
        """
        for rpt_data in self.rpt_data_list:
            field_list = []
            init_flag = True
            for line in rpt_data.split('\n'):
                if init_flag:
                    for field in line.strip().split(','):
                        if field.find('nick') >= 0:
                            field_list.append('nick')
                        else:
                            field_list.append(field.strip())
                    init_flag = False 
                    continue
                single_rpt_dict = {}
                value_count = 0
                for value in line.strip().split(','):
                    field = field_list[value_count]
                    single_rpt_dict[field] = value
                    value_count += 1 
                if len(single_rpt_dict.keys()) == len(field_list):
                    change_dict_value_type(single_rpt_dict, SAMPLE_BASE_DICT)
                    self.rpt_record_list.append(single_rpt_dict)

        return True

    def download_rpt(self):
        """
        下载报表
        """
        rpt_data_tmp = []
        for download_url in self.download_url_list:
            urlopen = urllib2.urlopen(download_url)
            rpt_data_tmp.append(urlopen.read())

        if len(rpt_data_tmp) != len(self.download_url_list):
            return False
        #print rpt_data_tmp 
        self.rpt_data_list.extend(rpt_data_tmp)
        return True

    def check_task_ok(self):
        """
        判断异步task是否完成，完成获得下载链接
        """
        download_url_tmp = []
        for task_id in self.task_id_list:
            result = TopatsResultGet.get_task_result(task_id, self.access_token)
            if not result.download_url:
                return False    
            download_url_tmp.append(result.download_url)
        
        if len(download_url_tmp) != len(self.task_id_list):
            return False

        self.download_url_list.extend(download_url_tmp)
        return True

        
if __name__ == '__main__':
    pass 
