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

from tao_open_models.topats_simba_campkeywordeffect_get import TopatsSimbaCampkeywordeffectGet
from tao_open_models.topats_result_get import TopatsResultGet
from global_tools import change_dict_value_type
from settings_report import SAMPLE_EFFECT_DICT

class EffectRptTopatsGet():
    """
    获得店铺的异步效果报表
    """
    TIME_SLOT = 'DAY'
    SEARCH_TYPE = 'SEARCH,CAT'
    SOURCE = '1,2'

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

    def generate_effect_task_id(self):
        """
        生成效果报表的异步task
        """
        for campaign_id in self.campaign_id_list:
            task_id = TopatsSimbaCampkeywordeffectGet.get_campaign_keywordeffect_task(
                    self.nick, campaign_id, EffectRptTopatsGet.TIME_SLOT, EffectRptTopatsGet.SEARCH_TYPE
                    , EffectRptTopatsGet.SOURCE, self.access_token)
            self.task_id_list.append(task_id)
    
    def parse_rpt(self):
        """
        解析报表返回结果，生成[{rpt_record_1}, {rpt_record_2}]格式
        """
        for rpt_data in self.rpt_data_list:
            rpt_data_normal = rpt_data[rpt_data.find('['):rpt_data.find(']')+1]
            one_rpt_record_list = json.loads(rpt_data_normal)
            self.rpt_record_list.extend(one_rpt_record_list)
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
                    change_dict_value_type(single_rpt_dict, SAMPLE_EFFECT_DICT)
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
