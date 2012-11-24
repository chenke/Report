#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: Chen Ke
@contact: chenke@MaimiaoTech.com
@date: 2012-10-17 18:05
@version: 0.0.0
@license: Copyright MaimiaoTech.com
@copyright: Copyright MaimiaoTech.com

"""
import sys, os, datetime
sys.path.append('../../Lib/')

from tao_open_models.simba_adgroups_get import SimbaAdgroupsGet 
from tao_open_models.simba_rpt_adgroupkeywordbase_get import SimbaRptAdgroupkeywordbaseGet 
from common.exceptions import TBDataNotReadyException
from date_tools.date_handle import DateHandle

class BaseRptGet(object):
    """
    class to update campaign adgroup base report
    """
    def __init__(self, in_nick, in_access_token, in_subway_token, in_campaign_list):
        self.nick = in_nick
        self.access_token = in_access_token
        self.subway_token = in_subway_token
        self.campaign_list = in_campaign_list
        self.search_type = 'CAT,SEARCH,NOSEARCH'
        self.source = '1,2'

        
    def _get_adgroup_list(self):
        
        adgroup_list = []

        for campaign in self.campaign_list:
            tmp_list = SimbaAdgroupsGet.get_adgroup_list_by_campaign(self.access_token, self.nick, campaign)
            adgroup_list.extend(tmp_list)
        
        return adgroup_list

            
    def _rpt_adgroupkeyword_base_update(self,adgroup_list, start_date, end_date):

        base_rpt_list_all = []
        for adgroup in adgroup_list:

            'get and store base_rpts to daily database'
            #TODO: 确定报表类型
            try:
                base_rpt_list = SimbaRptAdgroupkeywordbaseGet.get_rpt_adgroupkeywordbase_list(
                        self.nick, adgroup.campaign_id, adgroup.adgroup_id, start_date,
                        end_date, '1,2', self.search_type, self.access_token, self.subway_token)
            except TBDataNotReadyException,e:
                raise

            if not base_rpt_list:
                continue
            
            base_rpt_list_all.extend(base_rpt_list)

        return base_rpt_list_all
   
    def keys_to_lower(self, base_rpt_list):
        for base_rpt in base_rpt_list:
            for key in base_rpt.keys():
                if key.lower() != key:
                    base_rpt[key.lower()] = base_rpt[key]
                    del base_rpt[key]


    def yesterday_rpt_get(self):
        yesterday_date = DateHandle.get_yesterday_date()
        yesterday_date_str = DateHandle.date_to_ustring(yesterday_date)

        start_date = yesterday_date_str
        end_date = yesterday_date_str
        adgroup_list = self._get_adgroup_list()
        base_rpt_list = self._rpt_adgroupkeyword_base_update(adgroup_list, start_date, end_date)
        BaseRptGet.keys_to_lower(self, base_rpt_list)

        return base_rpt_list

if __name__ == '__main__':
    
    access_token = '6201306555701dfhb9dee99fdff5dbcb08c40f0f11eebe633816634'
    nick = 'caoxuejun1'
    campaign_id = '8116979'
    adgroup_id = '134057150'
    subway_token = '1104449174-27957743-1349227298700-083186dc'

    base_rpt_get = BaseRptGet(nick, access_token, subway_token, [campaign_id])
    base_rpt_list = base_rpt_get.yesterday_rpt_get()

    for base_rpt in base_rpt_list:
        print base_rpt



