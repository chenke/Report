#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: Chen Ke
@contact: chenke@MaimiaoTech.com
@date: 2012-10-17 18:19
@version: 0.0.0
@license: Copyright MaimiaoTech.com
@copyright: Copyright MaimiaoTech.com

"""
import sys
import time
sys.path.append('../')

from settings_report import mongoConn, report_logger, TOPATS_SLEEP_TIME
from db_models.shop_info_db import ShopInfoDB
from db_models.rpt_base_db import RptBaseDB 
from db_models.rpt_effect_db import RptEffectDB 
from base_rpt_topas_get import BaseRptTopatsGet
from effect_rpt_topas_get import EffectRptTopatsGet
from base_rpt_get import BaseRptGet
from effect_rpt_get import EffectRptGet
from tao_open_models.simba_campaigns_get import SimbaCampaignsGet
from tao_open_models.simba_rpt_campaignbase_get import SimbaRptCampaignBaseGet 
from tao_open_models.simba_rpt_campaigneffect_get import SimbaRptCampaignEffectGet 
from date_tools.date_handle import DateHandle

class RptAll(object):
    """
    根据店铺信息列表 获取所有的报表信息 
    """
    YESTERDAY = ''
    ONE_WEEK_AGO = ''
    
    @classmethod
    def get_day_info(cls):
        RptAll.YESTERDAY = DateHandle.date_to_string(DateHandle.get_yesterday_date())
        RptAll.ONE_WEEK_AGO = DateHandle.date_to_string(DateHandle.get_one_week_ago_date())

    @classmethod
    def get_base_data_topats(cls, shop_info_list):
        """
        获取店铺的所有计划的base数据
        """
        #初始化异步报表下载
        fail_shop_info_list = []
        rpt_topats_list_mid = []
        shop_info_list_mid = []
        for shop_info in shop_info_list:
            try:
                campaign_ids = shop_info['campaign_ids']
                base_rpt_topats = BaseRptTopatsGet(shop_info['sid']
                        , shop_info['nick'], shop_info['access_token']
                        , shop_info['subway_token'], campaign_ids )
                base_rpt_topats.generate_base_task_id()
            except Exception, data:
                report_logger.exception('初始化异步报表出错')
                fail_shop_info_list.append(shop_info) 
                continue
            rpt_topats_list_mid.append(base_rpt_topats)
            shop_info_list_mid.append(shop_info)
            time.sleep(2)
    
        time.sleep(TOPATS_SLEEP_TIME)
        #time.sleep(20)
    
        #获取下载链接, 下载报表并存储
        for i in range(0, len(shop_info_list_mid)):
            rpt_topats = rpt_topats_list_mid[i]
            
            try:
                if not rpt_topats.check_task_ok():
                    fail_shop_info_list.append(shop_info_list_mid[i])
                if not rpt_topats.download_rpt():
                    fail_shop_info_list.append(shop_info_list_mid[i])
                if not rpt_topats.parse_rpt():
                    fail_shop_info_list.append(shop_info_list_mid[i])
                rpt_base_db = RptBaseDB(mongoConn, str(shop_info_list_mid[i]['sid']))
                rpt_base_db.clear_one_day_ago_rpt(RptAll.ONE_WEEK_AGO)
                rpt_base_db.clear_one_day_rpt(RptAll.YESTERDAY)
                rpt_base_db.save_records(rpt_topats.rpt_record_list)
            except Exception, data:
                report_logger.exception('获取下载链接，抓取或存储出错')
                fail_shop_info_list.append(shop_info_list_mid[i])
            
            time.sleep(0.2)
    
        return fail_shop_info_list
    
    @classmethod
    def get_effect_data_topats(cls, shop_info_list):
        """
        获取店铺的所有计划的base数据
        """
        #初始化异步报表下载
        fail_shop_info_list = []
        rpt_topats_list_mid = []
        shop_info_list_mid = []
        for shop_info in shop_info_list:
            try:
                campaign_ids = shop_info['campaign_ids']
                effect_rpt_topats = EffectRptTopatsGet(shop_info['sid']
                        , shop_info['nick'], shop_info['access_token']
                        , shop_info['subway_token'], campaign_ids )
                effect_rpt_topats.generate_effect_task_id()
            except Exception, data:
                report_logger.exception('初始化异步报表出错')
                fail_shop_info_list.append(shop_info) 
                continue
            rpt_topats_list_mid.append(effect_rpt_topats)
            shop_info_list_mid.append(shop_info)
            time.sleep(0.1)
    
        time.sleep(TOPATS_SLEEP_TIME)
        #time.sleep(20)
    
        #获取下载链接, 下载报表并存储
        for i in range(0, len(shop_info_list_mid)):
            rpt_topats = rpt_topats_list_mid[i]
            try:
                if not rpt_topats.check_task_ok():
                    fail_shop_info_list.append(shop_info_list_mid[i])
                if not rpt_topats.download_rpt():
                    fail_shop_info_list.append(shop_info_list_mid[i])
                if not rpt_topats.parse_rpt():
                    fail_shop_info_list.append(shop_info_list_mid[i])
            except Exception, data:
                report_logger.exception('获取下载链接，抓取或存储出错')
                fail_shop_info_list.append(shop_info_list_mid[i])
    
            rpt_effect_db = RptEffectDB(mongoConn, str(shop_info_list_mid[i]['sid']))
            rpt_effect_db.clear_one_day_ago_rpt(RptAll.ONE_WEEK_AGO)
            rpt_effect_db.clear_one_day_rpt(RptAll.YESTERDAY)
            rpt_effect_db.save_records(rpt_topats.rpt_record_list)
            time.sleep(2)
    
        return fail_shop_info_list
    
    @classmethod
    def get_campaigns_by_shop_info(cls, shop_info):
        """
        根据店铺信息获得店铺所有的计划信息
        """
        campaigns = SimbaCampaignsGet.get_campaign_list(shop_info['access_token']
                , shop_info['nick'])
        campaign_ids = []
        for campaign in campaigns:
            campaign_ids.append(campaign.campaign_id)
    
        return campaign_ids
    
    @classmethod
    def get_shop_infos_by_shop_ids(cls, in_shop_id_list):
        """
        根据店铺id获得店铺相应信息包括access_token,subway_token,nick
        """
        shop_info_list = []
        shop_info_db = ShopInfoDB(mongoConn)
        for shop_id in in_shop_id_list:
            shop_info = shop_info_db.get_shop_info_by_shop_id(shop_id)
            try:
                shop_info.update({'campaign_ids':RptAll.get_campaigns_by_shop_info(shop_info)})
            except Exception, data:
                report_logger.exception('get campaign_ids failed')
                continue
            shop_info_list.append(shop_info)
    
        return shop_info_list
    
    @classmethod
    def is_base_rpt_correct(cls, shop_info):
        rpt_base_db = RptBaseDB(mongoConn, str(shop_info['sid']))
        summary_rpt = rpt_base_db.get_one_day_summary_rpt(RptAll.YESTERDAY)
    
        summary_rpt_stand = {}
        for campaign_id in shop_info['campaign_ids']:
            rpt_base_yesterday = SimbaRptCampaignBaseGet.get_yesterday_rpt_campaignbase_list(
                    shop_info['nick'],  campaign_id, 'SEARCH,CAT', '1,2', shop_info['access_token'], shop_info['subway_token'])
            
            summary_rpt_stand[campaign_id] = {'impressions':rpt_base_yesterday['impression']
                    , 'click':rpt_base_yesterday['click']
                    , 'cost':rpt_base_yesterday['cost']}
        
        for campaign_id in summary_rpt_stand.keys():
            if summary_rpt_stand[campaign_id]['click'] == 0 and summary_rpt_stand[campaign_id]['cost'] == 0 \
                    and summary_rpt_stand[campaign_id]['impressions'] == 0: 
                continue
            if not summary_rpt.has_key(campaign_id):
                return False
            if summary_rpt_stand[campaign_id] != summary_rpt[campaign_id]:
                return False
        return True
    
    @classmethod
    def get_base_data(cls, shop_info_list):
    
        for shop_info in shop_info_list:
            try:
                if RptAll.is_base_rpt_correct(shop_info):
                    continue
                
                report_logger.info("店铺  [%s] 需要重新获取基础报表", shop_info['nick'])
                rpt_base_db = RptBaseDB(mongoConn, str(shop_info['sid']))
                rpt_base_db.clear_one_day_ago_rpt(RptAll.ONE_WEEK_AGO)
                rpt_base_db.clear_one_day_rpt(RptAll.YESTERDAY)
        
                base_rpt_get = BaseRptGet(shop_info['nick']
                        , shop_info['access_token'], shop_info['subway_token'], shop_info['campaign_ids']) 
            
                base_rpt_list = base_rpt_get.yesterday_rpt_get()
                rpt_base_db.save_records(base_rpt_list)
                report_logger.info("店铺  [%s] 重新获取基础报表完成，条数 %d", shop_info['nick'], len(base_rpt_list))
            except Exception, data:
                report_logger.exception('get base data exception')
        return True
    
    @classmethod
    def is_effect_rpt_correct(cls, shop_info):
        rpt_effect_db = RptEffectDB(mongoConn, str(shop_info['sid']))
        summary_rpt = rpt_effect_db.get_one_day_summary_rpt(RptAll.YESTERDAY)
    
        summary_rpt_stand = {}
        for campaign_id in shop_info['campaign_ids']:
            rpt_effect_yesterday = SimbaRptCampaignEffectGet.get_yesterday_rpt_campaigneffect_list(
                    shop_info['nick'],  campaign_id, 'SEARCH,CAT', '1,2', shop_info['access_token'], shop_info['subway_token'])
    
            summary_rpt_stand[campaign_id] = {'pay':rpt_effect_yesterday['pay']
                    , 'paycount':rpt_effect_yesterday['paycount']
                    , 'fav':rpt_effect_yesterday['fav']
                    }
        
        for campaign_id in summary_rpt_stand.keys():
            if summary_rpt_stand[campaign_id]['pay'] == 0 and summary_rpt_stand[campaign_id]['paycount'] == 0 \
                    and summary_rpt_stand[campaign_id]['fav'] == 0: 
                continue
            if not summary_rpt.has_key(campaign_id):
                return False
            if summary_rpt_stand[campaign_id] != summary_rpt[campaign_id]:
                return False
        return True
    
    @classmethod
    def get_effect_data(cls, shop_info_list):
    
        for shop_info in shop_info_list:
            try:
                if RptAll.is_effect_rpt_correct(shop_info):
                    continue
                
                report_logger.info("店铺  [%s] 需要重新获取效果报表", shop_info['nick'])
                rpt_effect_db = RptEffectDB(mongoConn, str(shop_info['sid']))
                rpt_effect_db.clear_one_day_ago_rpt(RptAll.ONE_WEEK_AGO)
                rpt_effect_db.clear_one_day_rpt(RptAll.YESTERDAY)
    
                effect_rpt_get = EffectRptGet(shop_info['nick']
                        , shop_info['access_token'], shop_info['subway_token'], shop_info['campaign_ids']) 
        
                effect_rpt_list = effect_rpt_get.yesterday_rpt_get()
                rpt_effect_db.save_records(effect_rpt_list)
                report_logger.info("店铺  [%s] 重新获取效果报表完成，条数 %d", shop_info['nick'], len(effect_rpt_list))
            except Exception, data:
                report_logger.exception('get effect data exception')
    
        return True


if __name__ == '__main__':
    shop_info_db = ShopInfoDB(mongoConn)
    shop_id_list = shop_info_db.get_all_shop_id_list()
    #shop_id_list = shop_id_list[0:3]
    #shop_id_list = ['58735843']
    
    RptAll.get_day_info()
    shop_info_list = RptAll.get_shop_infos_by_shop_ids(shop_id_list)
    report_logger.info('获得店铺信息完成, 店铺总数 %d', len(shop_info_list))
    base_fail_list = RptAll.get_base_data_topats(shop_info_list)
    report_logger.info('异步基础报表抓取完成, 已知失败店铺数 %d', len(base_fail_list))
    effect_fail_list = RptAll.get_effect_data_topats(shop_info_list)
    report_logger.info('异步效果报表抓取完成, 已知失败店铺数 %d', len(effect_fail_list))
    
    #通过同步报表，修正异步报表错误，或者补充异步报表
    RptAll.get_base_data(shop_info_list)    
    report_logger.info("补充基础报表完成")
    RptAll.get_effect_data(shop_info_list)    
    report_logger.info("补充效果报表完成")
    
    
