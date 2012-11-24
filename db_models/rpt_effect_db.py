#encoding=utf8
__author__ = 'lym liyangmin@maimiaotech.com'


import sys
import os

curr_path = os.path.dirname(__file__)
sys.path.append(os.path.join(curr_path,'../../Lib/'))
sys.path.append(os.path.join(curr_path,'../../TaobaoOpenPythonSDK/'))
from common.decorator import mongo_exception 

class RptEffectDB(object):
    """
    class to operate ShopInfo 
    """
    @classmethod
    @mongo_exception
    def __init__(cls, in_conn, in_shop_id):
        """
        """
        cls._conn = in_conn
        cls._db = in_shop_id 
        cls.rpt_effect_coll = cls._conn[cls._db]['rpt_effect']

    @classmethod
    @mongo_exception
    def save_records(cls, records):
        """
        保存日志数据
        """
        for record in records:
            cls.rpt_effect_coll.save(record)
    
    @classmethod
    @mongo_exception
    def clear_one_day_rpt(cls, day_str):
        """
        清除一天的日志数据
        """
        for record in cls.rpt_effect_coll.find({'date':day_str}):
            cls.rpt_effect_coll.remove({'_id':record['_id']})

    @classmethod
    @mongo_exception
    def clear_one_day_ago_rpt(cls, day_str):
        """
        清除某一天之前的全部日志数据
        """
        for record in cls.rpt_effect_coll.find({'date':{'$lt':day_str}}):
            cls.rpt_effect_coll.remove({'_id':record['_id']})

    @classmethod
    @mongo_exception
    def get_one_day_summary_rpt(cls, day_str):
        """
        按计划统计一天的汇总日志数据
        """
        summary_rpt_dict = {}
        for record in cls.rpt_effect_coll.find({'date':day_str}):
            campaign_id = record['campaignid']
            if summary_rpt_dict.has_key(campaign_id):
                summary_rpt_dict[campaign_id]['fav'] += record['favitemcount']
                summary_rpt_dict[campaign_id]['fav'] += record['favshopcount']
                summary_rpt_dict[campaign_id]['pay'] += record['indirectpay']
                summary_rpt_dict[campaign_id]['pay'] += record['directpay']
                summary_rpt_dict[campaign_id]['paycount'] += record['indirectpaycount']
                summary_rpt_dict[campaign_id]['paycount'] += record['directpaycount']
            else:
                if record.has_key('favitemcount') and record.has_key('favshopcount') \
                        and record.has_key('indirectpay') and record.has_key('directpay') \
                        and record.has_key('indirectpaycount') and record.has_key('directpaycount'):
                    summary_rpt_dict[campaign_id] = {
                            'fav':record['favitemcount'] + record['favshopcount']
                            , 'pay':record['indirectpay'] + record['directpay']
                            , 'paycount':record['indirectpaycount'] + record['directpaycount']
                            }

        #for key in summary_rpt_dict.keys():
        #    summary_rpt_dict[key]['impressions'] /=3
        #    summary_rpt_dict[key]['click'] /=3
        #    summary_rpt_dict[key]['cost'] /=3

        #print "ssss:", summary_rpt_dict
        return summary_rpt_dict


if __name__ == '__main__':

    MONGODB_HOST = 'app.maimiaotech.com'
    MONGODB_PORT = 2006
    from pymongo import Connection
    mongoConn = Connection(host = MONGODB_HOST, port = MONGODB_PORT)
    
    rpt_effect_db = RptEffectDB(mongoConn, '57620080')
    rpt_effect_db.save_records([{'a':1, 'b':3}, {'a':5, 'b':9}])
