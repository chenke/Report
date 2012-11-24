#encoding=utf8
__author__ = 'lym liyangmin@maimiaotech.com'


import sys
import os

curr_path = os.path.dirname(__file__)
sys.path.append(os.path.join(curr_path,'../../Lib/'))
sys.path.append(os.path.join(curr_path,'../../TaobaoOpenPythonSDK/'))
from common.decorator import mongo_exception 

class RptBaseDB(object):
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
        cls.rpt_base_coll = cls._conn[cls._db]['rpt_base']

    @classmethod
    @mongo_exception
    def save_records(cls, records):
        """
        保存日志数据
        """
        for record in records:
            cls.rpt_base_coll.save(record)

    @classmethod
    @mongo_exception
    def clear_one_day_ago_rpt(cls, day_str):
        """
        清除某一天之前的全部日志数据
        """
        for record in cls.rpt_base_coll.find({'date':{'$lt':day_str}}):
            cls.rpt_base_coll.remove({'_id':record['_id']})

    @classmethod
    @mongo_exception
    def clear_one_day_rpt(cls, day_str):
        """
        清除一天的日志数据
        """
        for record in cls.rpt_base_coll.find({'date':day_str}):
            cls.rpt_base_coll.remove({'_id':record['_id']})

    @classmethod
    @mongo_exception
    def get_one_day_summary_rpt(cls, day_str):
        """
        按计划统计一天的汇总日志数据
        """
        summary_rpt_dict = {}
        for record in cls.rpt_base_coll.find({'date':day_str}):
            campaign_id = record['campaignid']
            if summary_rpt_dict.has_key(campaign_id):
                summary_rpt_dict[campaign_id]['impressions'] += record['impressions']
                summary_rpt_dict[campaign_id]['click'] += record['click']
                summary_rpt_dict[campaign_id]['cost'] += record['cost']
            else:
                summary_rpt_dict[campaign_id] = {'impressions':record['impressions']
                        , 'click':record['click'], 'cost':record['cost']}

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
    
    rpt_base_db = RptBaseDB(mongoConn, '57620080')
    rpt_base_db.save_records([{'a':1, 'b':3}, {'a':5, 'b':9}])
