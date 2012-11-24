#encoding=utf8
__author__ = 'lym liyangmin@maimiaotech.com'


import sys
import os

curr_path = os.path.dirname(__file__)
sys.path.append(os.path.join(curr_path,'../../Lib/'))
sys.path.append(os.path.join(curr_path,'../../TaobaoOpenPythonSDK/'))
from common.decorator import mongo_exception 

class ShopInfoDB(object):
    """
    class to operate ShopInfoDB 
    """
    @classmethod
    @mongo_exception
    def __init__(cls, in_conn):
        """
        """
        cls._conn = in_conn
        cls._db = 'CommonInfo'
        cls.tokenInfo = cls._conn[cls._db]['tokenInfo']
        cls.user2shop = cls._conn[cls._db]['user2shop']

    @classmethod
    @mongo_exception
    def get_all_shop_id_list(cls):
        """
        获得所有的店铺id
        """
        all_shop_id_list = []
        for element in cls.user2shop.find({}):
            all_shop_id_list.append(element['sid'])

        return all_shop_id_list

    @classmethod
    @mongo_exception
    def get_shop_info_by_shop_id(cls, in_shop_id):
        """
        get shop_info by shop id, result is a dict
        """
        element = cls.user2shop.find_one({'sid':int(in_shop_id)})
        if not element:
            return {}
        shop_info = {'sid':int(in_shop_id)}
        shop_info['subway_token'] = element['subway_token']
        shop_info['nick'] = element['nick']
        shop_info['uid'] = int(element['_id'])
        
        element = cls.tokenInfo.find_one({'_id':str(shop_info['uid'])})
        if not element:
            shop_info['access_token'] = None
        else:
            shop_info['access_token'] = element['access_token']

        return shop_info

if __name__ == '__main__':

    MONGODB_HOST = 'app.maimiaotech.com'
    MONGODB_PORT = 2006
    from pymongo import Connection
    mongoConn = Connection(host = MONGODB_HOST, port = MONGODB_PORT)
    
    shop_info = ShopInfoDB(mongoConn)
    shop_id_list = shop_info.get_all_shop_id_list()

    if len(shop_id_list) >= 1:
        print shop_info.get_shop_info_by_shop_id(shop_id_list[0])
