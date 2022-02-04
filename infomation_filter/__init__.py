"""
# 基于信息摘要算法进行数据的去重和去重判断和储存
# 1.基于内存的储存
# 2.基于redis的储存
# 3.基于mysql的储存
# 4.实现redis版的布隆去重
"""

import hashlib
import six


class BaseFilter(object):
    """基于信息摘要算法进行数据的去重和去重判断和储存"""

    def __init__(self,
                 hash_func_name='md5',
                 redis_host="localhost",
                 redis_port=6379,
                 redis_db=0,
                 redis_key='filter',
                 mysql_url=None,
                 my_table_name='filter'):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_key = redis_key
        self.mysql_url = mysql_url
        self.my_table_name = my_table_name
        self.hash_func = getattr(hashlib, hash_func_name)
        self.storage = self._get_storage()

    def _safe_data(self, data):
        """

        :param data: 给定的原始的数值
        :return:二进制数据的字符串数据
        """
        if six.PY3:
            if isinstance(data, bytes):
                return data
            elif isinstance(data, str):
                return data.encode()
            else:
                raise Exception('Please provide a string')  # 请提供一个字符串
        else:
            if isinstance(data, str):
                return data
            # elif isinstance(data, unicode):
            #     return data.encode()
            else:
                raise Exception('Please provide a string')  # 请提供一个字符串

    def _get_hash_value(self, data):
        """
        根据给定的数据，返回的对应的信息摘要的hasd值
        :param data:给定的原始的数值（二进制数据的字符串数据）
        :return:hash值
        """
        hash_obj = self.hash_func()
        hash_obj.update(self._safe_data(data))
        hash_value = hash_obj.hexdigest()
        return hash_value

    def save(self, data):
        """
        根据data计算出对应的指纹进行存储
        :param data:给定的原始的数据（二进制数据的字符串数据）
        :return:存储的结果
        """
        hash_value = self._get_hash_value(data)
        self._save(hash_value)

    def _save(self, hash_value0):
        """
        存储对应的hash值交给对应的子类去继承
        :param hash_value0:通过信息摘要算求出的hash值
        :return:存储的结果
        """
        pass

    def is_exists(self, data):
        """
        判断给定的数据对应的指纹是否存在
        :param data:给定的原始数据（二进制数据的字符串数据）
        :return:True or Flase
        """
        hash_value = self._get_hash_value(data)
        return self._is_exists(hash_value)

    def _is_exists(self, hash_value):
        """
        判断给定的数据对应的指纹是否存在交给对应的子类去继承
        :param hash_value:通过信息摘要算求出的hash值
        :return:判断的结果 (true of flase)
        """
        pass

    def _get_storage(self):
        """

        :return:
        """
