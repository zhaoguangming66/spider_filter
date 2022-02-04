"""
    布隆过滤器 redis版
    如果出现发限都是重复数据请查看 is_exist 方法 尝试修改

    特别注意----MultipleHash(salts) 在同一项目中不能改值
    改了后  以前的数据就变成新数据添加到相对应的存储

    在 dome.py 进行测试

    会使内存暴涨
    redis bitmap - 用两个 setbit 操作让 bitmap 内存暴涨到 512MB(max)......
    https://www.cnblogs.com/christmad/p/11967882.html

    示例：
    if __name__ == '__main__':
        date = ['1', '12', '23', '12', 'sad', 'sad', 'we']
        bm = BloomFilter(salts=['1', '2', '3', '4'])
        for d in date:
            if bm.is_exist(d):
                bm.save(d)
                print('不重复', d)
            else:
                print('发现重复数据', d)
    结果：
    不重复 1
    不重复 12
    不重复 23
    发现重复数据 12
    不重复 sad
    发现重复数据 sad
    不重复 we

"""
import hashlib

import redis
import six


# 1,多个hash函数实现和求职
# 2，hash表实现和实现对应的映射
class MultipleHash(object):
    """ 提供原始数据，和预定多个salt，生成hash函数值 """

    def __init__(self, salts, hash_func_name='md5'):
        self.hash_func = getattr(hashlib, hash_func_name)
        if len(salts) < 3:
            raise Exception("请提供至少3个salt")
        self.salts = salts

    def get_hash_values(self, data):
        hash_values = []
        for i in self.salts:
            hash_obj = self.hash_func()
            hash_obj.update(self._safe_data(data))
            hash_obj.update(self._safe_data(i))
            ret = hash_obj.hexdigest()
            hash_values.append(int(ret, 16))
        return hash_values

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


class BloomFilter(object):
    """ """

    def __init__(self, salts, redis_host='localhost', redis_port=6379, redis_db=0, redis_key='bloomfilter'):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_key = redis_key
        self.client = self._get_redis_client()
        self.multiple_hash = MultipleHash(salts)

    def _get_redis_client(self):
        """ 返回一个客户端连接"""
        pool = redis.ConnectionPool(host=self.redis_host, port=self.redis_port, db=self.redis_db)
        client = redis.StrictRedis(connection_pool=pool)
        return client

    def save(self, data):
        hash_values = self.multiple_hash.get_hash_values(data)
        for hash_value in hash_values:
            offset = self._get_offset(hash_value)
            self.client.setbit(self.redis_key, offset, 1)
        return True

    def is_exist(self, data):
        """
        这里有争议 原文件 if v == 0返回 False  return Ture
        :param data:
        :return:
        """
        hash_values = self.multiple_hash.get_hash_values(data)
        for hash_value in hash_values:
            offset = self._get_offset(hash_value)
            v = self.client.getbit(self.redis_key, offset)
            if v == 0:
                return True
        return False

    def _get_offset(self, hash_value):
        """
        2**8=256
        2**20 = 1024*1024
        :param hash_value:
        :return:
        """
        return hash_value % (2 ** 8 * 2 ** 20 * 2 * 3)
