"""
    基于redis的持久化存储和去重的判断实现

    在test.py进行测试
    示例：
    filter2 = RedisFilter()
    data = ['111','222','333','222','232']
    for d in data:
        if filter2.is_exists(d):
            print('重复',d)
        else:
            filter2.save(d)
            print('不重复', d)
    结果：
    不重复 111
    不重复 222
    不重复 333
    重复 222
    不重复 232
"""




from . import BaseFilter
import redis


class RedisFilter(BaseFilter):
    # 基于redis的持久化存储和去重的判断实现

    def _get_storage(self):
        """ 返回一个客户端连接"""
        pool = redis.ConnectionPool(host=self.redis_host, port=self.redis_port, db=self.redis_db)
        client = redis.StrictRedis(connection_pool=pool)
        return client

    def _save(self, hash_value):
        """
        利用redis无序集合进行存储
        :param hash_value:
        :return:
        """
        return self.storage.sadd(self.redis_key, hash_value)

    def _is_exists(self, hash_value):
        """
        判断无序集合是否有对应的数据
        :param hash_value:
        :return:
        """
        return self.storage.sismember(self.redis_key, hash_value)
