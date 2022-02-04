from infomation_filter.memory_filter import MemoryFilter
from infomation_filter.redis_filter import RedisFilter

from infomation_filter.mysql_filter import MySQLFilter

# # 利用set去重
# filter1 = MemoryFilter()
# data = ['111','222','333','222','232']
# for d in data:
#     if filter1.is_exists(d):
#         print('重复',d)
#     else:
#         filter1.save(d)
#         print('不重复', d)


filter2 = RedisFilter()
data = ['111','222','333','222','232']
for d in data:
    if filter2.is_exists(d):
        print('重复',d)
    else:
        filter2.save(d)
        print('不重复', d)

#
# # 利用MySQL去重
# 模板 mysql_url = 'pymysql+mysql://root:password@localhost:3306/db_name?charset=utf8'
# mysql_url = 'mysql+pymysql://root:123456@localhost:3306/test?charset=utf8'
#
# filter3 = MySQLFilter(mysql_url=mysql_url)
#
# data = ['111', '222', '333', '222', '232']
# for d in data:
#     if filter3.is_exists(d):
#         print('重复', d)
#     else:
#         filter3.save(d)
#         print('不重复', d)

