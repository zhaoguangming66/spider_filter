"""
    基于python中的集合进行去重
    利用set去重

    在 dome.py 进行测试
    示例：
    if __name__ == '__main__':
        filter1 = MemoryFilter()
        data = ['111', '222', '333', '222', '232']
        for d in data:
            if filter1.is_exists(d):
                print('重复', d)
            else:
                filter1.save(d)
                print('不重复', d)
    结果：
    不重复 111
    不重复 222
    不重复 333
    重复 222
    不重复 232


"""

from . import BaseFilter


class MemoryFilter(BaseFilter):
    """基于python中的集合进行去重"""

    def _get_storage(self):
        return set()

    def _save(self, hash_value):
        """
        利用set进行存储
        :param hash_value:
        :return:
        """
        return self.storage.add(hash_value)

    def _is_exists(self, hash_value):
        """

        :param hash_value:
        :return:
        """
        if hash_value in self.storage:
            return True
        return False
