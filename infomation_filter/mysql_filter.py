"""
    利用Mysql进行去重


    在 dome.py 进行测试
    示例：
    if __name__ == '__main__':
    模板 mysql_url = 'pymysql+mysql://root:password@localhost:3306/db_name?charset=utf8'
    mysql_url = 'mysql+pymysql://root:123456@localhost:3306/test?charset=utf8'

    filter3 = MySQLFilter(mysql_url=mysql_url)

    data = ['111', '222', '333', '222', '232']
    for d in data:
        if filter3.is_exists(d):
            print('重复', d)
        else:
            filter3.save(d)
            print('不重复', d)

    不重复 111
    不重复 222
    不重复 333
    重复 222
    不重复 232
"""





from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from . import BaseFilter

Base = declarative_base()


class Filter(Base):
    __tablename__ = 'fitter'

    id = Column(Integer, primary_key=True)
    hash_value = Column(String(40), index=True, unique=True)


class MySQLFilter(BaseFilter):
    '''基于Mysql的去重判断依据的存储'''

    def _get_storage(self):
        """返回的mysql链接对象"""
        engine = create_engine(self.mysql_url)
        Base.metadata.create_all(engine)  # 创建表 如果有就忽略
        Session = sessionmaker(engine)
        return Session

    def _save(self, hash_value):
        """
        利用set进行存储
        :param hash_value:
        :return:
        """
        session = self.storage()
        filter = Filter(hash_value=hash_value)
        session.add(filter)
        session.commit()

    def _is_exists(self, hash_value):
        """

        :param hash_value:
        :return:
        """
        session = self.storage()
        ret = session.query(Filter).filter_by(hash_value=hash_value).first()
        session.close()
        if ret is None:
            return False
        return True
