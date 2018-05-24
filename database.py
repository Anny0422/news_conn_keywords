import os
#import ConfigParser

import configparser
import pymysql.cursors
class Database(object):
    def __init__(self):
        self.conn = None
    def getconf(self, db):
        # 当前文件的绝对路径
        path = os.path.dirname(__file__)
        # 数据库的配置文件
        name = 'database.cfg'
        # 组合成cfg的绝对路径
        name = os.path.join(path, name)
        config = configparser.ConfigParser()
        config.read(name)
        # 数据库类型mysql
        dbms = config.get(db, 'dbms')
        # 主机名或ip地址
        host = config.get(db, 'host')
        # 用户名
        user = config.get(db, 'user')
        # 密码
        password = config.get(db, 'password')
        # 数据库名
        database = config.get(db, 'database')
        # 端口和编码
        if config.has_option(db, 'port'):
            port = config.get(db, 'port')
        else:
            port = ''
        if config.has_option(db, 'charset'):
            charset = config.get(db, 'charset')
        else:
            charset = 'utf8'
        # 返回一个字典
        return {'dbms':dbms,'host':host,'user':user,'password':password,'database':database,
                'charset':charset,'port': port}
    def connect(self, db):
        self.conn = None
        conf = self.getconf(db)
        dbms = conf['dbms']
        host = conf['host']
        user = conf['user']
        password = conf['password']
        database = conf['database']
        charset = conf['charset']
        port = conf['port']
        # mysql数据库
        if dbms == 'mysql':
            port = int(port) if port else 3306
            try:
                pymysql = __import__('pymysql')
                # 获取数据库连接
                conn = pymysql.connect(host=host,user=user,passwd=password,db=database,port=port,
                                        use_unicode=True,charset='utf8')
                self.conn = conn
                print(self.conn)
                self.execute('set names' + charset)
            except Exception as e:
                print(e)
                return None
        elif dbms == 'mssql':
            try:
                pymssql = __import__('pymssql')
                conn = pymssql.connect(host=host,user=user,password=password,database=database,charset=charset,
                                       as_dict=True)
                self.conn = conn
            except Exception as e:
                print(e)
                return None
        else:
            print("not support!")
            return None
    def close(self):
        if self.conn != None:
            self.conn.close()
    def execute(self, sql, data= None):
        cur = self.conn.cursor()
        cur.execute(sql, data)
        id = self.conn.insert_id()
        self.conn.commit()
        return id
    def executemany(self, sql, data):
        cur = self.conn.cursor()
        cur.executemany(sql, data)
        cur.close()
        self.conn.commit()
    # 数据库查询
    def query(self, sql, data=None):
        cur = self.conn.cursor()
        if data:
            cur.execute(sql, data)
        else:
            cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return rows


if __name__ == '__main__':
    print()