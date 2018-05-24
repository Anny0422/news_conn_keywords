import csv
import pandas as pd
from database import  Database
import myEmail
#from connect_keywords.database import Database

class ConnShare(object):
    def __init__(self):
        print()

    #csv_file = csv.reader(open('finace_news_content.csv', 'r'))

    def get_keywords(self):
        csv_rightkeyword = csv.reader(open('Keyword.csv', 'r'))
        csv_errorkeyword = csv.reader(open('erroKeyword.csv', 'r'))
        csv_del_one = csv.reader(open('select_one.csv', 'r'))
        csv_del_double = csv.reader(open('select_double.csv', 'r'))
        #websites = ['新华网', '东方财富网']  #603888  300059

        right_keyword_dict = {}
        for row in csv_rightkeyword:
            right_keyword_dict[row[1]] = row[2:]
        print('关键字无异议的个股数量是：', len(right_keyword_dict))

        #有歧义的关键字
        error_keyword_dict = {}
        for row in csv_errorkeyword:
            error_keyword_dict[row[1]] = row[2:]
        print('关键字有异议的个股数量是：', len(error_keyword_dict))

        del_one = []
        for row in csv_del_one:
            value = str(row[0]).split('%')
            del_one.append(value[1])
        print('删除含有单个关键字的数量：', len(del_one))

        del_two = []
        for row in csv_del_double:
            values = str(row[0]).split('%')[1]
            value = values.split(',')
            del_two.append(value)
        print('删除同时含有这两个关键字的数量：', len(del_two))
        return right_keyword_dict, error_keyword_dict, del_one, del_two

    #连接数据库,取出用来对照的文本，sql_saved是一个元组；取出刚抓取的内容，放在元组sql_craw内；
    def conn_db(self):
        db = Database()
        db.connect('news_connect_keywords')

        #sql_saved = "select OldID, Content from tFastNew_wallstreet_stock_negative"
        #sql_saved = "select SelfID, content from tRiskNews_FinChina"
        sql_saved = "select id, content from news_conn_risk"
        saved_files = db.query(sql_saved)

        db.close()
        return saved_files #元组，元素也为元组（包含OldID，Title）。

    def get_json(self, saved_files):
        contents = []
        for i in range(len(saved_files)):
            content = {}
            content['id'] = saved_files[i][0]
            content['content'] = saved_files[i][1]
            contents.append(content)
        print('新闻数量：', len(contents))
        return contents
    #
    # def get_contents(self, csv_file): #contents是个列表，元素为字典，记录每个新闻的几个特征：原数据库ID,content,counts
    #     contents = []
    #     for row in csv_file:
    #         content = {}
    #         content['id'] = row[0]
    #         content['content'] = row[2].strip('\t\n')
    #         content['counts'] = row[1]
    #         contents.append(content)
    #     print('新闻数量：', len(contents))
    #     return contents


    #针对一批新闻
    def connect_share(self, contents, del_one, del_two, error_keyword_dict, right_keyword_dict):
        double_id = []
        texts = []       #把匹配上个股的新闻放在列表内
        count = 0
        for i in range(len(contents)):
        #for i in range(10):
            count += 1
            if count%10000 ==0:
                print('已处理数据数量：', count)
            flag = True
            text = contents[i]   ###取出第i个新闻；是一个字典，存储的有id，content, counts
            content = text['content']
            for m in range(len(del_one)):
                word = del_one[m]
                if word in content:
                    flag = False
                    break
            if flag:
                for n in range(len(del_two)):
                    words = del_two[n]
                    word1 = words[0]
                    word2 = words[1]
                    if word1 in content and word2 in content:
                        flag = False
                        break
                if flag:

                    #取出有异议的个股及对应的关键字
                    for key, value in error_keyword_dict.items():
                        #错误的词
                        errorvalue = str(value[1]).split(',')  # ['（新华网）', '新华网：', '据新华网']

                        #正确的关键字
                        key_value = str(value[0]).split(',')    #['600295', '鄂尔多斯']

                        if key_value[1] in content:  # 包含有歧义的关键字时：若同时包含错误关键字，则不继续；若不包含错误关键字，则。。。
                            for i in range(len(errorvalue)):
                                if errorvalue[i] in content:
                                    flag = False
                            if flag:
                                for item in texts:
                                    if item['id'] == text['id'] and text['id'] != None:
                                        item['stoc_id'].extend([key])
                                        #print(item['id'] + keyword + key )
                                        double_id.append(item['id'])
                                        flag = False
                                if flag:
                                    jre = {}
                                    jre['id'] = text['id']
                                    jre['stoc_id'] = [key]
                                    jre['content'] = content
                                    texts.append(jre)
                                    # print(keyword + key)
                                    break

                        if key_value[0] in content:  # 若含有个股编码关键字时，没有奇异，继续
                            for item in texts:
                                if item['id'] == text['id'] and text['id'] != None:
                                    item['stoc_id'].extend([key])
                                    # print(item['id'] + keyword + key )
                                    double_id.append(item['id'])
                                    flag = False
                            if flag:
                                jre = {}
                                jre['id'] = text['id']
                                jre['stoc_id'] = [key]
                                jre['content'] = content
                                texts.append(jre)
                                # print(keyword + key)
                                break

                    #取出没有异议的个股及对应的关键字
                    for key, value in right_keyword_dict.items():
                        value = str(value[0]).split(',')
                        #print(value)    #['603773', '沃格光电']
                        #遍历指定个股的关键字
                        for j in range(len(value)):
                            keyword = value[j]   #关键字
                            #判断关键字是否在文本中
                            if keyword in content:
                                for item in texts:  ##若此新闻之前已匹配上其他个股，则将stoc_id添加一个元素
                                    if item['id'] == text['id'] and text['id'] != None:
                                        item['stoc_id'].extend([key])
                                        #print(item['id'] + keyword + key )
                                        double_id.append(item['id'])
                                        flag = False
                                if flag:
                                    jre = {}
                                    jre['id'] = text['id']
                                    jre['stoc_id'] = [key]
                                    jre['content'] = content
                                    texts.append(jre)
                                    break
        print(len(double_id))
        return texts, len(double_id)

    def run(self, texts):

        db = Database()
        db.connect('news_connect_keywords')
        count = 0
        for i in range(len(texts)):
            data = texts[i]
            print(data)
            # insert = 'INSERT IGNORE INTO news_conn_risk(news_id, content, stock_id, counts) VALUES (%s, %s, %s, %s)'
            # db.execute(insert, [data['id'], data['content'], str(data['stoc_id']), str(data['counts'])])
            sql_update = """update news_conn_risk set stock_id = "%s" where id = "%s" """ % (str(data['stoc_id']), str(data['id']))
            count += 1
            try:
                # sql_update = """update announce_conn_risk set risk = "%s" where id = '1204822535'""" % (str(data['risk']))
                db.execute(sql_update)
            except:
                print('有误:', data['id'], data['stoc_id'])
                continue
            if count % 10 == 0:
                print('已更新数量：', count)
        db.close()

    # def run(self, texts):
    #
    #     db = Database()
    #     db.connect('news_connect_keywords')
    #
    #     for i in range(len(texts)):
    #         data = texts[i]
    #         # insert = 'INSERT IGNORE INTO news_conn_risk(news_id, content, stock_id, counts) VALUES (%s, %s, %s, %s)'
    #         #db.execute(insert, [data['id'], data['content'], str(data['stoc_id']), str(data['counts'])])
    #         sql_update = """update news_conn_risk set stock_id = "%s" where id = "%s" """ %(str(data['stoc_id']), str(data['id']))
    #         #sql_update = """update announce_conn_risk set risk = "%s" where id = '1204822535'""" % (str(data['risk']))
    #         db.execute(sql_update)
    #     db.close()

if __name__ == '__main__':
    CS = ConnShare()
    db = Database()
    db.connect('news_connect_keywords')
    saved_files = CS.conn_db()
    contents = CS.get_json(saved_files)
    right_keyword_dict, error_keyword_dict, del_one, del_two = CS.get_keywords()
    print('接下来去匹配个股')
    texts, len_double = CS.connect_share(contents, del_one, del_two, error_keyword_dict, right_keyword_dict)
    #CS.run(texts)
    # ret = myEmail.mail('若运行完成')
    # if ret:
    #     print("邮件发送成功")
    # else:
    #     print("邮件发送失败")