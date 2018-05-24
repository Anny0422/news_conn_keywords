# news_conn_keywords
匹配个股：
# 1、从数据库表中读取想要的内容：
sql_saved = "select id, content from news_conn_risk"
# 2、批量处理，得到一个列表，元素为字典，每个字典里存储id和content
get_json(self, saved_files)
# 3、将需要用到的关键字分别读取出来
get_keywords(self):
# 4、匹配个股
    def connect_share(self, contents, del_one, del_two, error_keyword_dict, right_keyword_dict):
        double_id = []
        texts = []       #把匹配上个股的新闻放在列表内
        count = 0
        for i in range(len(contents)):
        #for i in range(10):
            count += 1
            if count%10000 ==0:
                print('已处理数据数量：', count)
                
# 5、更新数据库
    def run(self, texts):

        db = Database()
        db.connect('news_connect_keywords')
        count = 0
        for i in range(len(texts)):
            data = texts[i]
            print(data)
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
 # 6、执行完后，发送邮件
 if __name__ == '__main__':
    CS = ConnShare()
    db = Database()
    db.connect('news_connect_keywords')
    saved_files = CS.conn_db()
    contents = CS.get_json(saved_files)
    right_keyword_dict, error_keyword_dict, del_one, del_two = CS.get_keywords()
    print('接下来去匹配个股')
    texts, len_double = CS.connect_share(contents, del_one, del_two, error_keyword_dict, right_keyword_dict)
    CS.run(texts)
    ret = myEmail.mail('若运行完成')
    if ret:
        print("邮件发送成功")
    else:
        print("邮件发送失败")
