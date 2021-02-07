#!/home/liangzhi/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-07-22 17:30
# Filename     : news_arango2mysql.py
# Description  : 同步脚本, 将news从arangodb同步到mysql
#******************************************************************************

import sys
import logging
import datetime
import time
import json
import pymysql
from tqdm import tqdm
from pyArango.connection import Connection as ArangoConnection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

## 导入目标arangodb数据库


##mysql数据库


class NewsArango2mysql(object):

    def __init__(self):
        self.arango_count = 0         ## arango找到的资讯数目
        self.mysql_count = {
            "人工智能": 0,
            "地理信息": 0,
            "生物医药": 0,
            "光电产业": 0,
            "新能源汽车": 0,
            "5G产业": 0
        }

    def insert(self, doc, industry):

        db = None
        if industry == "人工智能":
            db = AI_DB
        elif industry == "地理信息":
            db = GEO_DB
        elif industry in ["生物制药", "医疗器械"]:
            industry = "生物医药"
            db = MED_DB
        elif industry == "光电产业":
            db = OP_DB
        elif industry == "新能源汽车":
            db = NECAR_DB
        elif industry == "5G产业":
            db = _5G_DB
        else:
            logger.info("尚不支持该行业数据插入: {}".format(industry))
            return

        sql_connector = pymysql.connect(host=MYSQL_HOST,
                                        port=MYSQL_PORT,
                                        user=MYSQL_USER,
                                        passwd=MYSQL_PASSWD,
                                        db=db,
                                        charset="utf8mb4")

        with sql_connector.cursor() as cursor:
            ## 同id覆盖
            query = "DELETE FROM event WHERE id=\"{}\"".format(doc["id"])
            cursor.execute(query)
            
            self.mysql_count[industry] += 1
            insert_query = "INSERT INTO `event` (`id`, `name`, `time`, `tags`, `content`) VALUES (%s, %s, %s, %s, %s)"
            cursor.execute(insert_query , (doc["id"], doc["name"], doc["time"], doc['event_type'], doc["content"]))
        
        sql_connector.commit()
        sql_connector.close()


    def process(self, date_str):
        process_date = None

        if date_str == "yesterday":
            date_str = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        logger.info("将采集时间为: {} 的资讯从arangodb导入mysql".format(date_str))
        process_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        next_date = process_date + datetime.timedelta(days=1)
        process_date = datetime.datetime.strftime(process_date, "%Y-%m-%d")
        next_date = datetime.datetime.strftime(next_date, "%Y-%m-%d")

        start_time = time.time()
        ## 从arangodb获取需要同步的资讯
        arango_connector = ArangoConnection(arangoURL=ARANGO_URL,
                                            username=ARANGO_USER,
                                            password=ARANGO_PASSWD)

        arango_db = arango_connector[ARANGO_DB]
        aql = "for x in {} FILTER x.update_time >= \"{}\" AND x.update_time <= \"{}\" RETURN x".format(ARANGO_COLLECTION, process_date, next_date)
        results = []
        try:
            results = arango_db.fetch_list(aql)
        except Exception as e:
            logger.error("查询arangodb错误: " + str(e))
        self.arango_count = len(results)

        for result in tqdm(results):
            doc = {}
            doc["id"] = result["_key"]
            doc["name"] = result["title"]
            doc["time"] = result["publish_time"].split(" ")[0]
            doc["url"] = result["url"]
            doc["content"] = result["abstract"]
            
            industrys = []       ## 资讯的行业分类, 存在某个企业对应多个行业的情况
            tags = result["tags"]
            for tag in tags:
                if tag["conceptName"] == "产业":
                    industrys.append(tag["name"])
                if tag["conceptName"] == "事件":
                    doc["event_type"] = tag["name"]

            ## 没有事件就不导入
            if "event_type" not in doc:
                continue
            
            ## 根据企业的行业分类，分别插入到不同的数据库中，一篇资讯可以插入多个表
            industrys = list(set(industrys))
            for industry in industrys:
                try:
                    self.insert(doc, industry)
                except Exception as e:
                    logger.error(str(e))
                    logger.info("插入错误, 资讯id: {}".format(doc["id"]))

        end_time = time.time()
        logger.info('''本次同步日期: {}, 从arango导入mysql, 
                        arango找到资讯[{}]篇, 
                        人工智能[{}]篇,
                        光电产业[{}]篇, 
                        新能源汽车[{}]篇, 
                        生物医药[{}]篇,
                        地理信息[{}]篇,
                        5G产业[{}]篇,
                        同步耗时: {} 秒 '''.format(date_str,
                                                 self.arango_count, 
                                                 self.mysql_count["人工智能"], 
                                                 self.mysql_count["光电产业"],
                                                 self.mysql_count["新能源汽车"],
                                                 self.mysql_count["生物医药"],
                                                 self.mysql_count["地理信息"],
                                                 self.mysql_count["5G产业"],
                                                 int(end_time - start_time)))

if __name__ == "__main__":
    news_arango2mysql = NewsArango2mysql()

    if len(sys.argv) > 1:
        news_arango2mysql.process(sys.argv[1])
    else:
        raise Exception("请输入执行日期参数")