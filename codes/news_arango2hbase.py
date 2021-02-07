#!/home/liangzhi/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-06-24 09:56
# Filename     : news_arango2hbase.py
# Description  : 同步脚本, 将news从arangodb同步到hbase
#******************************************************************************

import os
import sys
import re
import logging 
import requests
import json
import time
import datetime
from dateutil import parser
import pymysql
import happybase
from pyArango.connection import Connection as ArangoConnection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

## 资讯概念体系mysql数据库

## 导入目标arangodb数据库


##导入Hbase


class NewsArango2hbase(object):

    def __init__(self):
        self.news_concept = {}          ## 资讯的概念体系对应表
        self.get_news_concept()
        self.arango_count = 0           ## arangodb需要同步的资讯数量
        self.hbase_count = 0            ## 导入hbase的资讯数量


    def get_news_concept(self):
        mysql_connector = pymysql.connect(host=MYSQL_HOST,
                                          port=MYSQL_PORT,
                                          user=MYSQL_USER,
                                          passwd=MYSQL_PASSWD,
                                          db=MYSQL_DB,
                                          charset="utf8")
        mysql_cursor = mysql_connector.cursor()
        query = "select * from {} where type=\"news\"".format(MYSQL_TABLE)
        
        count = mysql_cursor.execute(query)
        if count > 0:
            results = mysql_cursor.fetchall()
            for result in results:
                self.news_concept[result[0]] = result[5]
            logger.info("资讯概念关系表加载完成!")
        else:
            logger.error("资讯概念关系表加载失败!")

    
    ## 同步主函数
    def process(self, date_str):
        process_date = None
        next_date = None

        ## 默认处理昨天采集的数据
        if date_str == "yesterday":
            date_str = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        process_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        next_date = process_date + datetime.timedelta(days=1)
        process_date = datetime.datetime.strftime(process_date, "%Y-%m-%d")
        next_date = datetime.datetime.strftime(next_date, "%Y-%m-%d")

        logger.info("执行采集日期为: {} 的资讯同步任务, 从arangodb导入hbase".format(process_date))

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

        for i in range(0, self.arango_count, BATCH_SIZE):
            start = i
            end = min(self.arango_count, start + BATCH_SIZE)
            batch_start_time = time.time()

            ## hbase连接
            hbase_connector = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT)
            hbase_table = hbase_connector.table(HBASE_TABLE)
            hbase_batch = hbase_table.batch()

            for result in results[start : end]:
                ## 判断资讯的数据源是否写入mysql的概念表
                source = result["source"]
                if source not in self.news_concept:
                    logger.error("未找到该数据源 [{}] 的概念关联, 请在mysql中添加!".format(source))
                    continue

                concept_id = self.news_concept[source]
                rowkey = concept_id + "|" + result["create_time"][:10] + "|" + result["_key"]
                rowkey = bytes(rowkey, encoding="utf-8")

                column_family = {
                    b"raw:html":            bytes(result["html"], encoding="utf8"),
                    b"info:concept_id":     bytes(concept_id, encoding="utf8"),
                    b"info:doc_id":         bytes(result["_key"], encoding="utf8"),
                    b"info:title":          bytes(result["title"], encoding="utf8"),
                    b"info:content":        bytes(result["content"], encoding="utf8"),
                    b"info:url":            bytes(result["url"], encoding="utf8"),
                    b"info:abstract":       bytes(result["abstract"], encoding="utf-8"),
                    b"info:source":         bytes(result["source"], encoding="utf8"),
                    b"info:img_url":        bytes(json.dumps(result["img_url"], ensure_ascii=False), encoding="utf-8"),
                    b"info:tags":           bytes(json.dumps(result["tags"], ensure_ascii=False), encoding="utf8"),
                    b"info:entities":       bytes(json.dumps(result["entities"], ensure_ascii=False), encoding="utf8"),
                    b"info:publish_time":   bytes(result["publish_time"], encoding="utf8"),
                    b"info:insert_time":    bytes(datetime.date.today().strftime("%Y-%m-%d %H:%M:%S"), encoding="utf8")
                }

                hbase_batch.put(rowkey, column_family)
            
            hbase_batch.send()
            self.hbase_count += (end - start)
            hbase_connector.close()

            batch_end_time = time.time()
            logger.info("第 {} - {} 条数据处理结束, 共耗时: {} 秒".format(start, end, int(batch_end_time - batch_start_time)))

        end_time = time.time()
        logger.info("本次资讯由arangodb同步到hbase处理完成，共耗时: {} 秒".format(int(end_time - start_time)))
        logger.info("其中共需要同步资讯数据: {} 条, 导入hbase: {} 条".format(self.arango_count, self.hbase_count))


if __name__ == "__main__":
    news_arango2hbase = NewsArango2hbase()

    if len(sys.argv) > 1:
        news_arango2hbase.process(sys.argv[1])
    else:
        raise Exception("请输入执行日期参数")