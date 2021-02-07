#!/home/liangzhi/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-07-23 13:39
# Filename     : news_partition.py
# Description  : 资讯原始数据清洗与分区
#******************************************************************************
import sys
import json
import logging
import pymongo
import re
import time
from pymongo import MongoClient
from dateutil import parser
import datetime
from bson.objectid import ObjectId

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)





class NewsPartition(object):

    ## 时间格式规整处理
    def datetime_formatter(self, datetime_str):
        if type(datetime_str) == datetime.datetime: # some like datetime.datetime type
            datetime_str = str(datetime_str)[ : 19]
        tmp = datetime_str.split(" ")
        d = ""
        if len(tmp) == 1:
            tmp1 = re.split("[-/]", tmp[0]) 
            if len(tmp1) == 3: # like 2020-03-25 or 2020/03/27
                d = datetime.datetime.strptime(tmp[0], "%Y-%m-%d")
            elif len(tmp1) == 4: # like 2020-03-25-10:3
                d = datetime.datetime.strptime('-'.join(tmp1[ : 3]) + ' ' + tmp1[3], "%Y-%m-%d %H:%M")
            else: # for other format
                d = datetime_str
        elif len(tmp) == 2:
            if len(re.split("[-/]", tmp[0])) == 3: # like 2020/03/26 16:13 or 2020-03-26 16:13
                d1 = tmp[0].replace("/", "-")
            else: # for other format
                d1 = tmp[0]
            if len(tmp[1].split(':')) == 2: # like 2020-03-25 10:3
                d = datetime.datetime.strptime(d1 + ' ' + tmp[1], "%Y-%m-%d %H:%M")
            elif len(tmp[1].split(':')) == 3: # like 2020-03-25 10:3:1
                d = datetime.datetime.strptime(d1 + ' ' + tmp[1], "%Y-%m-%d %H:%M:%S")
        else:
            d = datetime_str
        return d.strftime("%Y-%m-%d %H:%M:%S")


    ## 资讯分库分表主函数
    def process(self, date_str):
        source_count = 0            ## 采集库资讯数量
        target_count = 0            ## 清洗库资讯数量
        target_collection_name = ""      ## 目标清洗库

        process_date = None
        next_date = None

        start_time = time.time()
        ## 默认情况下处理昨天采集的数据
        if date_str == "yesterday":
            date_str = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        logger.info("执行采集日期为: {} 的资讯的清洗与分库".format(date_str))
        process_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        next_date = process_date + datetime.timedelta(days=1)

        connector = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
        admin_db = connector["admin"]
        admin_db.authenticate(MONGO_USER, MONGO_PASSWD)
        source_db = connector[SOURCE_DB]
        source_collection = source_db[SOURCE_COLLECTION]
        target_db = connector[TARGET_DB]
        ## 查找某一天区间的资讯, 目前只处理微信公众号
        docs = source_collection.find({"source": "公众号", "crawl_time": {"$gte": process_date, "$lte": next_date}})

        for doc in docs:
            source_count += 1
            if ("content" not in doc) or (not doc["content"]):
                logger.info("跳过资讯[{}], 原因: content内容为空".format(str(doc["_id"])))
                continue
            if not "publish_time" in doc:
                logger.info("跳过资讯[{}], 原因: publish_time字段不存在".format(str(doc["_id"])))
                continue
            
            try:
                publish_time = self.datetime_formatter(doc["publish_time"])
            except Exception as e:
                logger.error("日期格式化出错, 资讯ID {}".format(str(doc["_id"])))
                continue

            doc.update({"publish_time": publish_time})
            doc.update({"html": ""})                    ## html字段不流转

            ## 根据新闻的爬取时间，将新闻分到对应的表中
            target_collection_name = TARGET_PREFIX + datetime.datetime.strftime(doc["crawl_time"], "%Y%m")
            target_collection = target_db[target_collection_name]

            if target_collection_name not in target_db.collection_names():
                target_collection.create_index([('crawl_time', pymongo.ASCENDING)])

            ## 考虑同id覆盖
            if target_collection.find_one_and_delete({"_id": ObjectId(doc["_id"])}):
                logger.info("覆盖原数据: {}".format(str(doc["_id"])))
            target_collection.insert_one(doc)
            target_count += 1

        end_time = time.time()
        logger.info("完成采集日期为: {} 的资讯清洗分库, 其中采集库有 {} 条, 导入 [{}] 清洗库 {} 条, 耗时: {}秒"
                        .format(str(process_date), 
                                source_count, 
                                target_collection_name, 
                                target_count,
                                int(end_time - start_time)))


if __name__ == '__main__':
    news_partition = NewsPartition()

    if len(sys.argv) > 1:
        news_partition.process(sys.argv[1])
    else:
        raise Exception("请输入执行日期参数")
    