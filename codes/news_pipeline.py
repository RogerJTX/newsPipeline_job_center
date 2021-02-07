#!/home/liangzhi/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-06-23 16:55
# Filename     : news_pipeline.py
# Description  : 资讯标注；从MongoDB读取数据调用news pipeline处理，并存入ArangoDB
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
from pymongo import MongoClient
from pyArango.connection import Connection as ArangoConnection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

## 资讯mongodb数据库


## 导入目标arangodb数据库


## 调用news pipeline地址以及批处理大小
BATCH_SIZE = 20

## 相似内容判断阈值
THRESHOLD = 0.8

class NewsPipeline(object):

    def __init__(self):
        self.news_count = 0         #资讯采集总数
        self.pipeline_count = 0     #经过nlp pipeline的资讯数
        self.arango_count = 0       #存入arango的资讯数
        self.duplicate_count = 0    #重复的资讯数
        self.ignore_count = 0       #过滤的无关资讯


    ## 两篇文章相似度计算
    def similarity(self, doc_1, doc_2):
        ## 发布时间判断
        date_1 = datetime.datetime.strptime(doc_1["publish_time"], "%Y-%m-%d %H:%M:%S")
        date_2 = datetime.datetime.strptime(doc_2["publish_time"], "%Y-%m-%d %H:%M:%S")
        date_diff = date_2 - date_1
        ## 发布时间差一个月，基本认定不重复(即时重复在news pipeline中也有去重组件)
        if date_diff.days > 10:
            return 0
        
        set_1 = set(doc_1["title"])
        set_2 = set(doc_2["title"])

        score = len((set_1 & set_2)) / (min(len(set_1), len(set_2)))
        return score


    ## 资讯处理主函数
    def process(self, date_str):
        
        process_date = None
        next_date = None

        ## 默认处理昨天采集的数据
        if date_str == "yesterday":
            date_str = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        logger.info("执行采集时间为: {} 的资讯标注news pipeline".format(date_str))
        process_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        next_date = process_date + datetime.timedelta(days=1)
  
        start_time = time.time()

        ## 从mongodb获取需要处理的资讯
        collection_name = MONGO_NEWS_COLLECTION_PREFIX + "".join(date_str.split("-")[ : 2])
        mongo_client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
        admin_db = mongo_client["admin"]
        admin_db.authenticate(MONGO_USER, MONGO_PASSWD)
        news_collection = mongo_client[MONGO_NEWS_DB][collection_name]

        ## 只处理公众号资讯
        docs = news_collection.find({"source": "公众号", "crawl_time": {"$gte": process_date, "$lte": next_date}})
        self.news_count = docs.count()
        logger.info("采集时间为: {}, 从表: {} 共找到资讯 {} 条".format(date_str, collection_name, self.news_count))

        ##组装naf结构
        naf_list = []
        for doc in docs:
            naf = {}
            metadata = {}
            metadata["doc_id"] = str(doc["_id"])
            metadata["source"] = doc["source"]
            metadata["search_key"] = doc["search_key"]                 ## 用于来源于公众号的资讯带企业名称
            metadata["url"] = doc["url"]
            metadata["title"] = doc["title"]
            metadata["publish_time"] = doc["publish_time"]
            metadata["crawl_time"] = doc["crawl_time"].strftime("%Y-%m-%d %H:%M:%S")
            metadata["content"] = doc["content"]
            metadata["html"] = doc["html"]
            metadata["img_url"] = doc["img_url"]
            naf["metadata"] = metadata
            naf_list.append(naf)

        logger.info("资讯组装成naf结构, 调用news pipeline接口服务")

        ## news pipeline服务处理
        index = 0
        while index < len(naf_list):
            batch_start_time = time.time()

            ## 每次取batch大小的数据
            end = len(naf_list) if (index + BATCH_SIZE > len(naf_list)) else index + BATCH_SIZE
            naf_batch = naf_list[index : end]

            post_data = {
                "documents": naf_batch,
                "config": {
                    "name": "资讯标注PIPELINE",
                    "components": [
                        "title_filter",                 ## 行业资讯标题过滤
                        "deduplicate",                  ## 去重
                        "company_link",                 ## 企业名链接
                        "topic_classify",               ## 资讯主题分类
                        "abstract_extract",             ## 资讯摘要抽取
                        "sentiment_classify"            ## 情感分析
                    ]                       
                }
            }

            response = requests.post(NEWS_PIPELINE_URL, json.dumps(post_data))
            if response.status_code == 200:
                logger.info("news pipeline返回结果")
                process_naf_list = response.json().get("body")
                self.pipeline_count += len(process_naf_list)

                ##插入arangodb
                arango_connector = ArangoConnection(arangoURL=ARANGO_URL,
                                                    username=ARANGO_USER,
                                                    password=ARANGO_PASSWD)

                arango_db = arango_connector[ARANGO_DB]
                arango_collection = arango_db[ARANGO_COLLECTION]

                for i, process_naf in enumerate(process_naf_list):
                    naf = process_naf.get("naf")
                    messages = process_naf.get("messages")              ## 返回的messages为数组，带有每个组件的处理标志

                    if not naf:
                        message = " ".join([message for message in messages if message])
                        logger.info("资讯被过滤, doc_id=[{}], title=[{}], 原因=[{}]"
                                    .format(naf_batch[i]["metadata"]["doc_id"], naf_batch[i]["metadata"]["title"], message))
                        if "标题含有过滤词" in message:
                            self.ignore_count += 1
                        elif "找到相同资讯" in message:
                            self.duplicate_count += 1

                    else:
                        ## 同id覆盖问题
                        _id = naf["metadata"]["doc_id"]
                        try:
                            doc = arango_collection[_id]
                            doc.delete()
                            logger.info("资讯覆盖, id: {}".format(_id))
                        except:
                            pass

                        ## naf 结构转化为arangodb存储的数据格式
                        doc = {}
                        doc["_key"] = naf["metadata"]["doc_id"]
                        doc["name"] = naf["metadata"]["title"]
                        doc["create_time"] = naf["metadata"]["crawl_time"]          ## 采集时间变为create_time
                        doc["update_time"] = doc["create_time"]
                        
                        ## 基本属性
                        # doc["doc_id"] = naf["metadata"]["doc_id"]
                        doc["title"] = naf["metadata"]["title"]
                        doc["content"] = naf["metadata"]["content"]
                        doc["abstract"] = naf["metadata"]["abstract"]
                        doc["url"] = naf["metadata"]["url"]
                        doc["html"] = naf["metadata"]["html"]
                        doc["img_url"] = naf["metadata"]["img_url"]
                        doc["publish_time"] = naf["metadata"]["publish_time"]
                        doc["source"] = naf["metadata"]["source"]

                        ## 标签
                        if "tags" not in naf or len(naf["tags"]) == 0:
                            logger.info("该资讯无标签: {}".format(doc["_key"]))
                            continue
                        tags = naf["tags"]
                        doc["tags"] = tags
                        
                        ## 实体
                        if "entities" not in naf or len(naf["entities"]) == 0:
                            logger.info("该资讯中无关联企业: {}".format(doc["_key"]))
                            continue
                        entities = naf.get("entities")
                        doc["entities"] = entities

                        ## 去重以及插入
                        duplicate = False
                        company_name = doc["entities"][0]["name"]
                        event_type = ""
                        for tag in naf["tags"]:
                            if tag["conceptName"] == "事件":
                                event_type = tag["name"]

                        try:
                            aql = "FOR x IN {} FILTER x.entities[0].name == \"{}\" RETURN x".format(ARANGO_COLLECTION, company_name)
                            results = arango_db.fetch_list(aql)
                            for result in results:
                                for tag in result["tags"]:
                                    ## 相同事件类型才判断是否相似
                                    if tag["name"] == event_type:
                                        logger.info("两篇文章比较: {} ^^^^^ {}".format(result["title"], doc["title"]))
                                        score = self.similarity(result, doc)
                                        if score > THRESHOLD:
                                            logger.info("比较结果: 相似")
                                            duplicate = True
                                        else:
                                            logger.info("比较结果：不相似")
                        except Exception as e:
                            logger.error(str(e))
                        
                        ## 重复就不插入
                        if duplicate:
                            logger.info("该文章与数据库内文章相似")
                            continue

                        try:
                            arango_collection.createDocument(doc).save()
                            self.arango_count += 1
                        except Exception as e:
                            logger.error("插入arangodb出错, 文档id: {}".format(doc["_key"]))

                batch_end_time = time.time()
                logger.info("第 {} - {} 条数据处理结束, 共耗时: {} 秒".format(index, end, int(batch_end_time - batch_start_time)))
            else:
                logger.error("第 {} - {} 条数据处理失败".format(index, end))
            
            index = end

        ## 输出处理结果
        end_time = time.time()
        logger.info("本次news pipeline处理完成，共耗时: {} 秒".format(int(end_time - start_time)))
        logger.info("其中清洗库共 {} 条数据, pipeline共处理 {} 条数据, 其中无关数据 {} 条, 相似数据 {} 条, 导入arango {} 条数据"
                    .format(self.news_count, self.pipeline_count, self.ignore_count, self.duplicate_count, self.arango_count))


if __name__ == '__main__':
    news_pipeline = NewsPipeline()
    if len(sys.argv) > 1:
        news_pipeline.process(sys.argv[1])
    else:
        raise Exception("请输入执行日期参数")
