#!/home/liangzhi/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-07-02 18:08
# Filename     : news_fragment_pipeline.py
# Description  : 资讯关键句事件抽取, 从arangodb kb_news读取资讯数据, 事件抽取后导入kb_news_fragment
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
import jieba
from pyArango.connection import Connection as ArangoConnection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

## arangodb数据库


## 调用fragment pipeline地址以及批处理大小
BATCH_SIZE = 20

## 相似性阈值
TEXT_THRESHOLD = 0.8
ENTITY_THRESHOLD = 0.8

class FragmentPipeline(object):

    def __init__(self):
        self.news_count = 0             #kb_news需要处理的资讯总数
        self.pipeline_count = 0         #抽取出fragment的资讯数
        self.fragment_count = 0         #存入arango的fragment数
        self.duplicate_count = 0        # 重复事件数目


    def text_similarity(self, sent_1, sent_2):
        set_1 = set([word for word in jieba.cut(sent_1)])
        set_2 = set([word for word in jieba.cut(sent_2)])
        set_all = set_1 & set_2
        return len(set_all) / min(len(set_1), len(set_2))

    
    def entity_similarity(self, entities_1, entities_2):
        if not len(entities_1) or not len(entities_2):
            return 0
        
        set_1 = set([entity["name"] for entity in entities_1])
        set_2 = set([entity["name"] for entity in entities_2])
        set_all = set_1 & set_2

        return len(set_all) / min(len(set_1), len(set_2))


    ## 资讯处理主函数
    def process(self, date_str):
        
        process_date = None         ## 执行日期
        next_date = None            ## 指定日期的第二天间隔

        if date_str == "today":
            date_str = datetime.date.today().strftime("%Y-%m-%d")
        
        logger.info("执行资讯关键事件抽取news fragment pipeline, 日期: " + date_str)
        process_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        next_date = process_date + datetime.timedelta(days=1)
        process_date = datetime.datetime.strftime(process_date, "%Y-%m-%d")
        next_date = datetime.datetime.strftime(next_date, "%Y-%m-%d")

        start_time = time.time()

        ## 从arangodb kb_news获取需要处理的资讯
        arango_connector = ArangoConnection(arangoURL=ARANGO_URL,
                                            username=ARANGO_USER,
                                            password=ARANGO_PASSWD)

        arango_db = arango_connector[ARANGO_DB]
        source_collection = arango_db[SOURCE_COLLECTION]
        target_collection = arango_db[TARGET_COLLECTION]

        aql = "for x in {} FILTER x.update_time >= \"{}\" AND x.update_time <= \"{}\" RETURN x".format(SOURCE_COLLECTION, process_date, next_date)
        results = arango_db.fetch_list(aql)

        self.news_count = len(results)
        logger.info("共找到资讯 {} 条".format(self.news_count))

        logger.info("调用news fragment pipeline服务")
        for i in range(0, self.news_count, BATCH_SIZE):
            start = i
            end = min(self.news_count, start + BATCH_SIZE)
            batch_start_time = time.time()

            doc_batch = results[start : end]

            post_data = {
                "documents": doc_batch,
            }

            response = requests.post(FRAGMENT_PIPELINE_URL, json.dumps(post_data))

            if response.status_code == 200:
                logger.info("fragment pipeline成功返回结果")
                process_docs = response.json().get("body")
                
                ## 返回结果的处理与封装，导入目标arangodb数据库
                for process_doc in process_docs:
                    naf = process_doc.get("naf")
                    if "emfs" in naf:
                        ## 抽取出事件微文档的数量+1 
                        self.pipeline_count += 1

                        for emf in naf["emfs"]:
                            ## 每次插入的时候检测近一周数据重复性: 1) section 短文本相似性; 2)事件类型与实体相似性
                            duplicate_flag = False
                            
                            check_date = None           ## 检查重复数据的日期，检测一周以内的数据
                            check_date = datetime.datetime.strptime(naf["publish_time"], '%Y-%m-%d %H:%M:%S')
                            check_date = check_date - datetime.timedelta(days=7)
                            check_date = datetime.datetime.strftime(check_date, "%Y-%m-%d")

                            sentence = emf["section"]
                            event_type = emf["event_type"]
                            entities = emf.get("entities", [])
                            duplicate_aql = "FOR x in {} FILTER x.publish_time >= \"{}\" RETURN x".format(TARGET_COLLECTION, check_date)
                            duplicate_results = []
                            try:
                                duplicate_results = arango_db.fetch_list(duplicate_aql)
                            except:
                                pass
                            
                            for duplicate_result in duplicate_results:
                                ## 文本相似性
                                if self.text_similarity(sentence, duplicate_result["section"]) > TEXT_THRESHOLD:
                                    duplicate_flag = True
                                    logger.info("该事件句: >>[{}]<<与数据库内事件句:>>[{}]<<文本相似, 过滤".format(sentence, duplicate_result["section"]))
                                    break
                                ## 事件类型与实体相似性
                                if event_type == duplicate_result["event_type"] and self.entity_similarity(entities, duplicate_result.get("entities", [])) > ENTITY_THRESHOLD:
                                    duplicate_flag = True
                                    logger.info("该事件句: >>[{}]<<与数据库内事件句:>>[{}]<<实体相似, 过滤".format(sentence, duplicate_result["section"]))
                                    break

                            if duplicate_flag:
                                self.duplicate_count += 1
                                continue
                            
                            ## 事件不重复则导入
                            doc = {}
                            doc["doc_id"] = naf["doc_id"]
                            doc["section"] = emf["section"]
                            doc["event_type"] = emf["event_type"]
                            doc["entities"] = emf["entities"]
                            doc["action_words"] = emf["action_words"]
                            doc["event_date"] = emf["event_date"]
                            doc["publish_time"] = emf["publish_time"]
                            doc["title"] = naf["title"]
                            doc["create_time"] = datetime.date.today().strftime("%Y-%m-%d %H:%M:%S")
                            doc["update_time"] = doc["create_time"]
                            target_collection.createDocument(doc).save(waitForSync = True)
                            self.fragment_count += 1

                batch_end_time = time.time()
                logger.info("第 {} - {} 条数据处理结束, 共耗时: {} 秒".format(start, end, int(batch_end_time - batch_start_time)))
            else:
                logger.error("第 {} - {} 条数据处理失败".format(start, end))

        ## 输出处理结果
        end_time = time.time()
        logger.info("本次fragment pipeline处理完成，共耗时: {} 秒".format(int(end_time - start_time)))
        logger.info("其中kb_news库共 {} 条数据, pipeline共处理 {} 条数据, 导入kb_new_fragment {} 条数据, 重复事件 {} 条"
                    .format(self.news_count, self.pipeline_count, self.fragment_count, self.duplicate_count))



if __name__ == '__main__':
    fragment_pipeline = FragmentPipeline()
    if len(sys.argv) > 1:
        fragment_pipeline.process(sys.argv[1])
    else:
        raise Exception("请输入执行日期参数")
