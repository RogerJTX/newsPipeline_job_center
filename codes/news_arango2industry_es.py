#!/home/liangzhi/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-08-19 15:52
# Filename     : news_arango2industry_es.py
# Description  : 同步脚本, 将news从arangodb同步到量知产业知识中心es
#******************************************************************************

import sys
import logging
import datetime
import time
import json
from elasticsearch import Elasticsearch
from pyArango.connection import Connection as ArangoConnection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

## arangodb数据库

## ES新闻库


class NewsArango2IndustryEs(object):

    def process(self, date_str):
        arango_count = 0        ## arango中资讯数量
        es_count = 0            ## 导入es的资讯数量
        process_date = None

        if date_str == "yesterday":
            date_str = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        logger.info("将采集日期为: {} 的资讯从arangodb导入es".format(date_str))
        process_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        next_date = process_date + datetime.timedelta(days=1)
        process_date = datetime.datetime.strftime(process_date, "%Y-%m-%d")
        next_date = datetime.datetime.strftime(next_date, "%Y-%m-%d")

        start_time = time.time()
        es = Elasticsearch(ES_URL)
        ## 从arangodb获取需要同步的资讯
        arango_connector = ArangoConnection(arangoURL=ARANGO_URL,
                                            username=ARANGO_USER,
                                            password=ARANGO_PASSWD)

        arango_db = arango_connector[ARANGO_DB]
        aql = "for x in {} FILTER x.update_time >= \"{}\" AND x.update_time <= \"{}\" RETURN x".format(ARANGO_COLLECTION, process_date, next_date)
        # aql = "FOR x in {} RETURN x".format(ARANGO_COLLECTION)
        results = []
        try:
            results = arango_db.AQLQuery(aql, batchSize=1000)
        except Exception as e:
            logger.error("查询arangodb错误: " + str(e))

        for result in results:
            arango_count += 1
            doc = {}
            _id = result["_key"]
            ## 同id覆盖问题
            if es.exists(index=ES_INDEX, doc_type=ES_TYPE, id=_id):
                es.delete(index=ES_INDEX, doc_type=ES_TYPE, id=_id, params={'refresh':'true'})

            doc["title"]        = result["title"]
            doc["publish_time"] = result["publish_time"].split(" ")[0]
            doc["create_time"]  = result["create_time"]
            doc["update_time"]  = result["update_time"]
            doc["url"]          = result["url"]
            doc["content"]      = result["content"]
            doc["abstract"]     = result["abstract"]
            doc["source"]       = result["source"]
            doc["img_url"]      = result["img_url"]
            doc["tags"]         = result["tags"]
            doc["entities"]     = result["entities"]
            doc["is_pushed"]    = False
          
            ## 判断去重
            query = {
                "query": {
                    "match": {
                        "title": doc["title"]
                    }
                }
            }

            try:
                score = es.search(index=ES_INDEX, body=query)["hits"]["max_score"]
                if score and score > 2.0:
                    logger.info("发现类似资讯")
                    continue

                es.index(index=ES_INDEX, doc_type=ES_TYPE, id=_id, body=json.dumps(doc, ensure_ascii=False), request_timeout=30)
                es_count += 1
            except Exception as e:
                logger.error(str(e))

        end_time = time.time()
        logger.info("本次往量知产业知识中心es同步工作完成, 日期: {}, 从arango读取 [{}] 条, 导入es [{}] 条, 耗时: {} 秒".format(date_str, arango_count, es_count, int(end_time - start_time)))


if __name__ == "__main__":
    newsArango2IndustryEs = NewsArango2IndustryEs()

    if len(sys.argv) > 1:
        newsArango2IndustryEs.process(sys.argv[1])
    else:
        raise Exception("请输入执行日期参数")