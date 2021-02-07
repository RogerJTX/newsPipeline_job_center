#!/home/liangzhi/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-07-22 17:18
# Filename     : news_arango2es.py
# Description  : 同步脚本, 将news从arangodb同步到es
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

## 导入目标arangodb数据库


## ES新闻库


class NewsArango2es(object):

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
        results = []
        try:
            results = arango_db.fetch_list(aql)
        except Exception as e:
            logger.error("查询arangodb错误: " + str(e))
        arango_count = len(results)

        for result in results:
            doc = {}
            _id = result["_key"]
            ## 同id覆盖问题
            if es.exists(index=ES_INDEX, doc_type="event", id=_id):
                es.delete(index=ES_INDEX, doc_type="event", id=_id, params={'refresh':'true'})

            doc["title"] = result["title"]
            doc["date"] = result["publish_time"].split(" ")[0]
            doc["createTime"] = datetime.datetime.today().strftime("%Y-%m-%d")
            doc["url"] = result["url"]
            doc["content"] = result["content"]
            doc["abstract"] = result["abstract"]
            doc["html"] = result["html"]
            doc["source"] = result["source"]
            if doc["source"] != "公众号":
                continue

            doc["area"] = ""
            doc["park"] = ""
            
            doc["logo"] = ""
            imgs = result["img_url"]
            if imgs:
                doc["logo"] = imgs[0]
            
            ## 行业、领域、标签分类
            doc["industry"] = ""
            doc["domain"] = ""
            doc["subject"] = ""
            tags = result["tags"]
            for tag in tags:
                if tag["conceptName"] == "产业" and not doc["industry"]:
                    doc["industry"] = tag["name"]
                if tag["conceptName"] == "产业领域" and not doc["domain"]:
                    doc["domain"] = tag["name"]
                    doc["tags"] = doc["domain"]
                if tag["conceptName"] == "事件":
                    doc["subject"] = tag["name"]

            if not doc["industry"]:
                logger.info("该资讯没有行业分类: {}".format(doc["title"]))
                continue
            ## 生物医药行业合并
            if doc["industry"] in ["生物制药", "医疗器械"]:
                doc["industry"] = "生物医药"

            ## 企业名关联
            entities = result["entities"]
            doc["company_id"] = ""
            doc["company_name"] = ""
            if len(entities) > 0:
                doc["company_id"] = entities[0]["externalReference"]["id"].split("/")[1]
                doc["company_name"] = entities[0]["externalReference"]["name"]

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

                es.index(index=ES_INDEX, doc_type="event", id=_id, body=json.dumps(doc, ensure_ascii=False), request_timeout=30)
                es_count += 1
            except Exception as e:
                logger.error(str(e))

        end_time = time.time()
        logger.info("本次往es同步工作完成, 日期: {}, 从arango读取 [{}] 条, 导入es [{}] 条, 耗时: {} 秒".format(date_str, arango_count, es_count, int(end_time - start_time)))


if __name__ == "__main__":
    news_arango2es = NewsArango2es()

    if len(sys.argv) > 1:
        news_arango2es.process(sys.argv[1])
    else:
        raise Exception("请输入执行日期参数")