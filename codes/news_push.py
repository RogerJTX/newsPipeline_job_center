#!/home/liangzhi/anaconda3/bin/python3
#-*- coding: utf-8 -*-
#******************************************************************************
# Author       : jtx
# Last modified: 2020-08-06 16:20
# Filename     : news_push.py
# Description  : 资讯每日早餐包
# 从es获取近一周的资讯作为每日资讯早餐包
#******************************************************************************

import requests
import logging
import sys
import json
import time
import datetime
import hmac
import hashlib
import base64
import urllib.parse
from elasticsearch import Elasticsearch

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

## ES新闻库


# 资讯的详情页面

## 推荐条数控制
PUSH_COUNT = 8

class NewsPush():
    
    def gen_url(self, url, secret):
        timestamp = str(round(time.time() * 1000))       
        secret_enc = secret.encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, secret)
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        DING_URL = "{}&timestamp={}&sign={}".format(url, timestamp, sign)
        return DING_URL

    def process(self, date_str):
        if date_str == "yesterday":
            date_str = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        logger.info("从es查找发布日期为: {} 的近一周资讯".format(date_str))
        process_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        pre_date = process_date - datetime.timedelta(days=7)
        process_date = datetime.datetime.strftime(process_date, "%Y-%m-%d")
        pre_date = datetime.datetime.strftime(pre_date, "%Y-%m-%d")                 ##  搜集近一周的资讯

        es = Elasticsearch(ES_URL)
        query = {
            "query": {
                "bool": {
                    "filter": {
                        "term": {
                            "is_pushed": False                  ## 未推送的消息才可以                           
                        }
                    },
                    "must": {
                        "range": {
                            "publish_time": {
                                "gte": pre_date,
                                "lte": process_date
                            }
                        }
                    }
                }
            }
        }

        response = None
        try:
            response = es.search(index=NEWS_INDEX, body=query)
        except Exception as e:
            logger.error("资讯es查询错误: {}".format(str(e)))
            return

        results = response["hits"]["hits"]
        ## 按照发布时间排序
        results.sort(key=lambda x: x["_source"]["publish_time"], reverse=True)

        news_count = 0                  ## 资讯总数
        company_set = set()             ## 涉及企业数
        contents = []                   ## 链接
        ## 可推送的事件集合
        avaliable_events = ["企业合作", "投融资", "中标招标", "领导考察", "公司上市", "企业获奖", "高管变动", "会议动态", "企业收购"]

        for result in results:
            for tag in result["_source"]["tags"]:
                if tag["conceptName"] == "事件" and tag["name"] in avaliable_events:
                    content = {
                        "title"         :       "【{}】".format(tag["name"]) + result["_source"]["title"],
                        "messageURL"    :       NEWS_URL.format(result["_id"]),
                        "picURL"        :       "https://ss1.bdstatic.com/70cFvXSh_Q1YnxGkpoWK1HF6hhy/it/u=3448042000,668451117&fm=26&gp=0.jpg"    
                    }

                    for entity in result["_source"]["entities"]:
                        company_id = entity["externalReference"]["id"].split("/")[1]
                        company_name = entity["externalReference"]["name"]
                        company_set.add(company_name)

                        company_response = None
                        try:
                            company_response = es.get(index=COMPANY_INDEX, doc_type=COMPANY_TYPE, id=company_id)
                        except Exception as e:
                            logger.error("查询企业es错误: {}".format(str(e)))

                        if company_response:
                            content["picURL"] = company_response["_source"]["logo"]

                    contents.append(content)
                    ## 修改es中资讯是否推送状态
                    update_doc = {
                        "doc": {
                            "is_pushed": True
                        }
                    }
                    try:
                        update_response = es.update(index=NEWS_INDEX, doc_type=NEWS_TYPE, id=result["_id"], body=update_doc)
                    except Exception as e:
                        logger.error("修改资讯推送状态失败, id: {}".format(result["_id"]))

                    news_count += 1
                    ## 控制条数
                    if news_count >= PUSH_COUNT:
                        break  

        ## feedCard 头条
        links = [{
            "title": "量知招商每日资讯: {}".format((date_str[5:]+"日").replace("-", "月")),
            "picURL": "https://ss0.bdstatic.com/70cFvHSh_Q1YnxGkpoWK1HF6hhy/it/u=4257704521,3639258495&fm=15&gp=0.jpg"
        }]

        links.extend(contents)

        if not news_count:
            logger.info("今日无资讯推送")
            return
        
        ## 配置推送群信息
        dest_group = ["招商头条","演示群","测试群"]
        dest_urls = ["..."]
        secrets = ["..."]
        assert(len(dest_urls) == len(secrets))
        assert(len(dest_urls) == len(dest_group))
        
        group_num = len(dest_urls)
        for i in range(group_num):
            DING_URL = self.gen_url(dest_urls[i], secrets[i])
            
            headers = {
                "Content-Type": "application/json"
                }

            data = {
                "msgtype": "feedCard",
                "feedCard": {
                    "links": links
                }
            }

            data = json.dumps(data)
            response = requests.post(url=DING_URL, data=data, headers=headers)
            response = response.json()
            if response["errcode"]:
                logger.error("钉钉群[{}]推送消息失败, 错误码: {}, 错误原因: {}".format(dest_group[i], response["errcode"], response["errmsg"]))
            else:
                logger.info("钉钉推[{}]送消息成功, 共推送消息 {} 条".format(dest_group[i], news_count))


if __name__ == "__main__":
    news_push = NewsPush()

    if len(sys.argv) > 1:
        news_push.process(sys.argv[1])
    else:
        raise Exception("请输入执行日期参数")
