from elasticsearch import Elasticsearch

## ES新闻库


es = Elasticsearch(ES_URL)

mappings = {
    "settings": {
        "index": {
            "analysis": {
                "analyzer": {
                    "pinyin_analyzer": {
                        "filter": "word_delimiter",
                        "tokenizer": "my_pinyin"
                    }
                },
                "tokenizer": {
                    "my_pinyin": {
                        "first_letter": "none",
                        "padding_char": " ",
                        "type": "pinyin"
                    }
                }
            },
            "number_of_replicas": "0",
            "number_of_shards": "1",
        }
    },
    "mappings": {
        ES_TYPE: {
            "properties": {
                "url": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "source": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "title": {
                    "analyzer": "ik",
                    "type": "string",
                    "fields": {
                        "pinyin": {
                            "analyzer": "pinyin_analyzer",
                            "type": "string"
                        }
                    }
                },  
                "content": {
                    "analyzer": "ik",
                    "type": "string"
                },
                "abstract": {
                    "analyzer": "ik",
                    "type": "string"
                },
                "img_url": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "entities": {
                    "properties": {
                        "name": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "type": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                    }
                },
                "publish_time": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd"
                },
                "create_time": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss"
                },
                "tags": {
                    "type": "nested",
                    "properties": {
                        "name": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "conceptId": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "conceptName": {
                            "type": "string",
                            "index": "not_analyzed"
                        }
                    }
                } 
            }   
        } 
    }  
}

res = es.indices.create(index=ES_INDEX, body=mappings)
