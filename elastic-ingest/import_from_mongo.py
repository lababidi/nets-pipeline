"""
This file can be used to test/import open event database scraped articles from mongo into elasticsearch
"""

from datetime import datetime
from pymongo import MongoClient
from elasticsearch import Elasticsearch
import parsedatetime as pdt


def run():
    # ---- TODO: specify db and credentials before running
    mongo_auth_db = ''
    mongo_auth_user = ''
    mongo_auth_pass = ''
    mongo_db_host = 'localhost:27017'
    es_db_host = 'localhost:9200'
    es_auth_index = 'nets-article'
    es_add_if_url_is_unique = False

    # -- connect to mongodb & elasticsearch
    mongo = MongoClient(host=mongo_db_host)
    mongo[mongo_auth_db].authenticate(mongo_auth_user, mongo_auth_pass)
    mongo_db = mongo[mongo_auth_db]
    es = Elasticsearch([es_db_host])

    # -- walk through all stories in mongodb
    stories = mongo_db['stories'].find()
    print('---[ total stories to process: {}'.format(stories.count()))

    articles_added = 0
    cal = pdt.Calendar()

    for item_index in range(0, stories.count()):
        item = stories[item_index]

        found = False
        if es_add_if_url_is_unique:
            found = check_url(item["url"], es, es_auth_index)

        if not found:
            try:
                time_struct, parse_status = cal.parse(item["date"])
                date_parsed = datetime(*time_struct[:6]).isoformat()
            except (Exception, TypeError, ValueError, KeyError, IndexError, OverflowError, AttributeError):
                date_parsed = None

            article = {
                "url":          item["url"],
                "title":        item["title"],
                "publisher":    item["source"],
                "language":     item["language"],
                "content":      item["content"],

                "date_published_original":  item["date"],
                "date_published":           date_parsed,
                "date_collected":           item["date_added"]
            }

            es.index(index=es_auth_index, doc_type=es_auth_index, body=article)
            if item_index % 100 == 0:
                print('---[ processed: {} added: {}'.format(item_index, articles_added))
            articles_added += 1

    print('---[ total articles added to elastic:{}'.format(articles_added))


def check_url(url, es, es_auth_index):
    response = es.search(index=es_auth_index, doc_type=es_auth_index, body={
        "query":
            {
                "match_phrase": {
                    "url": url
                }
            }
        }, size=0, terminate_after=1, ignore_unavailable=True)

    return response["hits"]["total"] > 0


if __name__ == '__main__':
    print('Running import mongo to elasticsearch')
    run()
