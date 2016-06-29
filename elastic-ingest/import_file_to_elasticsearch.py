"""
This file can be used to import articles from json of csv into elastic. It gives each article a random date within a
specified date range. It also transforms the input set to wor for elastic.
"""

import csv
import json
from random import randrange
from datetime import timedelta, datetime
from elasticsearch import Elasticsearch

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
filename = '../data/articles.json'
es_id = 0
range_begin = datetime.strptime('Jan 1 2016  12:00AM', '%b %d %Y %I:%M%p')
range_end = datetime.today()


def random_date(start, end):
    """
    This function will return a random datetime between two datetime
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)


def read_csv():
    global es_id
    with open(filename) as f:
        items = csv.DictReader(f)
        for item in items:
            article = article_from_dict(item)
            es_id += 1
            es.index(index='article', doc_type='article', id=es_id, body=article)


def read_json():
    global es_id
    with open(filename) as f:
        for line in f:
            item = json.loads(line)
            article = article_from_dict(item)
            es_id += 1
            es.index(index='article', doc_type='article', id=es_id, body=article)


def article_from_dict(item):
    article = {
        "mongo_id": item["_id"]["$oid"],
        "content":  item["content"],
        "source":   item["source"],
        "date":     random_date(range_begin, range_end),
        # "date":     item["date_added"]["$date"],
        "language": item["language"],
        "title":    item["title"],
        "url":      item["url"],
        "stanford": item["stanford"],
    }

    return article


if __name__ == '__main__':
    read_json()
    print('articles imported: {}', es_id)

