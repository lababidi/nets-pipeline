# TODO: check elastic, if data from data/articles-100.json is not in the db, import it to elastic.

from elasticsearch import Elasticsearch
import argparse

_es = None

def init(url):
    global _es
    _es = Elasticsearch()

    # create article and event indices if needed .
    for ix in ['articles', 'events']:
        print ( ix, _es.indices.exists(index=ix))


parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('integers', metavar='N', type=int, nargs='+',
                    help='an integer for the accumulator')
parser.add_argument('--sum', dest='accumulate', action='store_const',
                    const=sum, default=max,
                    help='sum the integers (default: find the max)')

args = parser.parse_args()
print(args.accumulate(args.integers))
