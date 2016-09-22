import argparse
import json
import logging
from os import listdir
from os.path import isfile, join

import yaml
from elasticsearch import Elasticsearch, ElasticsearchException
from elasticsearch.client import IndicesClient

from elastic import event_mapping
from pipeline import Pipeline


class Ncli:
    _version = 1.0
    _yaml = 'nets.yaml'

    def __init__(self, yamlfile):

        with open(yamlfile) as f:
            self.parameters = yaml.load(f)

        logging.basicConfig(format=self.parameters['logging']['format'])
        self.logger = logging.getLogger("NETS")
        self.logger.level = logging.INFO

        try:
            self.es = Elasticsearch(hosts=[
                {'host': self.parameters['elasticsearch']['host'],
                 'port': self.parameters['elasticsearch']['port']}])
            info = self.es.info()
            self.logger.info("Connected to Elasticsearch v. %s, name: %s" % (info['version']['number'], info['name']))

        except ElasticsearchException:
            self.logger.info("Elasticsearch is not available.")
            exit(0)

    def indexinfo(self, target):
        for item in self.parameters['elasticsearch']['indexes']:
            if item['type'] == target:
                return item['name'], item['doctype']

    # display status check and exit

    def status(self):
        idx_client = IndicesClient(self.es)
        for idx in ['raw-article', 'enhanced-article']:
            es_index = self.indexinfo(idx)[0]
            if idx_client.exists(es_index):
                self.logger.info("%s contains %s documents." % (idx, self.es.count(index=es_index)['count']))
                if idx == 'article':
                    query = {"query": {"term": {"status": 1}}}
                    self.logger.info(
                        "%s articles have been processed." % self.es.count(index=es_index, body=query)['count'])
            else:
                self.logger.info("%s does not exist" % es_index)

    # initialize articles or events index.

    def initialize(self, idx):
        es_index, es_doctype = self.indexinfo(idx)
        self.logger.info("Initializing %s" % es_index)
        idx_client = IndicesClient(self.es)
        if idx_client.exists(es_index):
            idx_client.delete(es_index)
        idx_client.create(es_index)
        if idx == 'event':
            idx_client.put_mapping(doc_type=es_doctype, index=[es_index], body=event_mapping())
        self.logger.info("%s ready." % es_index)

    # find n articles and run them through the pipeline

    def pipeline(self, n):
        self.eventpipeline = Pipeline(self.parameters)
        es_index, es_doctype = self.indexinfo('raw-article')
        self.logger.info("Send %s articles through the pipeline" % n)
        query = '{"query": { "bool": { "must": { "match": { "status" : 0 }}}}}'
        result = self.es.search(index=es_index, doc_type=es_doctype, size=n, body=query)
        articles = result['hits']['hits']

        self.eventpipeline.batch(articles)

    # load articles from json files in a directory

    def load(self):
        self.logger.info("Load articles")
        es_index, es_doctype = self.indexinfo('raw-article')
        path = self.parameters['directories']['articles']
        files = [join(path, f) for f in listdir(path) if isfile(join(path, f))]
        for filename in files:
            with open(filename) as data_file:
                rows = [json.loads(row) for row in data_file.readlines()]
                for index, article in enumerate(rows):
                    if '_id' in article: del article['_id']
                    self.es.index(index=es_index, doc_type=es_doctype, body=article)

    def reset(self, n):
        resetpayload = {"doc": {"status": 0}}

        self.logger.info("reset %s  raw articles" % n)
        es_index, es_doctype = self.indexinfo('raw-article')
        query = '{"query": { "bool": { "must": { "match": { "status": "1" }}}}}'
        result = self.es.search(index=es_index, doc_type=es_doctype, size=n, body=query)
        articles = result['hits']['hits']
        tic = 0
        for article in articles:
            aid = article["_id"]
            status = article["_source"]["status"]
            self.es.update(index=es_index, doc_type=es_doctype, id=aid, body=resetpayload)
            tic = tic + 1
            if tic == 500:
                print("...", tic)
                tic = 0


parser = argparse.ArgumentParser(description='NETS Command Line Interface. v %s' % Ncli._version)
parser.add_argument('--yaml', default='nets.yaml', help='YAML file')
parser.add_argument('--status', help='Display NETS status', action='store_true')
parser.add_argument('--load', default=None, help='load raw article files', action='store_true')
parser.add_argument('--pipeline', default=None, help='Run the pipeline for up to given number of articles')
parser.add_argument('--initialize', default='NA', help='Initialize an index', choices=['article', 'enhanced-article'])
parser.add_argument('--reset', default='NA', help='Reset status of raw articles')

args = parser.parse_args()
me = Ncli(args.yaml)

# if no arguments on command line, pick up command from environment.

if args.status:
    me.status()
elif not args.initialize == 'NA':
    me.initialize(args.initialize)
elif args.load:
    me.load()
elif args.pipeline:
    me.pipeline(args.pipeline)
elif args.reset:
    me.reset(args.reset)
