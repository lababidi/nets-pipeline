import argparse
import yaml
import logging
import json
from  eventmapping import eventmapping
from pipeline import Pipeline
from elasticsearch import Elasticsearch, ElasticsearchException
from elasticsearch.client import IndicesClient
from os import listdir
from os.path import isfile, join


class Ncli:
    _version = 1.0
    _yaml = 'nets.yaml'

    def __init__(self, yamlFile):
        with open(yamlFile) as f:
            self.parameters = yaml.load(f)

        # todo - log to stdout AND file
        logging.basicConfig(format=self.parameters['logging']['format'])
        self.logger = logging.getLogger("NETS")
        self.logger.level = logging.INFO
        self.eventpipeline = Pipeline(self.parameters)

        self.es_url = 'http://%s:%s' % (
        self.parameters['elasticsearch']['host'], self.parameters['elasticsearch']['port'])
        self.logger.info("ready. ES URL : %s", self.es_url)
        try:
            self.es = Elasticsearch()
            info = self.es.info()
            self.logger.info("Connected to Elasticsearch v. %s, name: %s" % (info['version']['number'], info['name']))

        except ElasticsearchException, e:
            self.logger.error("Elasticsearch is not available.")
            exit(0)

    def indexinfo(self, target):
        for item in self.parameters['elasticsearch']['indexes']:
            if item['type'] == target:
                return (item['name'], item['doctype'])

    def status(self):
        self.logger.info("Status request")
        idx_client = IndicesClient(self.es)
        for idx in ['article', 'event']:
            es_index = self.indexinfo(idx)[0]
            if idx_client.exists(es_index):
                self.logger.info("%s contains %s documents." % (idx, self.es.count(index=es_index)['count']))
                if idx == 'article':
                    query = {"query": {"term" : {"status" :1 }}}
                    self.logger.info(
                        "%s articles have been processed." % self.es.count(index=es_index, body=query)['count'])
            else:
                self.logger.info("%s does not exist" % es_index)

    def initialize(self, idx):
        es_index, es_doctype = self.indexinfo(idx)
        self.logger.info("Initializing %s" % es_index)
        idx_client = IndicesClient(self.es)
        if idx_client.exists(es_index):
            idx_client.delete(es_index)
        idx_client.create(es_index)
        if idx == 'event': idx_client.put_mapping(doc_type=es_doctype, index=[es_index], body=eventmapping())
        self.logger.info("%s ready." % es_index)

    def pipeline(self, n):
        es_index, es_doctype = self.indexinfo('article')
        self.logger.info("Send %s articles through the pipeline" % n)
        query =  '{"query": { "bool": { "must_not": { "exists": { "field": "status" }}}}}'
        result = self.es.search(index=es_index,doc_type=es_doctype,size=n, body=query)
        articles = result['hits']['hits']

        self.eventpipeline.batch( articles )

    def load(self):
        self.logger.info("Load articles")
        es_index, es_doctype = self.indexinfo('article')
        path = self.parameters['directories']['articles']
        files = [join(path, f) for f in listdir(path) if isfile(join(path, f))]
        for filename in files:
            with open(filename) as data_file:
                rows = [json.loads(row) for row in data_file.readlines()]
                for index, article in enumerate(rows):
                    if '_id' in article: del article['_id']
                    self.es.index(index=es_index, doc_type=es_doctype, body=article)


parser = argparse.ArgumentParser(description='NETS Command Line Interface. v %s' % Ncli._version)
parser.add_argument('--yaml', default='nets.yaml', help='YAML file')
parser.add_argument('--status', help='Display NETS status', action='store_true')
parser.add_argument('--load', default=None, help='load raw article files', action='store_true')
parser.add_argument('--export', default=None, help='export up to 100 events with given type')
parser.add_argument('--pipeline', default=None, help='Run the pipeline for up to given number of articles')
parser.add_argument('--initialize', default='NA', help='Initialize an index', choices=['article', 'event'])

args = parser.parse_args()
me = Ncli(args.yaml)
if args.status:
    me.status()
elif not args.initialize == 'NA':
    me.initialize(args.initialize)
elif args.load:
    me.load()
elif args.export:
    me.export(args.export)
elif args.pipeline:
    me.pipeline(args.pipeline)
