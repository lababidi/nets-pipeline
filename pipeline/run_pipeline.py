import argparse
import yaml
import logging
import json
from  eventmapping import eventmapping
from pipeline import Pipeline
from elasticsearch import Elasticsearch, ElasticsearchException
from elasticsearch.client import IndicesClient
from os import listdir,getenv, system
from os.path import isfile, join
from time import sleep
import sys


class RunPipeline:

    _version = 1.0
    _yaml = 'nets.yaml'

    def __init__(self, yamlFile):

        with open(yamlFile) as f:
            self.parameters = yaml.load(f)

        #if environment variables are provided for elasticsearch host and port
        #use those instead

        for key in ['HOST', 'PORT']:
            v = getenv( 'NETS_ES_%s' % key )
            if v is not None:
                self.parameters['elasticsearch'][key.lower()] = v

        # log to stdout
        root = logging.getLogger()
        root.setLevel(logging.INFO)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        root.addHandler(ch)

        logging.basicConfig(format=self.parameters['logging']['format'])
        self.logger = logging.getLogger("pipeline")
        self.logger.level = logging.INFO
        self.delay = self.parameters['pipeline']['delay']
        self.batchsize = self.parameters['pipeline']['batchsize']

        for lib_loggers in ("requests", "urllib3", "elasticsearch"):
            logging.getLogger(lib_loggers).setLevel(logging.WARNING)

        try:
            self.es = Elasticsearch(hosts=[
                {'host': self.parameters['elasticsearch']['host'],
                 'port' : self.parameters['elasticsearch']['port']}])
            info = self.es.info()
            self.logger.info("Connected to Elasticsearch v. %s, name: %s" % (info['version']['number'], info['name']))

        except ElasticsearchException, e:
            self.logger.info("Elasticsearch is not available.")
            exit(0)

        self.logger.info("Pipeline. Batch size: %s.  Delay: %s" % (self.batchsize, self.delay))
        self.eventpipeline = Pipeline(self.parameters)

    def indexinfo(self, target):
        for item in self.parameters['elasticsearch']['indexes']:
            if item['type'] == target:
                return (item['name'], item['doctype'])

    def pipeline(self, n):
        es_index, es_doctype = self.indexinfo('article')
        query =  '{"query": { "bool": { "must_not": { "exists": { "field": "status" }}}}}'
        result = self.es.search(index=es_index,doc_type=es_doctype,size=n, body=query)
        articles = result['hits']['hits']
        self.eventpipeline.batch( articles )
        self.logger.info("%s articles processed.  Enhanced article count: %s." % (n, self.es.count(index='nets-event')['count']))

    def autopipeline(self):
        while True:
            self.pipeline(self.batchsize)
            sleep(self.delay)


runner = RunPipeline( RunPipeline._yaml)
runner.autopipeline()