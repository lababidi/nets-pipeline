import argparse
import yaml
import logging
import json
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from os import listdir
from os.path import isfile, join

from eventcandidate import EventCandidate

class Supportkit:

    def __init__(self, yamlFile):
        with open(yamlFile) as f:  self.parameters = yaml.load(f)

        #todo - log to stdout AND file
        logging.basicConfig(format=self.parameters['logging']['format'])
        self.logger = logging.getLogger("nets")
        self.logger.level=logging.INFO

        self.es_url = 'http://%s:%s' % (self.parameters['elasticsearch']['host'],self.parameters['elasticsearch']['port'])
        self.logger.info("ready. ES URL : %s", self.es_url)
        self.es = Elasticsearch()


    def status(self):
        self.logger.info("Status request")
        indices = ['articles','events']
        idx_client = IndicesClient( self.es)
        for idx in indices:
            if  idx_client.exists(idx):
                self.logger.info("%s contains %s documents." % (idx,self.es.count(index=idx)['count']))
            else:
                self.logger.info("%s does not exist" % idx)

    def load(self, type):
        self.logger.info("Load articles into type %s" % type)
        path = self.parameters['directories']['articles']
        files = [join(path,f)  for f in listdir(path) if isfile(join(path, f))]
        for filename in files:
            with open(filename) as data_file:
                rows = [ json.loads( row) for row in data_file.readlines() ]
                for index,x in enumerate(rows):
                    if '_id' in x: del x['_id']
                    self.es.index(index='articles', doc_type=type, body = x)

    def initialize(self,es_index):
        idx_client = IndicesClient( self.es)
        if idx_client.exists(es_index):
            self.logger.warn("Erasing %s" % es_index)
            idx_client.delete(es_index)

        self.logger.info("Initializing %s" % es_index)
        idx_client.create( es_index)
        self.logger.info("%s ready." % es_index)

    def pipeline(self, type):
        self.logger.info("%s Pipeline" % type)
        if not type in ['phoenix']:
            self.logger.error("%s article type not supported" % type )
            return

        result = self.es.search(index='articles',doc_type=type,size=500)
        hits= result['hits']['hits']
        self.logger.info("Retrieved %s %s articles" % (len(hits), type))
        for article in hits:
            ec = EventCandidate( type, article['_source']['content'], article['_source']['url'])
            ec.find_entities()
            self.es.index(index='events', doc_type=type, body = ec.__dict__)


#
parser = argparse.ArgumentParser(description='NETS support kit.')
parser.add_argument('--yaml',  default='nets.yaml', help='YAML file' )
parser.add_argument('--status', help='Display NETS status', action='store_true')
parser.add_argument('--load', default=None, help='load raw article files with given type' )
parser.add_argument('--pipeline',  default=None, help='convert all articles  of given type to events')
parser.add_argument('--initialize',  default='NA', help='Initialize an index', choices=['articles','events'])


args = parser.parse_args()
me = Supportkit(args.yaml)

if args.status :
    me.status()
elif args.load :
    me.load(args.load)
elif args.pipeline:
    me.pipeline(args.pipeline)
elif not args.initialize == 'NA':
    me.initialize( args.initialize)
