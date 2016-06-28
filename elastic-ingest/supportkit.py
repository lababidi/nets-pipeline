import argparse
import yaml
import logging
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient


class Supportkit:

    def __init__(self, yamlFile):
        with open(yamlFile) as f:  parameters = yaml.load(f)

        #todo - log to stdout AND file
        logging.basicConfig(format=parameters['logging']['format'])
        self.logger = logging.getLogger("nets")
        self.logger.level=logging.INFO

        self.es_url = 'http://%s:%s' % (parameters['elasticsearch']['host'],parameters['elasticsearch']['port'])
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

    def load(self):
        self.logger.info("Load articles")

    def initialize(self,es_index):
        idx_client = IndicesClient( self.es)
        if idx_client.exists(es_index):
            self.logger.warn("Erasing %s" % es_index)
            idx_client.delete(es_index)

        self.logger.info("Initializing %s" % es_index)
        idx_client.create( es_index)
        self.logger.info("%s ready." % es_index)

#
parser = argparse.ArgumentParser(description='NETS support kit.')
parser.add_argument('--yaml',  default='nets.yaml', help='YAML file' )
parser.add_argument('--status', help='Display NETS status', action='store_true')
parser.add_argument('--load', help='load raw article files from the data directory', action='store_true')
parser.add_argument('--initialize',  default='NA', help='Initialize an index', choices=['articles','events'])


args = parser.parse_args()
me = Supportkit(args.yaml)

if args.status :
    me.status()
elif args.load :
    me.load()
elif not args.initialize == 'NA':
    me.initialize( args.initialize)
