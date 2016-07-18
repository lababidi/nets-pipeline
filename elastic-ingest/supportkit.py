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
        self.articlesindex = self.parameters['elasticsearch']['articles-index']
        self.eventindex = self.parameters['elasticsearch']['events-index']

        EventCandidate._geocoder = self.parameters['geocoder']['url']


    def status(self):
        self.logger.info("Status request")
        indices = [ self.articlesindex, self.eventindex ]
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

    def initarticles(self):
        idx_client = IndicesClient( self.articlesindex)
        idx_client.create(self.articlesindex)

    def initevents(self):
        idx_client = IndicesClient(self.eventindex)

        idx_client.create(self.eventindex)
        idx_client.put_mapping( doc_type='event', index=[self.eventindex],body= EventCandidate._es_map )

    def initialize(self,spec):
        es_index = self.articlesindex
        if spec == 'events': es_index  self.eventindex

        idx_client = IndicesClient( self.es)

        self.logger.info("Initializing %s" % es_index)
        if es_index == 'articles':
            self.initarticles(es_index)
        else:
            self.initevents(es_index)

        self.logger.info("%s ready." % es_index)

    def pipeline(self, type):
        self.logger.info("%s Pipeline" % type)
        if not type in ['nets-article']:
            self.logger.error("%s article type not supported" % type )
            return

        result = self.es.search(index='nets-article',doc_type=type,size=500)
        hits= result['hits']['hits']
        self.logger.info("Retrieved %s %s articles" % (len(hits), type))
        for article in hits:
            ec = EventCandidate()
            ec.source = type
            ec.article = article['_id']
            ec.content = article['_source']['content']
            ec.url = article['_source']['url']
            ec.title = article['_source']['title']
            ec.language = article['_source']['language']
            ec.date_collected = article['_source']['date_published']
            ec.date_published = article['_source']['date_collected']
            ec.date_collected_as_date_published = False
#            if len(ec.date_published) == 0:
 #               ec.date_published = ec.date_collected
  #              ec.date_collected_as_date_published = True

            ec.find_entities()
#            ec.geocode()

            self.es.index(index='nets-events', doc_type='event', body = ec.__dict__)


    def export(self,type):
        result = self.es.search(index='nets-events', doc_type=type, size=500)
        hits = result['hits']['hits']
        self.logger.info("Retrieved %s %s events for export" % (len(hits), type))
        filename =  join( self.parameters['directories']['export'], 'nets-events.json')
        self.logger.info( '%s' % filename )
        with open(filename, 'w') as f:
            json.dump(hits, f)
#
parser = argparse.ArgumentParser(description='NETS support kit.')
parser.add_argument('--yaml',  default='nets.yaml', help='YAML file' )
parser.add_argument('--status', help='Display NETS status', action='store_true')
parser.add_argument('--load', default=None, help='load raw article files with given type' )
parser.add_argument('--export', default=None, help='export up to 100 events with given type' )
parser.add_argument('--pipeline',  default=None, help='convert all articles  of given type to events')
parser.add_argument('--initialize',  default='NA', help='Initialize an index', choices=['nets-article','nets-events'])



args = parser.parse_args()
me = Supportkit(args.yaml)

if args.status :
    me.status()
elif args.load :
    me.load(args.load)
elif args.export :
    me.export(args.export)
elif args.pipeline:
    me.pipeline(args.pipeline)
elif not args.initialize == 'NA':
    me.initialize( args.initialize)
