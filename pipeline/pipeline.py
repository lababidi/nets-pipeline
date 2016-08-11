import logging
import json
from elasticsearch import Elasticsearch,ElasticsearchException
from nlpcomponent import nlp
from geocodecomponent import geocoder
from os.path import join


class Pipeline:

    def __init__(self,parameters):

        for item in parameters['elasticsearch']['indexes']:
            if item['type'] == 'raw-article':
                self.raw_article_index = item['name']
                self.raw_article_doctype = item['doctype']
            if item['type'] == 'enhanced-article':
                self.enhanced_article_index = item['name']
                self.enhanced_article_doctype = item['doctype']

        self.logger = logging.getLogger('NETS')
        self.persistdirectory = parameters['directories']['persist']
        self.logger.info("Persist to directory %s", self.persistdirectory)
        # initialize pipeline components

        self.components = []
        for name in parameters['pipeline']['components']:
            component = None
            if name == 'nlp': component = nlp(parameters)
            if name == 'geocode': component = geocoder(parameters)
            self.components.append( { 'name' : name, 'component' : component})
            self.logger.info("pipeline component: %s" % name)

        try:
            self.es = Elasticsearch(hosts=[
                {'host': parameters['elasticsearch']['host'],
                 'port': parameters['elasticsearch']['port']}])
            info = self.es.info()

        except ElasticsearchException:
            self.logger.info("Elasticsearch is not available.")
            exit(0)

    def reformat(self, raw_article ):
        article = {}

        article['raw_article_id'] = raw_article['_id']
        article['content'] = raw_article['_source']['content']
        article['url'] = raw_article['_source']['url']
        article['title'] = raw_article['_source']['title']
        article['language'] = raw_article['_source']['language']
        article['source'] = 'NETS'
        article['publisher'] = raw_article['_source']['publisher']

        if 'date_collected' in raw_article['_source']:
            article['date_collected'] = raw_article['_source']['date_collected']
        else:
            article['date_collected'] = raw_article['_source']['date_added']['$date']

        if 'date_published' in raw_article['_source']:
            article['date_published'] = raw_article['_source']['date_published']
        else:
            article['date_published'] = raw_article['_source']['date_added']['$date']

        return article

    def persisttofile(self, articles):
        for article in articles:
            path = join( self.persistdirectory, article['raw_article_id'] + '.json')
            with open( path, 'w') as f:
                json.dump( article, f)

    def persist(self, articles):

        # for each article, write out the article and set the status to 1 for the associated raw_article

        payload  = { "doc" : { "status" : 1 }}
        for article in articles:
            self.es.index(index=self.enhanced_article_index, doc_type=self.enhanced_article_doctype, body=article)
            self.es.update(index=self.raw_article_index, doc_type=self.raw_article_doctype,id=article['raw_article_id'],body=payload)


    def batch(self, raw_articles):
        articles = []
        for raw_article in raw_articles:
            articles.append( self.reformat(raw_article))

        for c in self.components:
            component = c['component']
            if component is not None:
                article = component.process(articles)
            if c['name'] == 'persisttofile':
                self.persisttofile(articles)
            if c['name'] == 'persist':
                self.persist(articles)

    def single(self, raw_article):
        raw_articles = []
        raw_articles.append(raw_article)
        self.batch(raw_articles)


