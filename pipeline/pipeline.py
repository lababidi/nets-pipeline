import logging
from elasticsearch import Elasticsearch,ElasticsearchException
from nlpcomponent import nlp
from geocodecomponent import geocoder

class Pipeline:

    def __init__(self,parameters):

        for item in parameters['elasticsearch']['indexes']:
            if item['type'] == 'article':
                self.article_index = item['name']
                self.article_doctype = item['doctype']
            if item['type'] == 'event':
                self.event_index = item['name']
                self.event_doctype = item['doctype']

        self.logger = logging.getLogger('NETS')
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

    def articletoevent(self, article ):
        event = {}

        #todo - should be storing this with the article

        event['article'] = article['_id']
        event['content'] = article['_source']['content']
        event['url'] = article['_source']['url']
        event['title'] = article['_source']['title']
        event['language'] = article['_source']['language']
        event['source'] = 'NETS'
        event['publisher'] = article['_source']['publisher']

        if 'date_collected' in article['_source']:
            event['date_collected'] = article['_source']['date_collected']
        else:
            event['date_collected'] = article['_source']['date_added']['$date']

        if 'date_published' in article['_source']:
            event['date_published'] = article['_source']['date_published']
        else:
            event['date_published'] = article['_source']['date_added']['$date']

        return event

    def persisttofile(self, events):
        for event in events:
            print "......."
            print event

    def persist(self,events):

        payload  = { "doc" : { "status" : 1 }}

        # for each event, write out the event and set the status to 1 for the associated article
        for event in events:
            self.es.index(index=self.event_index, doc_type=self.event_doctype, body=event)
            self.es.update(index=self.article_index, doc_type=self.article_doctype,id=event['article'],body=payload)


    def batch(self, articles):
        events = []
        for article in articles:
            events.append( self.articletoevent(article))

        for c in self.components:
            component = c['component']
            if component is not None:
                event = component.process(events)
            if c['name'] == 'persisttofile':
                self.persisttofile(events)
            if c['name'] == 'persist':
                self.persist(events)

    def single(self, article):
        articles = []
        articles.append(article)
        self.batch(articles)


