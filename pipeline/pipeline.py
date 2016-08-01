import spacy
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient

class Pipeline:

    _nlp = None
    _geocoder = None
    _logger = None

    _nlpmap = {
        'PERSON' : 'people',
        'GPE' : 'places',
        'LOC' : 'places',
        'FAC' : 'places',
        'DATE' : 'dates',
        'TIME' : 'times',
        'ORG' : 'organizations',
        'LANGUAGE' : 'languages',
        'EVENT' : 'events'
    }


    _nlpentitymap = {
        'people' : 'person',
        'places' : 'place',
        'dates' : 'nlpdate',
        'times' : 'nlptime',
        'organizations' : 'organization',
        'languages' : 'language',
         'events' : 'nlpevent'
    }

    def __init__(self,parameters):
        Pipeline._nlp = spacy.load("en")
        self.es = Elasticsearch(
            hosts = [
                {'host': parameters['elasticsearch']['host'],
                 'port': parameters['elasticsearch']['port']}])


        for item in parameters['elasticsearch']['indexes']:
            if item['type'] == 'article':
                self.article_index = item['name']
                self.article_doctype = item['doctype']
            if item['type'] == 'event':
                self.event_index = item['name']
                self.event_doctype = item['doctype']

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

        event['people'] = []
        event['places'] = []
        event['dates'] = []
        event['times'] = []
        event['organizations'] = []
        event['events'] = []
        event['languages'] = []
        event['other'] = []
        return event


    def bucketlist(self, event, name ):
        if name == 'people': return event['people']
        elif name == 'places': return event['places']
        elif name == 'dates': return event['dates']
        elif name == 'times':return event['times']
        elif name == 'organizations':return event['organizations']
        elif name == 'events':return event['events']
        elif name == 'languages':return event['languages']
        else:
            return event['other']


    def find_entities(self,event):
        nlp_doc = Pipeline._nlp(event['content'])

        for entity in nlp_doc.ents:
            bucketname = 'other'
            if entity.label_ in Pipeline._nlpmap: bucketname = Pipeline._nlpmap[entity.label_]
            bucket =  self.bucketlist(event, bucketname)
            item = entity.string.strip()
            if len(item) > 0 and item not in bucket:
                bucket.append(item)

    #todo - use es.bulk functions
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

        for event in events:
            self.find_entities(event)

        self.persist(events)

    def single(self, article):
        articles = []
        articles.append(article)
        self.batch(articles)


