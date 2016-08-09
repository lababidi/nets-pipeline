from basecomponent import BaseComponent
import spacy

class nlp(BaseComponent):

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

    # spacy engine itself is a static member of this class
    # set up in initialize below

    _nlp = None

    def initialize(self, parameters):
        nlp._nlp = spacy.load("en")


    def bucketlist(self, article, name ):
        if name == 'people': return article['people']
        elif name == 'places': return article['places']
        elif name == 'dates': return article['dates']
        elif name == 'times':return article['times']
        elif name == 'organizations':return article['organizations']
        elif name == 'events':return article['events']
        elif name == 'languages':return article['languages']
        else:
            return article['other']


    def process(self, articles):

        for article in articles:

            article['people'] = []
            article['places'] = []
            article['dates'] = []
            article['times'] = []
            article['organizations'] = []
            article['events'] = []
            article['languages'] = []
            article['other'] = []

            nlp_doc = nlp._nlp(article['content'])

            for entity in nlp_doc.ents:
                bucketname = 'other'
                if entity.label_ in nlp._nlpmap: bucketname = nlp._nlpmap[entity.label_]
                bucket =  self.bucketlist(article, bucketname)
                item = entity.string.strip()
                if len(item) > 0 and item not in bucket:
                    bucket.append(item)

        return articles


