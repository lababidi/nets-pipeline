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


    def process(self, events):

        for event in events:

            event['people'] = []
            event['places'] = []
            event['dates'] = []
            event['times'] = []
            event['organizations'] = []
            event['events'] = []
            event['languages'] = []
            event['other'] = []

            nlp_doc = nlp._nlp(event['content'])

            for entity in nlp_doc.ents:
                bucketname = 'other'
                if entity.label_ in nlp._nlpmap: bucketname = nlp._nlpmap[entity.label_]
                bucket =  self.bucketlist(event, bucketname)
                item = entity.string.strip()
                if len(item) > 0 and item not in bucket:
                    bucket.append(item)

        return events


