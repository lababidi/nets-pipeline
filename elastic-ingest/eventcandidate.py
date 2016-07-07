import spacy
import logging
import urllib2
import json
from time import sleep

class EventCandidate:

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

    _es_map = \
        {
            "event": {
                "properties": {
                    "publisher": {
                        "type": "string"
                    },
                    "language": {
                        "type": "string"
                    },
                    "publisher": {
                        "type": "string"
                    },
                    "content": {
                        "type": "string",
                        "index" : "analyzed"
                    },
                    "url": {
                        "type": "string"
                    },
                    "article": {
                        "type": "string"
                    },
                    "title": {
                        "type": "string"
                    },
                    "date_collected": {
                        "type": "date"
                    },
                    "date_published": {
                        "type": "string"
                    },
                    "date_collected_as_date_publshed": {
                        "type": "boolean"
                    },
                    "people": {
                        "type" : "object",
                        "properties" : {
                            "code" : { "type" : "string" },
                            "value" : { "type" : "string" }
                        }
                    }

                }
            }
        }

    _proxy =  {'status': 'OK', 'results': [{'geometry': {'location_type': 'APPROXIMATE', 'bounds': {
            'northeast': {'lat': 24.7396987, 'lng': 80.21023199999999},
            'southwest': {'lat': 24.6879175, 'lng': 80.1552576}}, 'viewport': {
            'northeast': {'lat': 24.7396987, 'lng': 80.21023199999999},
            'southwest': {'lat': 24.6879175, 'lng': 80.1552576}},
            'location': {'lat': 24.7180311, 'lng': 80.1819268}},
            'address_components': [
                    {'long_name': 'Panna', 'types': ['locality', 'political'],
                    'short_name': 'Panna'}, {'long_name': 'Panna', 'types': [
                    'administrative_area_level_2', 'political'],
                    'short_name': 'Panna'},
                    {'long_name': 'Madhya Pradesh',
                    'types': ['administrative_area_level_1', 'political'],
                    'short_name': 'MP'},
                    {'long_name': 'India', 'types': ['country', 'political'],
                    'short_name': 'IN'},
                    {'long_name': '488001', 'types': ['postal_code'],
                    'short_name': '488001'}],
            'place_id': 'ChIJsZwuHIcDgzkRdPVvaw6F5VY',
            'formatted_address': 'Panna, Madhya Pradesh 488001, India',
            'types': ['locality', 'political']}]}

    def __init__(self):
        self.people = []
        self.places = []
        self.dates = []
        self.times = []
        self.organizations= []
        self.events = []
        self.languages = []
        self.other = []


    def bucketlist(self, name ):
        if name == 'people': return self.people
        elif name == 'places': return self.places
        elif name == 'dates': return self.dates
        elif name == 'times':return self.times
        elif name == 'organizations':return self.organizations
        elif name == 'events':return self.events
        elif name == 'languages':return self.languages
        else:
            return self.other


    @property
    def type(self):return self.type

    @type.setter
    def type(self,v): self.type = v

    @property
    def content(self): return self.content

    @content.setter
    def content(self, v): self._content = v

    @property
    def url(self): return self.url

    @url.setter
    def url(self, v): self._url = v

    @property
    def article(self): return self.article

    @article.setter
    def article(self, v): self._article = v

    @property
    def title(self): return self.title

    @title.setter
    def title(self, v): self._title = v

    @property
    def language(self): return self.language

    @language.setter
    def language(self, v): self._language = v


    @property
    def publisher(self): return self.publisher

    @publisher.setter
    def publisher(self, v): self._publisher = v

    @property
    def date_collected(self): return self.date_collected

    @date_collected.setter
    def date_collected(self, v): self._date_collected = v

    @property
    def date_published(self): return self.date_published

    @date_published.setter
    def date_published(self, v): self._date_published = v

    @property
    def date_collected_as_date_published(self): return self.date_collected_as_date_published

    @date_collected_as_date_published.setter
    def date_collected_as_date_published(self, v): self._date_collected_as_date_published = v

    def content(self):
        return self.content

    # run spacy here

    def find_entities(self):
        if EventCandidate._nlp == None:
            EventCandidate._nlp = spacy.load("en")

        nlp_doc = EventCandidate._nlp(self.content)

        for entity in nlp_doc.ents:
            bucket = 'other'
            if entity.label_ in EventCandidate._nlpmap: bucket = EventCandidate._nlpmap[entity.label_]
            item = { 'code' : entity.label_, 'value' : entity.string }
            #todo use reference
            self.bucketlist(bucket).append(item)

    # run geocoder here

    def geocode(self):

        # for place in ...
        # disabled temporarily use first result for all, for now

            if len( self.entities['places']) == 0 : return

            sleep(3)
            geocode = EventCandidate._proxy
            place = self.entities['places'][0]
            try:
                url = EventCandidate._geocoder.replace('__PAYLOAD__', place['value'])
                result = urllib2.urlopen(url)
                content = result.read()
                result = json.loads(content)
                if result['status'] != 'OVER_QUERY_LIMIT':
                    geocode  = result

            except Exception, e:
                #todo
                pass

            for place in self.entities['places']:
                place['geocode'] = geocode
