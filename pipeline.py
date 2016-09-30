import ahocorasick
import json
import logging
import sys
import urllib3
from os import getenv
from os.path import join
from time import sleep
from typing import List
from urllib.parse import quote
import dateutil.parser as date_parser
import parsedatetime.parsedatetime
import spacy
import yaml

from elastic import ElasticClient

logger = logging.getLogger(__name__)
conf = {}


def klass(class_str):
    return getattr(sys.modules[__name__], class_str)


class EventNLP(object):
    def __init__(self):
        self.times = []


class EventArticle:
    def __init__(self, raw_id, content="", url="", title="",
                 language="", source="", publisher="", date_published="", date_collected=""):
        self.nlp = EventNLP()
        self.hda = []
        self.geography = []
        self.entity = {}
        self.geokeys = []
        self.publisher = publisher
        self.source = source
        self.language = language
        self.title = title
        self.url = url
        self.content = content
        self.raw_id = raw_id
        self.date_published = date_published
        self.date_collected = date_collected

    def reformat(self, raw_article):
        print(self.content)
        article = {'raw_article_id': raw_article['_id'],
                   'content': raw_article['_source']['content'],
                   'url': raw_article['_source']['url'],
                   'title': raw_article['_source']['title'],
                   'language': raw_article['_source']['language'],
                   'source': 'NETS',
                   'publisher': raw_article['_source']['publisher']}

        if 'date_collected' in raw_article['_source']:
            article['date_collected'] = raw_article['_source']['date_collected']
        else:
            article['date_collected'] = raw_article['_source']['date_added']['$date']

        if 'date_published' in raw_article['_source']:
            article['date_published'] = raw_article['_source']['date_published']
        else:
            article['date_published'] = raw_article['_source']['date_added']['$date']

        return article

    def es(self):
        return {
            'raw_article_id': self.raw_id,
            'content': self.content,
            'url': self.url,
            'title': self.title,
            'language': self.language,
            'source': 'NETS',
            'publisher': self.publisher,
            'date_published': self.date_published,
            'date_collected': self.date_collected
        }

    # move some things around until we rethink Elasticsearch mappings

    def legacyMapping(self):
        self.places = self.entity['places']
        self.events = self.entity['events']
        self.times  = self.entity['times']
        self.people  = self.entity['people']
        self.dates = self.entity['dates']
        self.other  = self.entity['other']
        self.languages  = self.entity['languages']
        self.organizations = self.entity['organizations']


class BaseSource:
    def fetch(self, n=100) -> List[EventArticle]:
        raise NotImplementedError

    def count(self):
        raise NotImplementedError


class ElasticSource(BaseSource):
    def count(self):
        pass

    def __init__(self, elastic_client: ElasticClient, index, doctype):
        self.index = index
        self.doctype = doctype
        self.es = elastic_client

    def fetch(self, n=100) -> List[EventArticle]:
        event_articles = []
        for article in self.es.get_articles(self.index, self.doctype, n):
#            print(article)
            a = article['_source']
            event_articles.append(EventArticle(article['_id'],
                                               a['content'],
                                               a['url'],
                                               a['title'],
                                               a['language'],
                                               'nets',
                                               a['publisher'],
                                               date_published=a['date_published'],
                                               date_collected=a['date_collected'])
                                  )

        return event_articles


class FileSource(BaseSource):
    def count(self):
        pass

    def fetch(self, n=100) -> List[EventArticle]:
        pass


class BaseComponent:
    def process(self, article: EventArticle):
        raise NotImplementedError


class ElasticPersist(BaseComponent):
    def __init__(self, elastic_client: ElasticClient, enhanced_index=None, enhanced_doctype=None, raw_index=None,
                 raw_doctype=None):
        self.es = elastic_client
        self.raw_doctype = raw_doctype
        self.raw_index = raw_index
        self.enhanced_doctype = enhanced_doctype
        self.enhanced_index = enhanced_index

    def process(self, article: EventArticle):
        # for each article, write out the article and set the status to 1 for the associated raw_article
        article.legacyMapping()
        article.nlp = []
        self.es.persist(self.enhanced_index, self.enhanced_doctype, article.__dict__)
        self.es.update(self.raw_index, self.raw_doctype, doc_id=article.raw_id, payload={"doc": {"status": 1}})
        # self.es.index(index=self.enhanced_index, doc_type=self.enhanced_doctype, body=article.es())
        # self.es.update(index=self.raw_index, doc_type=self.raw_doctype, id=article.raw_id, body={"doc":
        # {"status": 1}})
        return article


class FilePersist(BaseComponent):
    def process(self, article: EventArticle):
        article.legacyMapping()
        path = join(self.destination, article.raw_id + '.json')
        article.nlp = []
        with open(path, 'w') as f:
            json.dump(article.__dict__, f)
        return article

    def __init__(self, destination):
        self.destination = destination


class NLP(BaseComponent):
    def __init__(self):
        super(self.__class__, self).__init__()
        self.nlp = spacy.load("en")
        self.types = {"people", "places", "dates", "times", "organizations", "events", "languages"}

        self.nlpmap = {
            'PERSON': 'people',
            'GPE': 'places',
            'LOC': 'places',
            'FAC': 'places',
            'DATE': 'dates',
            'TIME': 'times',
            'ORG': 'organizations',
            'LANGUAGE': 'languages',
            'EVENT': 'events'
        }

        self.entitymap = {
            'people': 'person',
            'places': 'place',
            'dates': 'nlpdate',
            'times': 'nlptime',
            'organizations': 'organization',
            'languages': 'language',
            'events': 'nlpevent'
        }

        self.cal = parsedatetime.Calendar()

    def bucketlist(self, article, name):
        return article[name] if name in self.types else article['other']

    def process(self, article: EventArticle):

        for ent_type in self.types:
            article.entity[ent_type] = []
        article.entity['other'] = []

        nlp_doc = self.nlp(article.content)

        for entity in nlp_doc.ents:
            ent_type = self.nlpmap.get(entity.label_, 'other')
            item = entity.string.strip()
            if len(item) > 0 and item not in article.entity[ent_type]:
                article.entity[ent_type].append(item)

        times = [ent.string for ent in nlp_doc.ents if ent.label_ == 'DATE' or ent.label_ == 'TIME']
        article.nlp.times = [self.parse_time(time, date_parser.parse(article.date_published)) for time in times]

        return article

    def parse_time(self, time, central_date):
        return self.cal.parseDT(time, central_date)[0].strftime("%Y-%m-%d %H:%M:%S")


class Geocoder(BaseComponent):

    def __init__(self):
        global conf
        super(self.__class__, self).__init__()
        self.NO_FEATURE = -1000
        self.url = conf['geocode']['url']
        self.pool = urllib3.PoolManager()
        self.keys = ['country_gid', 'macroregion_gid', 'region_gid', 'macrocounty_gid', 'county_gid',
                    'localadmin_gid', 'locality_gid',
                    'borough_gid,', 'neighborhood_gid']


    def process(self, article : EventArticle):

        for place in article.entity['places']:
            placeinfo = {'place': place}
            url = "%s?text=%s" % (self.url, quote(place))
            response = self.pool.request('GET', url)
            response.release_conn()
            geocode = json.loads(response.data.decode('utf-8'))

            confidence = 0.0
            findex = 0
            for index, feature in enumerate(geocode['features']):
                if feature['properties']['confidence'] > confidence:
                    confidence = feature['properties']['confidence']
                    findex = index

            if confidence > 0.0:
                feature = geocode['features'][findex]
                ancestry_keys = []
                placeinfo['geometry'] = feature['geometry']
                placeinfo['confidence'] = confidence

                for key in self.keys:
                    if key in feature['properties']:
                        ancestry_keys.append(feature['properties'][key])
                        placeinfo[key] = feature['properties'][key]
                        article.geokeys.append(placeinfo[key])

                if len(ancestry_keys) > 0:
                    placeinfo['ancestry'] = '.'.join(ancestry_keys)
            article.geography.append(placeinfo)
        return article



class HDA(BaseComponent):
    def __init__(self):
        self.ah = ahocorasick.Automaton()
        with open('data/hda_en.json') as f:
            self.categories = json.loads(f.read())

        for category in self.categories:
            # key = category['key']
            label = category['label']
            for word in category['words']:
                w = word['source']
                self.ah.add_word(w, (label, w))

        self.ah.make_automaton()

    def process(self, article: EventArticle):
        categories = {}

        # todo - review options for finding wildcards, trailing/leading spaces, etc.
        for item in self.ah.iter(article.content, ahocorasick.MATCH_EXACT_LENGTH):
            key, w = str(item[1][0]), item[1][1]
            if key not in article.hda:
                categories[key] = []
            categories[key].append(w)

        article.hda = [{"name": key, "words": w} for key, w in categories.items()]
        return article

class Pipeline:
    def __init__(self):

        indices = conf['elasticsearch']['indexes']
        self.raw_index = indices.get('raw').get('name')
        self.raw_doctype = indices.get('raw').get('doctype')
        self.enhanced_index = indices.get('enhanced').get('name')
        self.enhanced_doctype = indices.get('enhanced').get('doctype')

        self.persistdirectory = conf['directories']['persist']
        logger.info("Persist to directory %s", self.persistdirectory)
        # initialize pipeline components

        self.delay = int(conf['pipeline']['delay'])
        self.batch_size = int(conf['pipeline']['batchsize'])

        use_elastic = True

        es = conf['elasticsearch']
        print(conf)

        if use_elastic:
            elastic_client = ElasticClient(es['host'], int(es['port']))
            self.source = ElasticSource(elastic_client, es['indexes']['raw']['name'], es['indexes']['raw']['doctype'])
        else:
            self.source = FileSource()

        self.components = []

        # use klass to convert str to Class
        for name in conf['pipeline']['components']:
            self.components.append(klass(name)())
            logger.info("pipeline component: %s" % name)

        # configuration says persist to file or ElasticSearch

        if 'file' == conf['pipeline']['persistence']:
            self.components.append(
                FilePersist(conf['directories']['persist'])
            )
        else:
            self.components.append(
                ElasticPersist(ElasticClient(conf['elasticsearch']['host'], conf['elasticsearch']['port']),
                               self.enhanced_index, self.enhanced_doctype, self.raw_index, self.raw_index)
        )

    def single(self, article: EventArticle) -> EventArticle:
        for c in self.components:
            # assert isinstance(c, BaseComponent)
            try:
                article = c.process(article)
            except TypeError as e:
                print(article.__dict__, e, c)
        return article

    def process(self):
        articles = self.source.fetch()
        for article in articles:
            print(article.title)
            self.single(article)
        logger.info("%s article processed.  Enhanced article count: %s.".format(len(articles)))
        print("--------Sleep %s" % self.delay)
        sleep(self.delay)


def main(yaml_file='nets.yaml'):
    import spacy.download
    try:
        spacy.download.download('en', False)
    except SystemExit:
        pass
    global conf

    with open(yaml_file) as f:
        conf = yaml.load(f)

    # if environment variables are provided for elasticsearch host and port
    # use those instead

    for key in ['HOST', 'PORT']:
        v = getenv('NETS_ES_%s' % key)
        if v is not None:
            conf['elasticsearch'][key.lower()] = v

    logger.level = {"INFO": logging.INFO, "DEBUG": logging.DEBUG}.get(conf['logging']['level'])
    delay = int(conf['pipeline']['delay'])
    batch_size = int(conf['pipeline']['batchsize'])

    logger.info("Pipeline. Batch size: {}.  Delay: {}".format(batch_size, delay))

    event_pipeline = Pipeline()

    while True:
        event_pipeline.process()


if __name__ == '__main__':
    main()
