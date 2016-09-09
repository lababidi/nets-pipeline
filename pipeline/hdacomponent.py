from basecomponent import BaseComponent
import yaml
from elasticsearch import Elasticsearch,ElasticsearchException
import ahocorasick

class hda(BaseComponent):

    def initialize(self, parameters):
        self.ah  = ahocorasick.Automaton()
        self.categories = None
        es = None
        try:
            es = Elasticsearch(hosts=[ {'host': parameters['elasticsearch']['host'], 'port': parameters['elasticsearch']['port']}])
            result = es.get(index='nets-hda', doc_type='hda', id='en')
            self.categories = result['_source']['categories']
        except ElasticsearchException:
            print("Elasticsearch is not available.")
            exit(0)

        for category in self.categories:
            key = category['key']
            for word in category['words']:
                w = word['source']
                self.ah.add_word(w,(key,w))

        self.ah.make_automaton()


    def eval(self,text ):
        categories = {}

        #todo - review options for finding wildcards, trailing/leading spaces, etc.
        for item in self.ah.iter(text,ahocorasick.MATCH_EXACT_LENGTH):
            key = str(item[1][0])
            w = item[1][1]
            category = None
            if key in categories:
                category = categories[key]
            else:
                category = []
                categories[key] = category
            category.append(w)


        hda = []
        for key in categories:
            #todo  this conversion could  be a lot more efficient.  Store self.categories as a map by key
            for cat  in self.categories:
                if cat['key'] == key:
                    category = cat
                    category['words'].clear()
                    for w in categories[key] :
                        if w not in category['words']: category['words'].append(w)
                    hda.append(category)
                    break
        return hda

    def process(self, articles):
        for article in articles:
            article['hda '] = self.eval( article['content'])
        return articles


# with open('nets.yaml') as f:
#     parameters = yaml.load(f)
# me = hda( parameters )
# me.eval('high frequency antenna might be a crypto thing')