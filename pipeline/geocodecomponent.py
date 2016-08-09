from basecomponent import BaseComponent


class geocoder(BaseComponent):


    def initialize(self, parameters):
        self.dummydata = geocodes = [
            {'name': 'Lima', 'lat': 32.345, 'lon': 45.32, 'adm1' : 'Peru', 'adm2' : 'AN'},
            {'name': 'Stuebensville', 'lat': 32.345, 'lon': 45.32, 'adm1': 'USA', 'adm2': 'OH'}
        ]


    def process(self, articles):

        for article in articles:
            article['geocodes'] = self.dummydata

        return articles

