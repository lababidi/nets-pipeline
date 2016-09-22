import logging

from elasticsearch import Elasticsearch
from elasticsearch import ElasticsearchException

logger = logging.getLogger("elasticsearch")


class ElasticClient:
    def __init__(self, host: str, port: int):
        try:
            self.es = Elasticsearch(hosts=[
                {'host': host,
                 'port': port}])
            info = self.es.info()
            logger.info("Connected to Elasticsearch v. %s, name: %s" % (info['version']['number'], info['name']))

        except ElasticsearchException as e:
            logger.error("Elasticsearch is not available.", e)
            exit(0)

    def get_articles(self, index, doctype, batch_size):
        query = '{"query": { "bool": { "must_not": { "exists": { "field": "status" }}}}}'
        result = self.es.search(index=index, doc_type=doctype, size=batch_size, body=query)
        articles = result.get('hits').get('hits')
        return articles if articles is not None else []

    def count(self, index):
        return self.es.count(index=index)['count']

    def info(self):
        return self.es.info()

    def check_url(self, url: str, auth_index: str):
        """
        Private function to check if a URL appears in the database.

        Parameters
        ----------

        url: URL for the news stories to be scraped.

        auth_index: es index

        Returns
        -------

        found: Boolean.
                Indicates whether or not a URL was found in the database.
        """
        response = self.es.search(index=auth_index, doc_type=auth_index, body={
            "query":
                {
                    "match_phrase": {
                        "url": url
                    }
                }
        }, size=0, terminate_after=1, ignore_unavailable=True)

        return response["hits"]["total"] > 0

    def persist(self, index, doctype, payload):
        self.es.index(index=index, doc_type=doctype, body=payload)

    def update(self, index, doctype, doc_id, payload):
        self.es.update(index=index, doc_type=doctype, id=doc_id, body=payload)


event_mapping = {
    "event": {
        "properties": {
            "language": {"index": "not_analyzed", "type": "string"},
            "publisher": {"type": "string"},
            "content": {"type": "string", "index": "analyzed"},
            "url": {"index": "not_analyzed", "type": "string"},
            "articleid": {"index": "not_analyzed", "type": "string"},
            "title": {"type": "string"},
            "date_collected": {"type": "date"},
            "date_published": {"type": "date"},
            "date_collected_as_date_publshed": {"type": "boolean"},

            "people": {"index": "not_analyzed", "type": "string"},
            "places": {"index": "not_analyzed", "type": "string"},
            "dates": {"index": "not_analyzed", "type": "string"},
            "times": {"index": "not_analyzed", "type": "string"},
            "organizations": {"index": "not_analyzed", "type": "string"},
            "languages": {"index": "not_analyzed", "type": "string"},
            "events": {"index": "not_analyzed", "type": "string"},
            "other": {"index": "not_analyzed", "type": "string"}
        }
    }
}
