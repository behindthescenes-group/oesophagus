from datetime import datetime
from elasticsearch import Elasticsearch


class ESLoader:
    es = None

    def __init__(self, host):
        self.host = host

    def connect(self):
        self.es = Elasticsearch(host=self.host)

    def load(self, record, index_name):
        response = self.es.index(
            index=index_name, id=record['id'], body=record)
        return response

if __name__ == "__main__":
    pass