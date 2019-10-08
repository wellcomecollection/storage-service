from contextlib import closing
import requests


class DDSClient:
    def __init__(self, start_ingest_url, item_query_url, connection_pool_size=1):
        self.start_ingest_url = start_ingest_url
        self.item_query_url = item_query_url
        self.session = requests.Session()

    def status(self, bnumber):
        url = self.start_ingest_url.format(bnumber)
        r = self.session.get(url, verify=False)
        r.raise_for_status()

        return r.json()

    def ingest(self, bnumber):
        url = self.start_ingest_url.format(bnumber)
        r = self.session.get(url, verify=False)
        r.raise_for_status()

        return r.json()
