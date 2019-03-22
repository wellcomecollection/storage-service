# -*- encoding: utf-8

from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

from .exceptions import IngestNotFound


class StorageServiceClient:
    """
    Client for the Wellcome Storage Service API.
    """

    def __init__(self, api_url, sess):
        self.api_url = api_url
        self.sess = sess

    @classmethod
    def with_oauth(self, api_url, client_id, token_url, client_secret):
        client = BackendApplicationClient(client_id=client_id)
        sess = OAuth2Session(client=client)
        sess.fetch_token(
            token_url=token_url,
            client_id=client_id,
            client_secret=client_secret
        )
        return StorageServiceClient(api_url=api_url, sess=sess)

    def get_ingest(self, ingest_id):
        """
        Query the state of an individual ingest.
        """
        ingests_api_url = self.api_url + "/ingests/%s" % ingest_id
        resp = self.sess.get(ingests_api_url)

        if resp.status_code == 404:
            raise IngestNotFound("Ingests API returned 404 for ingest %s" % ingest_id)
        else:
            return resp.json()
