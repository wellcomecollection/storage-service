# -*- encoding: utf-8


class StorageServiceClient:
    """
    Client for the Wellcome Storage Service API.
    """

    def __init__(self, api_url, sess=None):
        self.api_url = api_url
        self.sess = sess

    @classmethod
    def with_oauth(self, token_url, client_id, client_secret):
        sess =