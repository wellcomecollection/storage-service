# -*- encoding: utf-8

import functools
import json
import time

from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

from .downloader import download_bag, download_compressed_bag
from .exceptions import BagNotFound, IngestNotFound, ServerError, UserError


__all__ = [
    "download_bag",
    "download_compressed_bag",
    "BagNotFound",
    "IngestNotFound",
    "ServerError",
    "UserError",
    "StorageServiceClient",
]


class StorageServiceClientBase(object):
    """
    Client for the Wellcome Storage Service API.
    """
    def __init__(self, api_url):
        self.api_url = api_url

    def _http_get(self, url):  # pragma: no cover
        """
        Make a GET request to the URL.  Returns a status code and a body (bytes).
        """
        raise NotImplementedError

    def _http_post(self, url, json):  # pragma: no cover
        """
        Make a POST request with a given JSON body.

        Returns a status code, headers and a body (bytes).
        pass
        """
        raise NotImplementedError

    def get_ingest_from_location(self, ingest_url):
        """
        Returns the state of an ingest.
        """
        status_code, body = self._http_get(ingest_url)

        if status_code == 404:
            raise IngestNotFound("Ingests API returned 404 for %s" % ingest_url)
        elif 400 <= status_code < 500:
            error = json.loads(body)
            raise UserError("%s: %s" % (error["label"], error["description"]))
        elif status_code != 200:
            raise ServerError()
        else:
            return json.loads(body)

    def get_ingest(self, ingest_id):
        """
        Returns the state of an ingest.
        """
        return self.get_ingest_from_location(
            ingest_url="%s/ingests/%s" % (self.api_url, ingest_id)
        )

    def get_bag(self, space, external_identifier, version=None):
        """
        Returns the contents of a bag.
        """
        bags_url_suffix = "%s/%s" % (space, external_identifier)
        if version:
            bags_url_suffix += "?version=%s" % version

        bags_url = "%s/bags/%s" % (self.api_url, bags_url_suffix)

        status_code, body = self._http_get(bags_url)
        if status_code == 404:
            if version:
                raise BagNotFound("Bags API returned 404 for bag %s/%s with version %s" % (space, external_identifier, version))
            else:
                raise BagNotFound("Bags API returned 404 for bag %s/%s" % (space, external_identifier))
        else:
            return json.loads(body)

    def create_s3_ingest(
        self,
        space,
        external_identifier,
        s3_bucket,
        s3_key,
        callback_url=None,
        ingest_type="create",
    ):
        """
        Create an ingest from an object in an S3 bucket.

        Returns the location of the new ingest if created, or raises an exception
        if not.
        """
        payload = {
            "type": "Ingest",
            "ingestType": {"id": ingest_type, "type": "IngestType"},
            "space": {"id": space, "type": "Space"},
            "sourceLocation": {
                "type": "Location",
                "provider": {"type": "Provider", "id": "amazon-s3"},
                "bucket": s3_bucket,
                "path": s3_key,
            },
            "bag": {
                "type": "Bag",
                "info": {"type": "BagInfo", "externalIdentifier": external_identifier},
            },
        }

        if callback_url is not None:
            payload["callback"] = {"type": "Callback", "url": callback_url}

        status_code, headers, body = self._http_post(
            url=self.api_url + "/ingests", json=payload
        )

        if 400 <= status_code < 500:
            error = json.loads(body)
            raise UserError("%s: %s" % (error["label"], error["description"]))
        elif status_code == 201:
            return headers["Location"]
        # This branch is untested because it needs a reliable way to trigger
        else:  # pragma: no cover
            raise ServerError()



class RequestsStorageServiceClient(StorageServiceClientBase):
    def __init__(self, api_url, sess):
        self.sess = sess

        super(RequestsStorageServiceClient, self).__init__(api_url=api_url)

    def _http_get(self, url):
        resp = self.sess.get(url)
        return (resp.status_code, resp.text)

    def _http_post(self, url, json):
        resp = self.sess.post(url, json=json)
        return (resp.status_code, resp.headers, resp.text)


def needs_token(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        # We need to refresh the token if:
        #   1. We've never asked for a token before
        #   2. The existing token is close to expiry
        #
        if not self.sess.token or (time.time() - 10 > self.sess.token["expires_at"]):
            self.sess.fetch_token(
                token_url=self.token_url,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
        return f(self, *args, **kwargs)

    return wrapper


class RequestsOAuthStorageServiceClient(RequestsStorageServiceClient):
    def __init__(self, api_url, client_id, client_secret, token_url):
        self.api_url = api_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = token_url

        client = BackendApplicationClient(client_id=client_id)
        sess = OAuth2Session(client=client)

        super(RequestsOAuthStorageServiceClient, self).__init__(api_url=api_url, sess=sess)

    @needs_token
    def _http_get(self, url):
        return super(RequestsOAuthStorageServiceClient, self)._http_get(url)

    @needs_token
    def _http_post(self, url, json):
        return super(RequestsOAuthStorageServiceClient, self)._http_post(url, json)
