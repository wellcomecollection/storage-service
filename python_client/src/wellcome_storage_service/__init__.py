# -*- encoding: utf-8

import functools
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


def raise_for_status(resp):
    if resp.status_code >= 500:
        raise ServerError("Unexpected error from storage service")
    elif resp.status_code >= 400:
        raise UserError(
            "Storage service reported a user error: %s" % resp.json()["description"]
        )


def check_api_resp(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        resp = f(*args, **kwargs)
        raise_for_status(resp)
        return resp.json()

    return wrapper


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


class StorageServiceClient:
    """
    Client for the Wellcome Storage Service API.
    """

    def __init__(self, api_url, client_id, client_secret, token_url):
        self.api_url = api_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = token_url

        client = BackendApplicationClient(client_id=client_id)
        self.sess = OAuth2Session(client=client)

    @needs_token
    @check_api_resp
    def get_space(self, space_id):
        """
        Get information about a named space.
        """
        # This isn't implemented in the storage service yet, so we can't
        # write a client handler for it!
        raise NotImplementedError

    @needs_token
    def get_ingest(self, ingest_id):
        """
        Query the state of an individual ingest.
        """
        ingest_url = self.api_url + "/ingests/%s" % ingest_id
        return self.get_ingest_from_location(ingest_url)

    @needs_token
    @check_api_resp
    def get_ingest_from_location(self, ingest_url):
        """
        Given a URL of the form /ingests/{ingest_id}, query the state of
        that ingest.
        """
        resp = self.sess.get(ingest_url)

        if resp.status_code == 404:
            raise IngestNotFound("Ingests API returned 404 for %s" % ingest_url)
        else:
            return resp

    @needs_token
    def create_s3_ingest(
        self,
        space_id,
        s3_bucket,
        s3_key,
        external_identifier,
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
            "space": {"id": space_id, "type": "Space"},
            "sourceLocation": {
                "type": "Location",
                "provider": {"type": "Provider", "id": "aws-s3-standard"},
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

        ingests_api_url = self.api_url + "/ingests"
        resp = self.sess.post(ingests_api_url, json=payload)

        raise_for_status(resp)

        return resp.headers["Location"]

    @needs_token
    @check_api_resp
    def get_bag(self, space_id, source_id, version=None):
        """
        Get an individual bag.
        """
        bags_url_suffix = "%s/%s" % (space_id, source_id)
        params = {}
        if version:
            params["version"] = version

        bags_api_url = self.api_url + "/bags/%s" % bags_url_suffix
        resp = self.sess.get(bags_api_url, params=params)

        if resp.status_code == 404:
            err = "Bags API returned 404 for bag %s" % bags_url_suffix
            if version:
                err += " with version %s" % version
            raise BagNotFound(err)
        else:
            return resp
