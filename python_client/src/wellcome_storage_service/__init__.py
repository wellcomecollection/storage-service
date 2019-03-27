# -*- encoding: utf-8

import functools

from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

from .exceptions import BagNotFound, IngestNotFound, ServerError, UserError


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
        if not self.sess.token:
            self.sess.fetch_token(
                token_url=self.token_url,
                client_id=self.client_id,
                client_secret=self.client_secret
            )
        return f(self, *args, **kwargs)

    return wrapper


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
        c = StorageServiceClient(api_url=api_url, sess=sess)
        c.token_url = token_url
        c.client_id = client_id
        c.client_secret = client_secret
        return c

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
    @check_api_resp
    def get_ingest(self, ingest_id):
        """
        Query the state of an individual ingest.
        """
        ingests_api_url = self.api_url + "/ingests/%s" % ingest_id
        resp = self.sess.get(ingests_api_url)

        if resp.status_code == 404:
            raise IngestNotFound("Ingests API returned 404 for ingest %s" % ingest_id)
        else:
            return resp

    @needs_token
    def get_ingest_from_location(self, ingest_url):
        """
        Given a URL of the form /ingests/{ingest_id}, query the state of
        that ingest.
        """
        parts = ingest_url.split("/")
        assert parts[-2] == "ingests", "Is %s an ingest URL?" % ingest_url
        return self.get_ingest(ingest_id=parts[-1])

    @needs_token
    def create_s3_ingest(self, space_id, s3_bucket, s3_key, callback_url=None):
        """
        Create an ingest from an object in an S3 bucket.

        Returns the location of the new ingest if created, or raises an exception
        if not.
        """
        payload = {
            "type": "Ingest",
            "ingestType": {"id": "create", "type": "IngestType"},
            "space": {"id": space_id, "type": "Space"},
            "sourceLocation": {
                "type": "Location",
                "provider": {"type": "Provider", "id": "aws-s3-standard"},
                "bucket": s3_bucket,
                "path": s3_key,
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
    def get_bag(self, space_id, source_id):
        """
        Get an individual bag.
        """
        bags_api_url = self.api_url + "/bags/%s/%s" % (space_id, source_id)
        resp = self.sess.get(bags_api_url)

        if resp.status_code == 404:
            raise BagNotFound(
                "Bags API returned 404 for bag %s/%s" % (space_id, source_id)
            )
        else:
            return resp
