#!/usr/bin/env python
# -*- encoding: utf-8

from ingest_bag import oauth_details_from_file, oauth_session


if __name__ == "__main__":
    details = oauth_details_from_file("oauth_credentials.json")
    sess = oauth_session(**details)
    resp = sess.get("https://api-stage.wellcomecollection.org/storage/v1/ingests/f9da1c55-a37c-47dc-9a20-7dbf999a28f6")
    from pprint import pprint
    pprint(resp.json())