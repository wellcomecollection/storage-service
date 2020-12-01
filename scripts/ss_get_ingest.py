#!/usr/bin/env python3
"""
Get a response from the ingests API.  Usage:

    python ss_get_ingest.py <INGEST_ID>

The script will attempt to find the ingest ID in both the prod and staging APIs.

For most use cases, you can use the web inspector:
https://wellcome-ingest-inspector.glitch.me/

This script is useful if you need to see the raw JSON response direct from
the ingests API.  We don't expose the raw JSON online -- it's publicly visible,
and we don't want callback URLs visible.

"""

import json
import sys

from wellcome_storage_service import IngestNotFound, RequestsOAuthStorageServiceClient


def lookup_ingest(ingest_id):
    api_variants = {"stage": "api-stage", "prod": "api"}

    for name, host in api_variants.items():
        api_url = f"https://{host}.wellcomecollection.org/storage/v1"
        client = RequestsOAuthStorageServiceClient.from_path(api_url=api_url)

        try:
            return name, client.get_ingest(ingest_id)
        except IngestNotFound:
            pass

    sys.exit(f"Could not find {ingest_id} in either API!")


if __name__ == "__main__":
    try:
        ingest_id = sys.argv[1]
    except IndexError:
        sys.exit(f"Usage: {__file__} <INGEST_ID>")

    ingest = lookup_ingest(ingest_id)
    print(json.dumps(ingest, indent=2, sort_keys=True))
