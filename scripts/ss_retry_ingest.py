#!/usr/bin/env python
# -*- encoding: utf-8
"""
Retry an ingest with the same parameters.  Usage:

    python ss_retry_ingest.py <INGEST_ID>

"""

import sys

from common import get_logger, get_storage_client
from ss_get_ingest import lookup_ingest


logger = get_logger(__name__)


if __name__ == "__main__":
    try:
        ingest_id = sys.argv[1]
    except IndexError:
        sys.exit(f"Usage: {__file__} <INGEST_ID>")

    ingest = lookup_ingest(ingest_id)

    if "api-stage" in ingest["@context"]:
        api_url = "https://api-stage.wellcomecollection.org/storage/v1"
    else:
        api_url = "https://api.wellcomecollection.org/storage/v1"

    client = get_storage_client(api_url)

    location = client.create_s3_ingest(
        space_id=ingest["space"]["id"],
        s3_bucket=ingest["sourceLocation"]["bucket"],
        s3_key=ingest["sourceLocation"]["path"],
        external_identifier=ingest["bag"]["info"]["externalIdentifier"],
    )

    logger.info("Ingest created at URL %s", location)
    logger.info("Ingest has ID %s", location.split("/")[-1])
    logger.info(
        "To look up the ingest:\n\n\tpython ss_get_ingest.py %s",
        location.split("/")[-1],
    )
