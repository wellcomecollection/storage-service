#!/usr/bin/env python
# -*- encoding: utf-8
"""
Start an ingest.  Usage:

    python3 ss_start_ingest.py <FILENAME>

The script will look in the bagger drop buckets to find the file,
and automatically send it to the correct API.

"""

import getpass
import json
import os
import re
import sys

from botocore.exceptions import ClientError
import click

from common import get_read_only_aws_resource, get_logger, get_storage_client


logger = get_logger(__name__)


if __name__ == "__main__":
    try:
        filename = sys.argv[1]
    except IndexError:
        sys.exit(f"Usage: {__file__} <FILENAME>")

    logger.debug("Creating ingest for file %s", filename)

    s3 = get_read_only_aws_resource("s3")

    buckets = {
        "prod": "wellcomecollection-storage-bagger-drop",
        "stage": "wellcomecollection-storage-staging-bagger-drop",
    }

    for api_name, bucket_name in buckets.items():
        try:
            s3.Object(bucket_name, filename).load()
        except ClientError as err:
            if (
                str(err)
                == "An error occurred (404) when calling the HeadObject operation: Not Found"
            ):
                logger.debug("Didn't find object in bucket %s", bucket_name)
                continue
            else:
                raise
        else:
            api = api_name
            break

    try:
        logger.info("Using %s API", api)
        logger.info("Detected bucket as %s", buckets[api])
    except NameError:
        logger.error(
            "Could not find object in buckets! %s", ", ".join(buckets.values())
        )
        sys.exit(1)

    ext_id_match = re.search(r"b[0-9]{7}[0-9x]", filename)
    if ext_id_match is None:
        logger.error("Could not detect external identifier!")
        sys.exit(1)
    else:
        external_identifier = ext_id_match.group(0)
        logger.info("Detected external identifier as %s", external_identifier)

    creds_path = os.path.join(
        os.environ["HOME"], ".wellcome-storage", "oauth-credentials.json"
    )
    oauth_creds = json.load(open(creds_path))

    space_id = "digitised"
    space_id = click.prompt("Storage space?", default=space_id)
    logger.info("Using storage space %s", space_id)

    logger.info("Making request to storage service")

    host = "api" if api == "prod" else "api-stage"
    api_url = f"https://{host}.wellcomecollection.org/storage/v1"

    client = get_storage_client(api_url=api_url)

    location = client.create_s3_ingest(
        space_id=space_id,
        s3_bucket=buckets[api],
        s3_key=filename,
        external_identifier=external_identifier,
    )

    logger.debug("Ingest created at URL %s", location)
    logger.info("Ingest has ID %s", location.split("/")[-1])
    logger.info(
        "To look up the ingest:\n\n\tpython3 ss_get_ingest.py %s",
        location.split("/")[-1],
    )
