#!/usr/bin/env python3
"""
Look up an bag.  Usage:

    python ss_get_bag.py <SPACE> <EXTERNAL_IDENTIFIER> [<VERSION>]

The script will check both APIs for the ingest ID.

"""

import json
import logging
import sys

from wellcome_storage_service import BagNotFound, staging_client, prod_client

from common import get_logger


logger = get_logger(__name__)


def lookup_bag(space, external_identifier, version):
    logger.debug("Looking up bag %s/%s", space, external_identifier)

    api_variants = {"stage": staging_client(), "prod": prod_client()}

    for name, client in api_variants.items():
        logging.debug("Checking %s API", name)

        try:
            bag = client.get_bag(space, external_identifier, version)
        except BagNotFound:
            logging.debug("Not found in %s API", name)
        else:
            logging.debug("Found bag in %s API:", name)
            return bag

    logging.error("Could not find %s/%s in either API!", space, external_identifier)
    sys.exit(1)


if __name__ == "__main__":
    try:
        space = sys.argv[1]
    except IndexError:
        sys.exit(f"Usage: {__file__} <SPACE> <EXTERNAL_IDENTIFIER> [<VERSION>]")

    try:
        external_identifier = sys.argv[2]
    except IndexError:
        sys.exit(f"Usage: {__file__} <SPACE> <EXTERNAL_IDENTIFIER> [<VERSION>]")

    try:
        version = sys.argv[3]
    except IndexError:
        version = None

    bag = lookup_bag(space, external_identifier, version)

    print(json.dumps(bag, indent=2, sort_keys=True))
