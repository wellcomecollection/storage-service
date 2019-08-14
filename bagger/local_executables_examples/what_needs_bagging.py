#!/usr/bin/env python
# -*- encoding: utf-8
"""
Usage:

    python what_needs_bagging.py > b_numbers.txt

"""

import collections
import logging
import re

import boto3
import daiquiri


BAGGER_DROP_RE = re.compile(r"^b(?P<b_number>\d{7}[\dx]).*\.tar\.gz$")

B_NUMBER_RE = re.compile(r"[bB](?P<b_number>\d{7}[\dx]?)")


daiquiri.setup(
    level=logging.INFO,
    outputs=[
        daiquiri.output.Stream(formatter=daiquiri.formatter.ColorFormatter(
            fmt="%(asctime)s.%(msecs)03d %(color)s[%(levelname)s] %(message)s%(color_stop)s",
            datefmt="%H:%M:%S"
        ))
    ]
)

logger = daiquiri.getLogger(__name__)


def get_matching_s3_objects(bucket, prefix=""):
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    kwargs = {'Bucket': bucket}

    if prefix:
        kwargs["Prefix"] = prefix

    for page in paginator.paginate(**kwargs):
        try:
            contents = page["Contents"]
        except KeyError:
            return

        yield from contents


if __name__ == "__main__":
    entries = collections.defaultdict(
        lambda: {"bagger_obj": None, "mets_files": []}
    )

    for s3_obj in get_matching_s3_objects(
        bucket="wellcomecollection-assets-workingstorage",
        prefix="mets_only"
    ):
        key = s3_obj["Key"]
        if key.endswith("/"):
            continue

        if key.endswith("/.DS_Store"):
            continue

        if not key.endswith(".xml"):
            logger.warn("Non XML S3 object: %s", key)
            continue

        try:
            b_number = B_NUMBER_RE.search(key).group("b_number")
        except AttributeError:
            logger.warn("Cannot extract b number from key: %s", key)
            continue

        entries[b_number]["mets_files"].append(s3_obj)

    logger.info("Fetched all objects from wc-assets-workingstorage")

    for s3_obj in get_matching_s3_objects(
        bucket="wellcomecollection-storage-bagger-drop"
    ):
        b_number = BAGGER_DROP_RE.match(s3_obj["Key"]).group("b_number")
        entries[b_number]["bagger_obj"] = s3_obj

    logger.info("Fetched all objects from wc-storage-bagger-drop")

    for b_number, data in entries.items():
        try:
            last_bagged_date = data["bagger_obj"]["LastModified"]
        except TypeError:
            last_bagged_date = None

        try:
            last_mets_update = max(
                entry["LastModified"] for entry in data["mets_files"]
            )
        except ValueError:
            logger.warn("Cannot find any source METS files for %s", b_number)
            continue

        logger.info("%s:\n  Last METS update: %s\n  Last bagged:      %s" % (
            b_number,
            last_mets_update,
            last_bagged_date
        ))

        # We want to flag something for rebagging if:
        #
        #   * it's never been bagged
        #   * there's an update to the METS file that might not have been bagged
        #     (remember it can take a day or so from requesting a bag to the bag
        #     actually being created)
        if last_bagged_date is None or (last_bagged_date - last_mets_update).days < 7:
            print(b_number)
