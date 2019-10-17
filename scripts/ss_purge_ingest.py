#!/usr/bin/env python
# -*- encoding: utf-8

import concurrent.futures
import json
import sys

import tqdm
from wellcome_storage_service import IngestNotFound

from common import get_aws_resource, get_logger
from ss_get_ingest import lookup_ingest

logger = get_logger(__name__)

import boto3


s3 = boto3.resource("s3").meta.client

dynamodb = boto3.resource("dynamodb").meta.client

def delete_s3_object(bucket, key):
    logger.debug("Deleting s3://%s/%s" % (bucket, key))
    s3.delete_object(Bucket=bucket, Key=key)


def delete_s3_prefix(bucket, prefix, known_keys):
    if known_keys:
        # Clean up all the keys we known about
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(delete_s3_object, bucket, key) for key in known_keys
            ]

            for fut in tqdm.tqdm(
                concurrent.futures.as_completed(futures), total=len(futures)
            ):
                fut.result()

    # Now clean up any left-over keys.
    for key in get_matching_s3_keys(bucket, prefix):
        delete_s3_object(bucket, key)


def delete_dynamodb_row(table, key):
    logger.debug("Deleting dynamodb://%s/%s" % (table, key))
    dynamodb.delete_item(TableName=table, Key=key)


def get_matching_s3_objects(bucket, prefix="", suffix=""):
    """
    Generate objects in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    paginator = s3.get_paginator("list_objects_v2")

    kwargs = {"Bucket": bucket}

    # We can pass the prefix directly to the S3 API.  If the user has passed
    # a tuple or list of prefixes, we go through them one by one.
    if isinstance(prefix, str):
        prefixes = (prefix,)
    else:
        prefixes = prefix

    for key_prefix in prefixes:
        kwargs["Prefix"] = key_prefix

        for page in paginator.paginate(**kwargs):
            try:
                contents = page["Contents"]
            except KeyError:
                return

            for obj in contents:
                key = obj["Key"]
                if key.endswith(suffix):
                    yield obj


def get_matching_s3_keys(bucket, prefix="", suffix=""):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    for obj in get_matching_s3_objects(bucket, prefix, suffix):
        yield obj["Key"]


def purge_ingest(ingest_id):
    logger.info("Purging ingest %s", ingest_id)

    logger.info("Looking up ingest data")
    try:
        ingest = lookup_ingest(ingest_id)
    except IngestNotFound:
        logger.error("Could not find ingest %s", ingest_id)
        return
    else:
        logger.info("Successfully retrieved ingest info!")

    external_identifier = ingest["bag"]["info"]["externalIdentifier"]
    logger.info("Detected external identifier as %s", external_identifier)

    try:
        version = ingest["bag"]["info"]["version"]
    except KeyError:
        logger.warning("Could not detect ingest version")
        version = None
    else:
        logger.info("Detected ingest version as %s", version)

    if version is None:
        logger.info("Not looking up bag register entry")
    else:
        logger.info("Looking up storage manifest in bag register table")

        resp = dynamodb.get_item(
            TableName="vhs-storage-manifests",
            Key={
                "id": f"digitised:{external_identifier}",
                "version": int(version.replace("v", "")),
            },
        )

        try:
            item = resp["Item"]

            s3_data = s3.get_object(
                Bucket=item["payload"]["typedStoreId"]["namespace"],
                Key=item["payload"]["typedStoreId"]["path"],
            )["Body"]

            storage_manifest = json.load(s3_data)
            logger.info("Retrieved storage manifest")

            paths = [f["path"] for f in storage_manifest["manifest"]["files"]] + [
                f["path"] for f in storage_manifest["tagManifest"]["files"]
            ]
        except KeyError:
            logger.info("No storage manifest")

            paths = []

        # We don't want to delete keys in another prefix!
        assert all(p.startswith(version + "/") for p in paths)

        logger.warning("Deleting primary replica files")
        delete_s3_prefix(
            bucket="wellcomecollection-storage",
            prefix="digitised/%s/%s" % (external_identifier, version),
            known_keys=["digitised/%s/%s" % (external_identifier, p) for p in paths],
        )

        logger.warning("Deleting Glacier replica files")
        delete_s3_prefix(
            bucket="wellcomecollection-storage-replica-ireland",
            prefix="digitised/%s/%s" % (external_identifier, version),
            known_keys=["digitised/%s/%s" % (external_identifier, p) for p in paths],
        )

        logger.warning("Deleting replica records")
        delete_dynamodb_row(
            table="storage_replicas_table",
            key={"id": "digitised/%s/%s" % (external_identifier, version)},
        )

        try:
            logger.warning("Deleting bag register entry")
            delete_s3_object(
                bucket=item["payload"]["typedStoreId"]["namespace"],
                key=item["payload"]["typedStoreId"]["path"],
            )

            delete_dynamodb_row(
                table="vhs-storage-manifests",
                key={
                    "id": f"digitised:{external_identifier}",
                    "version": int(version.replace("v", "")),
                },
            )
        except NameError:
            pass

        logger.warning("Deleting bag versioner entry")
        delete_dynamodb_row(
            table="storage_versioner_versions_table",
            key={
                "id": f"digitised:{external_identifier}",
                "version": int(version.replace("v", "")),
            },
        )

    logger.warning("Deleting ingest records")
    delete_dynamodb_row(table="storage-ingests", key={"id": ingest_id})


if __name__ == "__main__":
    for ingest_id in sys.argv[1:]:
        purge_ingest(ingest_id)
