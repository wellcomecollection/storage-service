#!/usr/bin/env python
"""
This is the skeleton of a script that can be used to add a replica in Azure to
bags that only have replicas in S3 and Glacier.

Part of https://github.com/wellcomecollection/platform/issues/4640
"""

import datetime
import json
from pprint import pprint
import re

from common import get_aws_client, get_aws_resource, scan_table, READ_ONLY_ROLE_ARN


dynamo_resource = get_aws_resource("dynamodb", role_arn=READ_ONLY_ROLE_ARN).meta.client
s3_client = get_aws_client("s3", role_arn=READ_ONLY_ROLE_ARN)


def find_incomplete_replicas(replicas_table):
    """
    Find all the bags that don't have a replica in Azure.
    """
    for item in scan_table(TableName=replicas_table):

        # Check the structure is sensible: we expect an S3 primary replica,
        # and one S3 secondary replica and (optionally) an Azure replica,
        # depending on whether this bag has already been replicated to Azure.
        #
        # Flag any replica records that don't match this structure; something
        # weird is going on.
        try:
            assert item["payload"]["location"].keys() == {"PrimaryS3ReplicaLocation"}
            assert item["payload"]["replicas"][0].keys() == {
                "SecondaryS3ReplicaLocation"
            }
            assert 1 <= len(item["payload"]["replicas"]) <= 2

            if len(item["payload"]["replicas"]) == 2:
                assert item["payload"]["replicas"][1].keys() == {
                    "SecondaryAzureReplicaLocation"
                }
        except AssertionError:
            pprint(item)
            raise

        # If the bag has two replicas, we know one of them is in Glacier and
        # one is in Azure, so it's complete.
        if len(item["payload"]["replicas"]) == 2:
            continue

        yield item


def find_ingest_id(manifests_table, *, space, externalIdentifier, version):
    """
    Find the ingest ID that corresponds to this big.
    """
    # First look up the corresponding storage manifest.
    manifest_pointer = dynamo_resource.get_item(
        TableName=manifests_table,
        Key={"id": f"{space}/{externalIdentifier}", "version": int(version)},
    )["Item"]

    bucket = manifest_pointer["payload"]["namespace"]
    key = manifest_pointer["payload"]["path"]

    # Now we look up the manifest in S3.  We could download the whole manifest
    # and parse it, but that's pretty expensive.  So we cheat: the JSON encoder
    # that writes storage manifests usually writes the ingestId at the end of
    # the JSON string.
    #
    # We'll read the last few bytes first and look for an ingest ID; only if we
    # can't find it do we try to parse the whole manifest.
    #
    try:
        size = s3_client.head_object(Bucket=bucket, Key=key)["ContentLength"]
        s3_fragment = (
            s3_client.get_object(Bucket=bucket, Key=key, Range=f"bytes={size - 51}-")[
                "Body"
            ]
            .read()
            .decode("utf8")
        )

        return re.match(r'^"ingestId": "([0-9a-f-]{36})"}$', s3_fragment).group(1)
    except Exception:
        s3_body = s3_client.get_object(Bucket=bucket, Key=key)["Body"]
        manifest = json.load(s3_body)
        return manifest["ingestId"]


def get_ingest(ingest_id, *, ingests_table):
    return dynamo_resource.get_item(TableName=ingests_table, Key={"id": ingest_id})[
        "Item"
    ]


def create_payload(replica_record, *, ingests_table, manifests_table):
    # The ID will be of the form {space}/{externalIdentifier}/{version}
    space, _remainder = replica_record["id"].split("/", 1)
    externalIdentifier, version = _remainder.rsplit("/", 1)

    version = int(re.match(r"^v([0-9]+)$", version).group(1))

    ingest_id = find_ingest_id(
        manifests_table,
        space=space,
        externalIdentifier=externalIdentifier,
        version=version,
    )

    ingest = get_ingest(ingest_id, ingests_table=ingests_table)

    context = {
        "ingestId": ingest["id"],
        "ingestType": {"id": ingest["payload"]["ingestType"]},
        "storageSpace": ingest["payload"]["space"],
        "externalIdentifier": ingest["payload"]["externalIdentifier"],
        # This is a bit of a kludge -- the timestamp in the ingests table uses
        # millisecond precision, but we throw away the millisecond portion here.
        # In practice, only the bag versioner looks at this field, so a bit of
        # loss is no big deal.
        "ingestDate": datetime.datetime.fromtimestamp(
            int(ingest["payload"]["createdDate"] / 1000)
        ).isoformat()
        + "Z",
    }

    bagRoot = replica_record["payload"]["location"]["PrimaryS3ReplicaLocation"][
        "prefix"
    ]

    return {
        "context": context,
        "bagRoot": bagRoot,
        "version": version,
        "type": "VersionedBagRootPayload",
    }


if __name__ == "__main__":
    config = {
        "stage": {
            "replicas_table": "storage-staging_replicas_table",
            "manifests_table": "vhs-storage-staging-manifests-2020-07-24",
            "ingests_table": "storage-staging-ingests",
        },
    }

    replicas_table = config["stage"]["replicas_table"]
    manifests_table = config["stage"]["manifests_table"]
    ingests_table = config["stage"]["ingests_table"]

    for replica_record in find_incomplete_replicas(replicas_table):
        payload = create_payload(
            replica_record, manifests_table=manifests_table, ingests_table=ingests_table
        )
        print(json.dumps(payload, indent=2))
        break
