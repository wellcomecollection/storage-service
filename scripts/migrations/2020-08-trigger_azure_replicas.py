#!/usr/bin/env python
"""
Sends a message to the Azure replicator requesting a replication of a bag to Azure.

Usage: 2020-08-request_azure_replicas.py (stage|prod)

"""

import datetime
import json
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from _aws import DEV_ROLE_ARN, get_aws_client, get_dynamo_client, scan_table  # noqa
from _azure_backfill import get_bags, has_been_replicated_to_azure  # noqa


dynamodb = get_dynamo_client()


def get_ingest_id_lookup(*, ingests_table):
    """
    Create a lookup (storage manifest ID) -> (ingest ID).

    Although this can be looked up from the storage manifests VHS, it's faster
    to get it from the ingests table.  The result is cached for speed on
    subsequent runs.
    """
    cache_name = f"ingests_cache_{ingests_table}.json"

    try:
        return json.load(open(cache_name))
    except FileNotFoundError:
        ingests_lookup = {}

        for ingest in scan_table(TableName=ingests_table):
            if ingest["payload"]["status"] != "Succeeded":
                continue
            space = ingest["payload"]["space"]
            externalIdentifier = ingest["payload"]["externalIdentifier"]
            version = ingest["payload"]["version"]
            manifest_id = f"{space}/{externalIdentifier}/{version}"

            assert (
                manifest_id not in ingests_lookup
            ), f"Multiple ingests matching manifest ID: {manifest_id}"

            ingests_lookup[manifest_id] = ingest["id"]

        json_string = json.dumps(ingests_lookup)

        with open(cache_name, "w") as outfile:
            outfile.write(json_string)

        return ingests_lookup


def get_bags_not_in_azure(*, vhs_table, backfill_table):
    """
    Generates all the bags in the VHS table that have not been replicated to Azure.
    """
    for space, externalIdentifier, version in get_bags(vhs_table=vhs_table):
        if not has_been_replicated_to_azure(
            backfill_table=backfill_table,
            space=space,
            externalIdentifier=externalIdentifier,
            version=version,
        ):
            yield {
                "space": space,
                "externalIdentifier": externalIdentifier,
                "version": version,
            }


def create_context(ingests_table, *, ingest_id):
    ingest = dynamodb.get_item(TableName=ingests_table, Key={"id": ingest_id})["Item"][
        "payload"
    ]
    return {
        "ingestId": ingest["id"],
        "ingestType": {"id": ingest["ingestType"]},
        "storageSpace": ingest["space"],
        "externalIdentifier": ingest["externalIdentifier"],
        # This is a bit of a kludge -- the timestamp in the ingests table uses
        # millisecond precision, but we throw away the millisecond portion here.
        # In practice, only the bag versioner looks at this field, so a bit of
        # loss is no big deal.
        "ingestDate": datetime.datetime.fromtimestamp(
            int(ingest["createdDate"] / 1000)
        ).isoformat()
        + "Z",
    }


if __name__ == "__main__":
    try:
        env = sys.argv[1]
    except IndexError:
        sys.exit(f"Usage: {__file__} (stage|prod)")

    if env == "stage":
        ingests_table = "storage-staging-ingests"
        vhs_table = "vhs-storage-staging-manifests-2020-07-24"
        backfill_table = "vhs-storage-staging-manifests-2020-08-19"
        primary_bucket = "wellcomecollection-storage-staging"
        azure_replicator_topic = "storage_staging_azure_backfill"
    else:
        assert False, f"Unsupported environment: {env}"

    ingests_lookup = get_ingest_id_lookup(ingests_table=ingests_table)

    sns_client = get_aws_client("sns", role_arn=DEV_ROLE_ARN)

    for bag in get_bags_not_in_azure(
        vhs_table=vhs_table, backfill_table=backfill_table
    ):
        manifest_id = f"{bag['space']}/{bag['externalIdentifier']}/{bag['version']}"
        try:
            ingest_id = ingests_lookup[manifest_id]
        except KeyError:
            print(f"!!! Unable to find ingest ID for {manifest_id}")
            continue

        context = create_context(
            ingests_table="storage-staging-ingests", ingest_id=ingest_id
        )

        payload = {
            "context": context,
            "bagRoot": {
                "bucket": primary_bucket,
                "keyPrefix": f"{bag['space']}/{bag['externalIdentifier']}/v{bag['version']}",
            },
            "version": bag["version"],
            "type": "VersionedBagRootPayload",
        }

        sns_client.publish(
            TopicArn=f"arn:aws:sns:eu-west-1:975596993436:{azure_replicator_topic}",
            Message=json.dumps(payload),
        )

        print(json.dumps(payload, indent=2, sort_keys=True))
        break
