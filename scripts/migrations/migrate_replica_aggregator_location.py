#!/usr/bin/env python
"""
This script was used to migrate the replica aggregator table when we changed the
ReplicaLocation model in June/July 2020.

This was part of a wide-reaching piece of work to change the ObjectLocation model:
https://github.com/wellcomecollection/platform/issues/4596

Previously:

    sealed trait StorageLocation {
      val provider: StorageProvider
      val prefix: ObjectLocationPrefix
    }

after:

    sealed trait ReplicaLocation {
      val prefix: Prefix[_ <: Location]
    }

    case class PrimaryS3ReplicaLocation(prefix: S3ObjectLocationPrefix)
        extends ReplicaLocation

    case class SecondaryS3ReplicaLocation(prefix: S3ObjectLocationPrefix)
        extends ReplicaLocation

This script read the DynamoDB table holding the record of replicas, and updated
the model on old replicas.

"""

import copy
import json
from pprint import pprint

import tqdm

from common import get_aws_resource, scan_table, DecimalEncoder


def fix_replicas(item):
    original_version = item["version"]
    new_version = original_version + 1

    if "location" in item["payload"]:
        if item["payload"]["location"].keys() == {"prefix", "provider"}:
            assert item["payload"]["location"]["provider"] in {
                "AmazonS3StorageProvider",
                "InfrequentAccessStorageProvider",
            }, item
            item["payload"]["location"] = {
                "PrimaryS3ReplicaLocation": {
                    "prefix": {
                        "bucket": item["payload"]["location"]["prefix"]["namespace"],
                        "keyPrefix": item["payload"]["location"]["prefix"]["path"],
                    }
                }
            }

            item["version"] = new_version
        elif item["payload"]["location"].keys() == {"PrimaryS3ReplicaLocation"}:
            pass
        else:
            raise ValueError(item["payload"]["location"].keys())

    if isinstance(item["payload"]["replicas"], list):
        for i, replica in enumerate(item["payload"]["replicas"]):
            if replica.keys() == {"prefix", "provider"}:
                assert replica["provider"] in {
                    "AmazonS3StorageProvider",
                    "GlacierStorageProvider",
                }, item
                item["payload"]["replicas"][i] = {
                    "SecondaryS3ReplicaLocation": {
                        "prefix": {
                            "bucket": replica["prefix"]["namespace"],
                            "keyPrefix": replica["prefix"]["path"],
                        }
                    }
                }
                item["version"] = new_version
            elif replica.keys() == {"SecondaryS3ReplicaLocation"}:
                pass
            else:
                raise ValueError(replica.keys())
    elif item["payload"]["replicas"] is None:
        pass
    else:
        raise TypeError


if __name__ == "__main__":
    out_name = "staging_replicas.json"
    table_name = "storage-staging_replicas_table"

    dynamodb_client = get_aws_resource(
        "dynamodb", role_arn="arn:aws:iam::975596993436:role/storage-developer"
    ).meta.client

    total_rows = dynamodb_client.describe_table(TableName=table_name)["Table"][
        "ItemCount"
    ]

    with open(out_name, "a") as out_file:
        out_file.write("---\n")

        for item in tqdm.tqdm(scan_table(TableName=table_name), total=total_rows):
            original_item = copy.deepcopy(item)
            out_file.write(json.dumps(item, cls=DecimalEncoder) + "\n")

            try:
                fix_replicas(item)
            except Exception:
                pprint(item)
                raise

            # This was a test item sent with the new replica aggregator models,
            # so it should be unchanged by this migration.
            if item["id"] == "testing/test_bag/v56":
                assert item == original_item

            if item == original_item:
                continue

            dynamodb_client.put_item(
                TableName=table_name,
                Item=item,
                ConditionExpression="version < :newVersion",
                ExpressionAttributeValues={":newVersion": item["version"]},
            )
