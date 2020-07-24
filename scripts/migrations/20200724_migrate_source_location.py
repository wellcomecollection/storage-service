#!/usr/bin/env python
"""
This script was used to migrate the storage manifests VHS when we changed the
StorageLocation model as part of work to disambiguate ObjectLocation.

Part of https://github.com/wellcomecollection/platform/issues/4596
"""

import json
import os
import pprint
import uuid

from common import get_aws_client, get_aws_resource, scan_table


def migrate_storage_locations(manifest):
    try:
        assert manifest["location"]["type"] == "PrimaryStorageLocation"
        assert manifest["location"]["provider"] == {"type": "AmazonS3StorageProvider"}
        assert manifest["location"].keys() == {"prefix", "provider", "type"}

        manifest["location"] = {
            "prefix": {
                "bucket": manifest["location"]["prefix"]["namespace"],
                "keyPrefix": manifest["location"]["prefix"]["path"],
            },
            "type": "PrimaryS3StorageLocation",
        }

        assert len(manifest["replicaLocations"]) == 1
        replica = manifest["replicaLocations"][0]
        assert replica["type"] == "SecondaryStorageLocation"
        assert replica["provider"] == {"type": "AmazonS3StorageProvider"}
        assert replica.keys() == {"prefix", "provider", "type"}

        manifest["replicaLocations"][0] = {
            "prefix": {
                "bucket": replica["prefix"]["namespace"],
                "keyPrefix": replica["prefix"]["path"],
            },
            "type": "SecondaryS3StorageLocation",
        }
    except AssertionError:
        # This is a new-style manifest written to the old table
        if (
            manifest["location"].keys() == {"prefix", "type"} and
            manifest["location"]["type"] == "PrimaryS3StorageLocation" and
            len(manifest["replicaLocations"]) == 1 and
            manifest["replicaLocations"][0].keys() == {"prefix", "type"} and
            manifest["replicaLocations"][0]["type"] == "SecondaryS3StorageLocation"
        ):
            return
        else:
            pprint.pprint(manifest)
            raise


if __name__ == "__main__":
    s3 = get_aws_client(
        "s3", role_arn="arn:aws:iam::975596993436:role/storage-developer"
    )
    dynamodb_client = get_aws_resource(
        "dynamodb", role_arn="arn:aws:iam::975596993436:role/storage-developer"
    ).meta.client

    old_table_name = "vhs-storage-staging-manifests-25062020"
    new_table_name = "vhs-storage-staging-manifests-2020-07-24"

    migrated_ids = set()

    for item in scan_table(TableName=new_table_name):
        migrated_ids.add((item["id"], item["version"]))

    for item in scan_table(TableName=old_table_name):
        print(item["id"], int(item["version"]))
        if (item["id"], item["version"]) in migrated_ids:
            continue

        manifest = json.load(
            s3.get_object(
                Bucket=item["payload"]["namespace"],
                Key=item["payload"]["path"]
            )["Body"]
        )

        migrate_storage_locations(manifest)

        new_key = os.path.join(
            os.path.dirname(item["payload"]["path"]),
            str(uuid.uuid4())
        )

        s3.put_object(
            Bucket=item["payload"]["namespace"],
            Key=new_key,
            Body=json.dumps(manifest)
        )

        item["payload"]["path"] = new_key
        dynamodb_client.put_item(
            TableName=new_table_name,
            Item=item
        )
