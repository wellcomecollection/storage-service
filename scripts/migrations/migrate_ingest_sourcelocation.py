#!/usr/bin/env python
"""
This script was used to migrate the ingests table when we changed the
SourceLocation model in June/July 2020.

This was part of a wide-reaching piece of work to change the ObjectLocation model:
https://github.com/wellcomecollection/platform/issues/4596

Previously:

    case class SourceLocation(
        provider: StorageProvider,
        location: ObjectLocation
    )

after:

    sealed trait SourceLocation {
        val provider: StorageProvider
        val prefix: Prefix[_]
    }

    case class S3SourceLocation(prefix: S3ObjectLocationPrefix) extends SourceLocation

This script read the DynamoDB table holding our ingests, and updated the model
on old ingests.

"""

import decimal
import json
from pprint import pprint

from common import get_aws_resource
import tqdm


def scan_table(dynamo_client, *, TableName, **kwargs):
    """
    Generates all the items in a DynamoDB table.

    :param dynamo_client: A boto3 client for DynamoDB.
    :param TableName: The name of the table to scan.

    Other keyword arguments will be passed directly to the Scan operation.
    See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.scan

    """
    # https://alexwlchan.net/2020/05/getting-every-item-from-a-dynamodb-table-with-python/
    paginator = dynamodb_client.get_paginator("scan")

    for page in paginator.paginate(TableName=TableName, **kwargs):
        yield from page["Items"]


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return int(obj)


if __name__ == "__main__":
    dynamodb_client = get_aws_resource(
        "dynamodb", role_arn="arn:aws:iam::975596993436:role/storage-developer"
    ).meta.client

    out_name = "staging_ingests.json"
    table_name = "storage-staging-ingests"

    total_rows = dynamodb_client.describe_table(TableName=table_name)["Table"][
        "ItemCount"
    ]

    with open(out_name, "a") as out_file:
        out_file.write("---\n")
        for row in tqdm.tqdm(
            scan_table(dynamodb_client, TableName=table_name), total=total_rows
        ):
            out_file.write(json.dumps(row, cls=DecimalEncoder) + "\n")

            if row["payload"]["sourceLocation"].keys() == {"location", "provider"}:
                assert (
                    row["payload"]["sourceLocation"]["provider"]
                    == "AmazonS3StorageProvider"
                ), row
                row["payload"]["sourceLocation"] = {
                    "S3SourceLocation": {
                        "location": {
                            "bucket": row["payload"]["sourceLocation"]["location"][
                                "namespace"
                            ],
                            "key": row["payload"]["sourceLocation"]["location"]["path"],
                        }
                    }
                }
            elif row["payload"]["sourceLocation"].keys() == {"S3SourceLocation"}:
                # Already migrated
                continue
            else:
                pprint(row)
                raise ValueError(
                    "Unrecognised sourceLocation: {row['payload']['sourceLocation']}"
                )

            row["version"] += 1

            # print(row["id"])

            dynamodb_client.put_item(
                TableName=table_name,
                Item=row,
                ConditionExpression="version < :newVersion",
                ExpressionAttributeValues={":newVersion": row["version"]},
            )

            break
