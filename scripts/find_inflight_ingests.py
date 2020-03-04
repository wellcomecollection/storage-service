#!/usr/bin/env python
"""
This script gives a list of every ingest that is in flight -- that is, not in
the "completed" or "failed" state.

This is useful:

*   For checking an ingest hasn't got "lost" in the system
*   If an ingest was created without recording the ingest ID anywhere (e.g. running
    a script but not saving the ingest location)
*   Archivematica doesn't expose the ingest ID anywhere except in the logs; this
    gives a slightly easier way to find it

"""

import csv
import datetime

from boto3.dynamodb.conditions import Attr
import tqdm

from common import get_read_only_aws_resource


def get_items():
    dynamodb = get_read_only_aws_resource("dynamodb").meta.client

    paginator = dynamodb.get_paginator("scan")

    for page in paginator.paginate(
        TableName="storage-ingests",
        FilterExpression=Attr("payload.status").is_in(["Processing", "Accepted"]),
    ):
        for _ in range(page["ScannedCount"] - page["Count"]):
            yield ()
        yield from page["Items"]


def get_inflight_items():
    for item in tqdm.tqdm(get_items()):
        if item:
            yield item


if __name__ == "__main__":
    now = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    csv_name = f"ingests_{now}.csv"

    with open(csv_name, "w") as csvfile:
        writer = csv.DictWriter(
            csvfile, fieldnames=["ingest_id", "space", "external identifier", "version"]
        )
        writer.writeheader()

        for item in get_inflight_items():
            if item.get("Version", 0) > 0:
                version = f"v{int(item['Version'])}"
            else:
                version = ""

            row = {
                "ingest_id": item["id"],
                "space": item["payload"]["space"],
                "external identifier": item["payload"]["externalIdentifier"],
                "version": version,
            }
            writer.writerow(row)
            csvfile.flush()

    print(f"✨ Written in-flight ingests to {csv_name} ✨")
