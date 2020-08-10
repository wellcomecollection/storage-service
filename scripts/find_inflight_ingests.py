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

import concurrent.futures
import csv
import datetime

from boto3.dynamodb.conditions import Attr

from _aws import scan_table
from common import get_read_only_aws_resource


dynamodb = get_read_only_aws_resource("dynamodb").meta.client


def get_inflight_ingests_for_segment(segment, total_segments):
    return list(
        scan_table(
            TableName="storage-ingests",
            FilterExpression=Attr("payload.status").is_in(["Processing", "Accepted"]),
            Segment=segment,
            TotalSegments=total_segments,
        )
    )


def get_inflight_items():

    # See https://alexwlchan.net/2019/10/adventures-with-concurrent-futures/
    # for an explanation of this code.
    #
    # Rather than scanning the DynamoDB table in serial, we run multiple threads
    # scanning different segments, so we get results faster.
    #
    total_segments = 25

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(
                get_inflight_ingests_for_segment,
                segment=segment,
                total_segments=total_segments,
            )
            for segment in range(total_segments)
        }

        done, _ = concurrent.futures.wait(
            futures, return_when=concurrent.futures.ALL_COMPLETED
        )

        for fut in done:
            yield from fut.result()


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
