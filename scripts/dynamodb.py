#!/usr/bin/env python
# -*- encoding: utf-8

import concurrent.futures
import datetime as dt
import decimal
import json
import os

from common import get_read_only_aws_resource
import tqdm


dynamodb = get_read_only_aws_resource("dynamodb").meta.client
paginator = dynamodb.get_paginator("scan")


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)


def scan_segment(table_name, out_path, segment, total_segments):
    kwargs = {
        "TableName": table_name,
        "Segment": segment,
        "TotalSegments": total_segments,
    }

    with open(out_path, "w") as outfile:
        for page in paginator.paginate(**kwargs):
            for item in page["Items"]:
                outfile.write(json.dumps(item, cls=DecimalEncoder) + "\n")


def scan_dynamodb_table(table_name, max_workers=5, segment_count=1000):
    scan_id = dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    out_dir = f"dynamodb/{table_name}_{scan_id}"
    os.makedirs(out_dir, exist_ok=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        for segment in range(segment_count):
            out_path = os.path.join(out_dir, f"segment_{segment}.json")
            fut = executor.submit(
                scan_segment, table_name, out_path, segment, segment_count
            )
            futures.append(fut)

        for fut in tqdm.tqdm(
            concurrent.futures.as_completed(futures), total=len(futures)
        ):
            fut.result()

    return out_dir


def cached_scan_iterator(out_dir):
    for f in os.listdir(out_dir):
        if f.endswith(".json"):
            path = os.path.join(out_dir, f)
            with open(path) as infile:
                for line in infile:
                    if not line.strip():
                        continue
                    yield json.loads(line)
