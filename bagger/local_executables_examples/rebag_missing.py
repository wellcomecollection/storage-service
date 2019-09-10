#!/usr/bin/env python
# -*- encoding: utf-8

import itertools
import json
import sys
import uuid

import boto3


def chunked_iterable(iterable, size):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk


def sqs_messages(unbagged_b_numbers):
    batch_id = str(uuid.uuid4())
    for batch in chunked_iterable(unbagged_b_numbers, size=10):
        messages = [
            json.dumps({
                "identifier": b_number,
                "bagger_batch_id": batch_id,
                "bagger_filter": "unbagged",
                "do_not_bag": False,
            })
            for b_number in batch
        ]
        yield messages


def b_numbers(path):
    with open(path) as infile:
        for line in infile:
            yield line.strip()


if __name__ == "__main__":
    try:
        unbagged = sys.argv[1]
    except IndexError:
        sys.exit(f"Usage: {__file__} <UNBAGGED_TXT>")

    sqs = boto3.client("sqs")

    for message_batch in sqs_messages(b_numbers(unbagged)):
        print(", ".join([json.loads(m)["identifier"] for m in message_batch]))
        sqs.send_message_batch(
            QueueUrl="https://sqs.eu-west-1.amazonaws.com/975596993436/storage_prod_bagger",
            Entries=[
                {
                    "Id": str(uuid.uuid4()),
                    "MessageBody": body
                }
                for body in message_batch
            ]
        )