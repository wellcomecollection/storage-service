#!/usr/bin/env python

import concurrent.futures
import hashlib
import itertools
import json
import os
import sqlite3
import sys
import time

import boto3


dynamodb = boto3.resource("dynamodb").meta.client
s3 = boto3.client("s3")
sqs_client = boto3.client("sqs")


def get_messages(sqs_client, queue_url):
    """
    Generates messages from an SQS queue.
    """
    while True:
        resp = sqs_client.receive_message(
            QueueUrl=queue_url,
            AttributeNames=["All"],
            MaxNumberOfMessages=1
        )

        # If there's nothing available, the queue is empty.  Abort!
        try:
            print(
                "Received %d new messages from %s" %
                (len(resp["Messages"]), queue_url)
            )
        except KeyError:
            print("No messages received from %s; waiting" % queue_url)
            time.sleep(10)
            continue

        # If we're deleting the messages ourselves, we don't need to send
        # the ReceiptHandle to the caller (it's only used for deleting).
        # If not, we send the entire response.
        for m in resp["Messages"]:
            yield {k: v for k, v in m.items() if k != "ReceiptHandle"}

        # Now delete the messages from the queue, so they won't be read
        # on the next GET call.
        print(
            "Deleting %d messages from %s" %
            (len(resp["Messages"]), queue_url)
        )
        sqs_client.delete_message_batch(
            QueueUrl=queue_url,
            Entries=[
                {"Id": m["MessageId"], "ReceiptHandle": m["ReceiptHandle"]}
                for m in resp["Messages"]
            ]
        )



def sha1(f):
    hasher = hashlib.sha1()

    while True:
        chunk = f.read(1024)
        if not chunk:
            break
        hasher.update(chunk)

    return hasher.hexdigest()



def get_sha1_checksum(bucket, key):
    s3_obj = s3.get_object(Bucket=bucket, Key=key)
    return sha1(s3_obj["Body"])


def get_cache_for_bag(space, externalIdentifier, version):
    print(f"Starting work on {space}/{externalIdentifier}/v{version}")

    bag_pointer = dynamodb.get_item(
        TableName="vhs-storage-manifests",
        Key={
            "id": f"{space}/{externalIdentifier}",
            "version": version
        }
    )

    bag = json.load(
        s3.get_object(
            Bucket=bag_pointer["Item"]["payload"]["typedStoreId"]["namespace"],
            Key=bag_pointer["Item"]["payload"]["typedStoreId"]["path"],
        )["Body"]
    )

    file_checksums = []

    bucket = bag["location"]["prefix"]["namespace"]

    for bag_file in bag["manifest"]["files"]:

        # Skip all the auto-generated files created by Archivematica
        if (
            bag["space"] == "born-digital" and
            bag_file["name"].startswith("data/logs")
        ):
            continue

        # Skip all the ALTO files, we got those separately
        if (
            bag["space"] == "digitised" and
            bag_file["name"].startswith("data/alto")
        ):
            continue

        # Skip files from a previous version
        if not bag_file["path"].startswith(f"v{bag['version']}/"):
            continue

        key = os.path.join(bag["location"]["prefix"]["path"], bag_file["path"])

        file_checksums.append({
            "key": key,
            "size": bag_file["size"],
            "checksum": f"sha1:{get_sha1_checksum(bucket, key)}"
        })

    payload = {
        "space": bag["space"],
        "externalIdentifier": bag["info"]["externalIdentifier"],
        "version": bag["version"],
        "bucket": bucket,
        "files": file_checksums,
    }

    s3.put_object(
        Bucket=f"wellcomecollection-storage-infra",
        Key=f"sha1/{space}/{externalIdentifier[:4]}/{externalIdentifier}/v{version}.json",
        Body=json.dumps(payload)
    )


if __name__ == "__main__":
    for msg in get_messages(
        sqs_client, queue_url="https://sqs.eu-west-1.amazonaws.com/975596993436/sha1_fetcher_input"
    ):
        bag_id = json.loads(msg["Body"])
        get_cache_for_bag(**bag_id)
