#!/usr/bin/env python
"""
This script will send every ingest in the storage service to the ingests indexer
to be re-indexed in Elasticsearch.
"""

import concurrent.futures
import datetime
import getpass
import itertools
import json

import boto3
import tqdm


def get_client(resource, role_arn):
    """
    Get a boto3 client with the given role ARN.
    """
    sts_client = boto3.client("sts")
    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn, RoleSessionName="AssumeRoleSession1"
    )
    credentials = assumed_role_object["Credentials"]

    return boto3.resource(
        resource,
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    ).meta.client


def get_total_ingests(table_name):
    dynamodb_client = get_client(
        "dynamodb", role_arn="arn:aws:iam::975596993436:role/storage-read_only"
    )

    resp = dynamodb_client.describe_table(TableName=table_name)

    return resp["Table"]["ItemCount"]


def as_date(decimal_value):
    return datetime.datetime.fromtimestamp(decimal_value / 1000).isoformat() + "Z"


def get_ingests(table_name):
    """
    Generates all the ingests from a DynamoDB table.
    """
    dynamodb_client = get_client(
        "dynamodb", role_arn="arn:aws:iam::975596993436:role/storage-read_only"
    )

    paginator = dynamodb_client.get_paginator("scan")
    for page in paginator.paginate(TableName=table_name):
        for item in page["Items"]:
            ingest = item["payload"]

            # Modify the structure to match the JSON serialisation of the
            # Scala libraries.  Ideally we'd use Scala directly, but getting that
            # to work is more effort than I care to spend right now.
            try:
                ingest["callback"]["status"] = {"type": ingest["callback"]["status"]}
            except KeyError:
                pass

            try:
                ingest["version"] = int(ingest["version"])
            except KeyError:
                pass

            # Note: ingest_events can be ``None``
            ingest_events = ingest.get("events", []) or []
            for event in ingest_events:
                event["createdDate"] = as_date(event["createdDate"])

            ingest["createdDate"] = as_date(ingest["createdDate"])
            ingest["ingestType"] = {"id": ingest["ingestType"]}
            ingest["status"] = {"type": ingest["status"]}
            ingest["sourceLocation"]["provider"] = {
                "type": ingest["sourceLocation"]["provider"]
            }

            if ingest["status"]["type"] == "Completed":
                ingest["status"]["type"] == "Succeeded"

            yield ingest


def send_sns_notifications(all_ingests, topic_arn, ingest_count):
    """
    Sends every entry in ``all_ingests`` to the given topic.
    """
    sns_client = get_client(
        "sns", role_arn="arn:aws:iam::975596993436:role/storage-developer"
    )

    def publish(ingest):
        return sns_client.publish(
            TopicArn=topic_arn,
            Subject=f"Sent by {__file__} (user {getpass.getuser()})",
            Message=json.dumps(ingest),
        )

    # This code parallelises publication, to make ingests go faster.
    # https://alexwlchan.net/2019/10/adventures-with-concurrent-futures/
    max_parallel_notifications = 50

    with tqdm.tqdm(total=ingest_count) as progress_bar:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Schedule the first N futures.  We don't want to schedule them all
            # at once, to avoid consuming excessive amounts of memory.
            futures = {
                executor.submit(publish, ingest)
                for ingest in itertools.islice(all_ingests, max_parallel_notifications)
            }

            while futures:
                # Wait for the next future to complete.
                done, futures = concurrent.futures.wait(
                    futures, return_when=concurrent.futures.FIRST_COMPLETED
                )

                for fut in done:
                    fut.result()

                progress_bar.update(len(done))

                # Schedule the next set of futures.  We don't want more than N futures
                # in the pool at a time, to keep memory consumption down.
                for ingest in itertools.islice(all_ingests, len(done)):
                    futures.add(executor.submit(publish, ingest))


def send_ingests_to_sns(table_name, topic_arn):
    send_sns_notifications(
        all_ingests=get_ingests(table_name=table_name),
        topic_arn=topic_arn,
        ingest_count=get_total_ingests(table_name=table_name),
    )


if __name__ == "__main__":
    send_ingests_to_sns(
        table_name="storage-staging-ingests",
        topic_arn="arn:aws:sns:eu-west-1:975596993436:storage_staging_updated_ingests",
    )

    # send_ingests_to_sns(
    #     table_name="storage-ingests",
    #     topic_arn="arn:aws:sns:eu-west-1:975596993436:storage_prod_updated_ingests"
    # )
