#!/usr/bin/env python3
"""
This script will send every bag in the storage service to the bags indexer
to be re-indexed in Elasticsearch.
"""

import concurrent.futures
import itertools
import getpass
import json
import math
from pprint import pprint
import uuid

import boto3
import click
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from tqdm import tqdm

ROLE_ARN = "arn:aws:iam::975596993436:role/storage-developer"

STAGE_CONFIG = {
    "table_name": "vhs-storage-staging-manifests",
    "topic_arn": "arn:aws:sns:eu-west-1:975596993436:storage_staging_retag_bags_script_output",
}

PROD_CONFIG = {
    "table_name": "vhs-storage-manifests",
    "topic_arn": "arn:aws:sns:eu-west-1:975596993436:storage_prod_retag_bags_script_output",
}


def scan_table(dynamodb_client, *, TableName, **kwargs):
    paginator = dynamodb_client.get_paginator("scan")

    for page in paginator.paginate(TableName=TableName, **kwargs):
        yield from page["Items"]


def create_client(service_name, role_arn):
    def _assumed_role_session(role_arn):
        sts_client = boto3.client("sts")

        assumed_role_object = sts_client.assume_role(
            RoleArn=role_arn, RoleSessionName=uuid.uuid1().hex
        )

        credentials = assumed_role_object["Credentials"]

        session = boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )

        return session

    session = _assumed_role_session(role_arn)
    return session.client(service_name)


def fake_notification(space, externalIdentifier, version):
    return {
        "space": space,
        "externalIdentifier": externalIdentifier,
        "version": f"v{version}",
        "type:": "RegisteredBagNotification",
    }


def get_total_bags(dynamodb_client, table_name):
    resp = dynamodb_client.describe_table(TableName=table_name)
    return resp["Table"]["ItemCount"]


def get_all_bags(dynamodb_client, table_name):
    total_bags = get_total_bags(dynamodb_client, table_name)

    print(f"\nGetting all versions of bags from {table_name}")

    bags = []
    seen_bags = 0

    for item in tqdm(
        scan_table(dynamodb_client, TableName=table_name), total=total_bags
    ):
        dynamo_id = item["id"]["S"]
        version = int(item["version"]["N"])
        seen_bags = seen_bags + 1

        bags.append({"dynamo_id": dynamo_id, "version": version})

    print(f"Found {len(bags)} bags.\n")

    return bags


def get_bags(dynamodb_client, table_name, bag_id):
    response = dynamodb_client.query(
        TableName=table_name,
        KeyConditionExpression="id = :bag_id",
        ExpressionAttributeValues={":bag_id": {"S": bag_id}},
    )

    bags = [
        {"dynamo_id": bag_id, "version": int(item["version"]["N"])}
        for item in response["Items"]
    ]

    return bags


def publish_bags(sns_client, topic_arn, bags, dry_run=False):
    unique_bags = len(bags)

    print(f"\nGenerating notifications for {unique_bags} bags.")
    payloads = []
    for bag in tqdm(bags, total=unique_bags):
        dynamo_id = bag["dynamo_id"]
        version = bag["version"]

        space, external_id = dynamo_id.split("/", 1)
        payload = fake_notification(space, external_id, version)
        payloads.append(payload)

    print(f"Prepared notifications for {len(payloads)} bags.\n")

    # This code parallelises publication, to make bags go faster.
    # https://alexwlchan.net/2019/10/adventures-with-concurrent-futures/
    max_parallel_notifications = 50

    def publish(payload):
        if not dry_run:
            return sns_client.publish(
                TopicArn=topic_arn,
                Subject=f"Sent by {__file__} (user {getpass.getuser()})",
                Message=json.dumps(payload),
            )
        else:
            return True

    payloads_length = len(payloads)
    payloads = iter(payloads)

    print(f"\nPublishing {payloads_length} notifications to {topic_arn}")
    with tqdm(total=payloads_length) as progress_bar:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Schedule the first N futures.  We don't want to schedule them all
            # at once, to avoid consuming excessive amounts of memory.
            futures = {
                executor.submit(publish, payload)
                for payload in itertools.islice(payloads, max_parallel_notifications)
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
                for payload in itertools.islice(payloads, len(done)):
                    futures.add(executor.submit(publish, payload))

    print(f"Published notifications for {payloads_length} bags.\n")


def get_config(env):
    if env == "prod":
        return PROD_CONFIG
    else:
        return STAGE_CONFIG


def gather_bags(dynamodb_client, table_name, bag_ids):
    split_bag_ids = bag_ids.split(",")
    bags = [get_bags(dynamodb_client, table_name, bag_id) for bag_id in split_bag_ids]

    bags_to_publish = []
    for bag in bags:
        bags_to_publish.append(bag)

    return bags_to_publish


@click.group()
def cli():
    pass


@click.command()
@click.option("--env", default="stage", help="Environment to run against (prod|stage)")
@click.option(
    "--ids", default=[], help="Specific Bags to tag (will not scan for all bags)"
)
@click.option("--dry_run", default=False, is_flag=True, help="Do not publish messages")
@click.option(
    "--role_arn", default=ROLE_ARN, help="AWS Role ARN to run this script with"
)
def publish(env, ids, dry_run, role_arn):
    config = get_config(env)

    dynamodb_client = create_client("dynamodb", role_arn)
    sns_client = create_client("sns", role_arn)

    if not ids:
        bags_to_publish = get_all_bags(dynamodb_client, config["table_name"])
    else:
        bags_to_publish = gather_bags(dynamodb_client, config["table_name"], ids)

    publish_bags(sns_client, config["topic_arn"], bags_to_publish, dry_run)


cli.add_command(publish)

if __name__ == "__main__":
    cli()
