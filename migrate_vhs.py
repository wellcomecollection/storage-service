#!/usr/bin/env python3

import concurrent.futures
import itertools
import sys
import uuid

import boto3
from boto3.dynamodb.types import TypeDeserializer
import click


ROLE_ARN = "arn:aws:iam::975596993436:role/storage-developer"

STAGE_CONFIG = {
    "src_table_name": "vhs-storage-staging-manifests",
    "dst_table_name": "vhs-storage-staging-manifests-test",
}

PROD_CONFIG = {
    "src_table_name": "vhs-storage-manifests",
    "dst_table_name": "",
}


def get_config(env):
    if env == "prod":
        return PROD_CONFIG
    else:
        return STAGE_CONFIG


def get_table_length(dynamodb_client, table_name):
    resp = dynamodb_client.describe_table(TableName=table_name)
    return resp["Table"]["ItemCount"]


def create_session(role_arn):
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

    return _assumed_role_session(role_arn)


def transform(item):
    return {
        'payload': item['payload']['typedStoreId']
    }


def parallel_scan_table(dynamo_client, *, TableName, **kwargs):
    total_segments = 25
    max_scans_in_parallel = 5

    tasks_to_do = [
        {
            **kwargs,
            "TableName": TableName,
            "Segment": segment,
            "TotalSegments": total_segments,
        }
        for segment in range(total_segments)
    ]

    scans_to_run = iter(tasks_to_do)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(dynamo_client.scan, **scan_params): scan_params
            for scan_params in itertools.islice(scans_to_run, max_scans_in_parallel)
        }

        while futures:
            # Wait for the first future to complete.
            done, _ = concurrent.futures.wait(
                futures, return_when=concurrent.futures.FIRST_COMPLETED
            )

            for fut in done:
                yield from fut.result()["Items"]

                scan_params = futures.pop(fut)

                try:
                    scan_params["ExclusiveStartKey"] = fut.result()["LastEvaluatedKey"]
                except KeyError:
                    break
                tasks_to_do.append(scan_params)

            for scan_params in itertools.islice(scans_to_run, len(done)):
                futures[executor.submit(dynamo_client.scan, **scan_params)] = scan_params


@click.command()
@click.option("--env", default="stage", help="Environment to run against (prod|stage)")
@click.option(
    "--table_name", help="AWS Role ARN to run this script with"
)
@click.option(
    "--role_arn", default=ROLE_ARN, help="AWS Role ARN to run this script with"
)
def migrate(env, table_name, role_arn):
    config = get_config(env)

    session = create_session(role_arn)
    dynamo_client = session.client("dynamodb")
    dynamo_resource = session.resource("dynamodb")

    src_table_name = config["src_table_name"]
    dst_table_name = config["src_table_name"]

    src_table_length = get_table_length(dynamo_client, src_table_name)

    scanner = parallel_scan_table(dynamo_client, TableName=src_table_name)

    dst_table = dynamo_resource.Table(config["dst_table_name"])

    with dst_table.batch_writer() as batch:
        for low_level_data in scanner:
            deserializer = TypeDeserializer()
            item = {k: deserializer.deserialize(v) for k,v in low_level_data.items()}
            updated_item = transform(item)

            batch.put_item(Item=item)

    dst_table_length = get_table_length(dynamo_client, dst_table_name)

    print(src_table_length)
    print(dst_table_length)


@click.group()
def cli():
    pass

cli.add_command(migrate)

if __name__ == "__main__":
    cli()