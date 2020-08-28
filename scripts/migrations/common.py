#!/usr/bin/env python

import concurrent.futures
import decimal
import itertools
import json

import boto3


READ_ONLY_ROLE_ARN = "arn:aws:iam::975596993436:role/storage-read_only"


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return int(obj)


def scan_table(*, TableName, **kwargs):
    """
    Generates all the items in a DynamoDB table.

    :param dynamo_client: A boto3 client for DynamoDB.
    :param TableName: The name of the table to scan.

    Other keyword arguments will be passed directly to the Scan operation.
    See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.scan

    """
    dynamodb_client = get_aws_resource(
        "dynamodb", role_arn=READ_ONLY_ROLE_ARN
    ).meta.client

    # https://alexwlchan.net/2020/05/getting-every-item-from-a-dynamodb-table-with-python/
    paginator = dynamodb_client.get_paginator("scan")

    for page in paginator.paginate(TableName=TableName, **kwargs):
        yield from page["Items"]


def parallel_scan_table(table_name,total_segments,max_scans_in_parallel):
    dynamodb = get_aws_resource("dynamodb", role_arn=READ_ONLY_ROLE_ARN)
    table = dynamodb.Table(table_name)
    tasks_to_do = [
        {
            "Segment": segment,
            "TotalSegments": total_segments,
        }
        for segment in range(total_segments)
    ]

    scans_to_run = iter(tasks_to_do)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(table.scan, **scan_params): scan_params
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
                futures[
                    executor.submit(table.scan, **scan_params)
                ] = scan_params


sts_client = boto3.client("sts")


def get_aws_resource(resource, *, role_arn):
    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn, RoleSessionName="AssumeRoleSession1"
    )
    credentials = assumed_role_object["Credentials"]
    return boto3.resource(
        resource,
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )


def get_aws_client(resource, *, role_arn):
    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn, RoleSessionName="AssumeRoleSession1"
    )
    credentials = assumed_role_object["Credentials"]
    return boto3.client(
        resource,
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )
