#!/usr/bin/env python

import decimal
import json

import boto3


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
        "dynamodb", role_arn="arn:aws:iam::975596993436:role/storage-read_only"
    ).meta.client

    # https://alexwlchan.net/2020/05/getting-every-item-from-a-dynamodb-table-with-python/
    paginator = dynamodb_client.get_paginator("scan")

    for page in paginator.paginate(TableName=TableName, **kwargs):
        yield from page["Items"]


def get_aws_resource(resource, role_arn):
    # Taken from https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-api.html

    # The calls to AWS STS AssumeRole must be signed with the access key ID
    # and secret access key of an existing IAM user or by using existing temporary
    # credentials such as those from another role. (You cannot call AssumeRole
    # with the access key for the root account.) The credentials can be in
    # environment variables or in a configuration file and will be discovered
    # automatically by the boto3.client() function. For more information, see the
    # Python SDK documentation:
    # http://boto3.readthedocs.io/en/latest/reference/services/sts.html#client

    # create an STS client object that represents a live connection to the
    # STS service
    sts_client = boto3.client("sts")

    # Call the assume_role method of the STSConnection object and pass the role
    # ARN and a role session name.
    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn, RoleSessionName="AssumeRoleSession1"
    )

    # From the response that contains the assumed role, get the temporary
    # credentials that can be used to make subsequent API calls
    credentials = assumed_role_object["Credentials"]

    # Use the temporary credentials that AssumeRole returns to make a
    # connection to Amazon S3
    return boto3.resource(
        resource,
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )
