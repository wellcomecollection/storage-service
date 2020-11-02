#!/usr/bin/env python

import functools
import gzip
import json
import os

import boto3
from elasticsearch import Elasticsearch
from wellcome_storage_service import RequestsOAuthStorageServiceClient

READ_ONLY_ROLE_ARN = "arn:aws:iam::975596993436:role/storage-read_only"

sts_client = boto3.client("sts")


@functools.lru_cache
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


@functools.lru_cache
def get_storage_client(api_url):
    return RequestsOAuthStorageServiceClient.from_path(api_url=api_url)


def get_secret(role_arn, secret_id):
    secretsmanager_client = get_aws_client(resource="secretsmanager", role_arn=role_arn)

    response = secretsmanager_client.get_secret_value(SecretId=secret_id)

    try:
        # The secret response may be a JSON string of the form
        # {"username": "…", "password": "…", "endpoint": "…"}
        secret = json.loads(response["SecretString"])
    except ValueError:
        secret = response["SecretString"]

    return secret


def get_elastic_client(role_arn, elastic_secret_id):
    secret = get_secret(role_arn, elastic_secret_id)

    return Elasticsearch(
        secret["endpoint"], http_auth=(secret["username"], secret["password"])
    )


def get_local_elastic_client(host, port=9200):
    return Elasticsearch(host=host, port=9200)


def gz_json_line_count(filename):
    with gzip.open(os.path.join("resources", filename), "rb") as infile:
        for i, _ in enumerate(infile):
            pass

    return i + 1


def gz_json_loader(filename):
    with gzip.open(os.path.join("resources", filename)) as infile:
        for line in infile:
            yield json.loads(line)
