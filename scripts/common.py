# -*- encoding: utf-8

import itertools
import json
import logging
import os
import sys

import boto3
import daiquiri
from wellcome_storage_service import RequestsOAuthStorageServiceClient


def get_logger(name):
    if "--debug" in sys.argv:
        level = logging.DEBUG
    else:
        level = logging.INFO

    daiquiri.setup(
        level=level,
        outputs=[
            daiquiri.output.Stream(
                formatter=daiquiri.formatter.ColorFormatter(
                    fmt="%(color)s[%(levelname)s] %(message)s%(color_stop)s",
                    datefmt="%H:%M:%S",
                )
            )
        ],
    )

    return daiquiri.getLogger(name)


def get_storage_client(api_url):
    creds_path = os.path.join(
        os.environ["HOME"], ".wellcome-storage", "oauth-credentials.json"
    )
    oauth_creds = json.load(open(creds_path))

    return RequestsOAuthStorageServiceClient(
        api_url=api_url,
        client_id=oauth_creds["client_id"],
        client_secret=oauth_creds["client_secret"],
        token_url=oauth_creds["token_url"],
    )


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


def get_aws_client(resource, *, role_arn):
    sts_client = boto3.client("sts")

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


def get_read_only_aws_resource(resource):
    return get_aws_resource(
        resource, role_arn="arn:aws:iam::975596993436:role/storage-read_only"
    )


def chunked_iterable(iterable, size):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk
