#!/usr/bin/env python
# -*- encoding: utf-8
"""
Start an ingest.  Usage:

    python3 ss_start_ingest.py <FILENAME>

The script will look in the bagger drop buckets to find the file,
and automatically send it to the correct API.

"""

import getpass
import json
import os
import re
import sys

import boto3
from botocore.exceptions import ClientError
import click

from common import get_logger, get_storage_client


logger = get_logger(__name__)


def get_s3_resource(role_arn):
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
        RoleArn=role_arn,
        RoleSessionName="AssumeRoleSession1"
    )

    # From the response that contains the assumed role, get the temporary
    # credentials that can be used to make subsequent API calls
    credentials = assumed_role_object["Credentials"]

    # Use the temporary credentials that AssumeRole returns to make a
    # connection to Amazon S3
    return boto3.resource(
        "s3",
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )


if __name__ == "__main__":
    try:
        filename = sys.argv[1]
    except IndexError:
        sys.exit(f"Usage: {__file__} <FILENAME>")

    logger.debug("Creating ingest for file %s", filename)

    s3 = get_s3_resource(
        role_arn="arn:aws:iam::975596993436:role/storage-read_only"
    )

    buckets = {
        "prod": "wellcomecollection-storage-bagger-drop",
        "stage": "wellcomecollection-storage-staging-bagger-drop",
    }

    for api_name, bucket_name in buckets.items():
        try:
            s3.Object(bucket_name, filename).load()
        except ClientError as err:
            if str(err) == "An error occurred (404) when calling the HeadObject operation: Not Found":
                logger.debug("Didn't find object in bucket %s", bucket_name)
                continue
            else:
                raise
        else:
            api = api_name
            break

    try:
        logger.info("Using %s API", api)
        logger.info("Detected bucket as %s", buckets[api])
    except NameError:
        logger.error(
            "Could not find object in buckets! %s",
            ", ".join(buckets.values())
        )
        sys.exit(1)

    ext_id_match = re.search(r"b[0-9]{7}[0-9x]", filename)
    if ext_id_match is None:
        logger.error("Could not detect external identifier!")
        sys.exit(1)
    else:
        external_identifier = ext_id_match.group(0)
        logger.info("Detected external identifier as %s", external_identifier)

    creds_path = os.path.join(
        os.environ["HOME"], ".wellcome-storage", "oauth-credentials.json"
    )
    oauth_creds = json.load(open(creds_path))

    space_id = "-".join([getpass.getuser(), "testing"])
    space_id = click.prompt("Storage space?", default=space_id)
    logger.info("Using storage space %s", space_id)

    logger.info("Making request to storage service")

    host = "api" if api == "prod" else "api-stage"
    api_url = f"https://{host}.wellcomecollection.org/storage/v1"

    client = get_storage_client(api_url=api_url)

    location = client.create_s3_ingest(
        space_id=space_id,
        s3_bucket=buckets[api],
        s3_key=filename,
        external_identifier=external_identifier
    )

    logger.debug("Ingest created at URL %s", location)
    logger.info("Ingest has ID %s", location.split("/")[-1])
    logger.info(
        "To look up the ingest:\n\n\tpython3 ss_get_ingest.py %s",
        location.split("/")[-1]
    )
