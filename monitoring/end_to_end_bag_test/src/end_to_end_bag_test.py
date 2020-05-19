#!/usr/bin/env python
"""
Trigger an end-to-end test of a bag through the storage service.
"""

import os

import boto3
from wellcome_storage_service import StorageServiceClient


def get_client(api_url):
    """
    Retrieve credentials from Secrets Manager to create a storage service client.
    """
    secrets_client = boto3.client("secretsmanager")

    client_id = secrets_client.get_secret_value(
        SecretId="end_to_end_bag_tester/client_id"
    )

    client_secret = secrets_client.get_secret_value(
        SecretId="end_to_end_bag_tester/client_secret"
    )

    return StorageServiceClient(
        api_url=api_url,
        client_id=client_id["SecretString"],
        client_secret=client_secret["SecretString"],
        token_url="https://auth.wellcomecollection.org/oauth2/token",
    )


def main(*args):
    bucket = os.environ["BUCKET"]
    key = os.environ["KEY"]
    external_identifier = os.environ["EXTERNAL_IDENTIFIER"]

    api_url = os.environ["API_URL"]

    client = get_client(api_url=api_url)

    ingest_location = client.create_s3_ingest(
        space_id="testing",
        s3_bucket=bucket,
        s3_key=key,
        external_identifier=external_identifier
    )

    print(ingest_location)



if __name__ == "__main__":
    main()
