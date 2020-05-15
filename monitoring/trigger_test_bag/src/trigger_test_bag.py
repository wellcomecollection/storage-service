#!/usr/bin/env python

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


def main():
    # TODO: Make this configurable by environment variable.
    client = get_client(
        api_url="https://api-stage.wellcomecollection.org/storage/v1"
    )

    # TODO: Use the bag from wc-storage-infra
    ingest_location = client.create_s3_ingest(
        space_id="testing",
        s3_bucket="wellcomecollection-workflow-export-bagit-stage",
        s3_key="b28656313.tar.gz",
        external_identifier="b28656313"
    )

    print(ingest_location)



if __name__ == "__main__":
    main()
