#!/usr/bin/env python
"""
Send a test bag to the storage service, then wait for it to be registered
and successfully stored.
"""

import sys
import time

import boto3

from wellcome_storage_service import RequestsOAuthStorageServiceClient


def get_secret_string(sess, *, secret_id):
    client = sess.client("secretsmanager")
    return client.get_secret_value(SecretId=secret_id)["SecretString"]


def get_client(*, api_url):
    sess = boto3.Session()
    secrets_client = sess.client("secretsmanager")

    client_id = get_secret_string(sess, secret_id="buildkite/storage_service/client_id")
    client_secret = get_secret_string(
        sess, secret_id="buildkite/storage_service/client_secret"
    )

    return RequestsOAuthStorageServiceClient(
        api_url=api_url,
        client_id=client_id,
        client_secret=client_secret,
        token_url="https://auth.wellcomecollection.org/oauth2/token",
    )


if __name__ == "__main__":
    try:
        api_url = {
            "prod": "https://api.wellcomecollection.org/storage/v1",
            "stage": "https://api-stage.wellcomecollection.org/storage/v1",
        }[sys.argv[1]]
    except (IndexError, KeyError):
        sys.exit(f"Usage: {__file__} (prod|stage)")

    client = get_client(api_url=api_url)

    print(f"Sending test ingest to {api_url}")
    ingest_url = client.create_s3_ingest(
        space="testing",
        external_identifier="test_bag",
        s3_bucket="wellcomecollection-storage-infra",
        s3_key="test_bags/bag_with_one_text_file.tar.gz",
        ingest_type="update",
    )
    print(f"Waiting for ingest {ingest_url}")

    now = time.time()
    while time.time() - now <= 3600:
        current_ingest = client.get_ingest_from_location(ingest_url)
        status = current_ingest["status"]["id"]
        elapsed_time = time.time() - now

        print(f"status = {status} (t = {int(elapsed_time)}s)")

        if status == "succeeded":
            print(f"🎉 Test bag successful!")
            break
        elif status == "failed":
            print(f"😱 Test bag failed!")
            sys.exit(1)
        else:
            time.sleep(5)

    else:  # no break
        sys.exit(f"Ingest did not complete within an hour: {ingest_url}")
