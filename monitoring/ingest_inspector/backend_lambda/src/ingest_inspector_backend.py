import argparse
import json
import functools

import boto3
from wellcome_storage_service import (
    RequestsOAuthStorageServiceClient,
    IngestNotFound,
    UserError,
)
from utils import (
    tally_event_descriptions,
    get_s3_url,
    get_display_s3_url,
    get_last_updated_date,
)

STAGING_URL = "https://api-stage.wellcomecollection.org/storage/v1"
PRODUCTION_URL = "https://api.wellcomecollection.org/storage/v1"


def _client_from_environment(api_url):
    secretsmanager = boto3.Session().client("secretsmanager")

    def _get_secretsmanager_value(secret_id: str):
        return secretsmanager.get_secret_value(SecretId=secret_id)["SecretString"]

    client_id = _get_secretsmanager_value("ingest-inspector/cognito-client-id")
    client_secret = _get_secretsmanager_value("ingest-inspector/cognito-client-secret")

    return RequestsOAuthStorageServiceClient(
        api_url=api_url,
        client_id=client_id,
        client_secret=client_secret,
        token_url="https://auth.wellcomecollection.org/oauth2/token",
    )


@functools.lru_cache()
def get_prod_client():
    return _client_from_environment(PRODUCTION_URL)


@functools.lru_cache()
def get_staging_client():
    return _client_from_environment(STAGING_URL)


def get_ingest(ingest_id: str):
    try:
        ingest = get_prod_client().get_ingest(ingest_id=ingest_id)
        environment = "production"
    except IngestNotFound:
        ingest = get_staging_client().get_ingest(ingest_id=ingest_id)
        environment = "staging"

    return ingest, environment


def lambda_handler(event, context):
    print(f"Starting lambda_handler, got event: {event}")

    ingest_id = event["pathParameters"]["ingest_id"]

    try:
        ingest, environment = get_ingest(ingest_id)
    except IngestNotFound:
        return {"statusCode": 404, "body": json.dumps({"message": "Ingest not found."})}
    except UserError:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Invalid ingest ID."}),
        }

    ingest["events"] = tally_event_descriptions(ingest["events"], environment)
    ingest["s3Url"] = get_s3_url(ingest["sourceLocation"])
    ingest["displayS3Url"] = get_display_s3_url(ingest["sourceLocation"])
    ingest["lastUpdatedDate"] = get_last_updated_date(ingest)
    ingest["environment"] = environment

    return {"statusCode": 200, "body": json.dumps(ingest)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Index EBSCO item fields into the Elasticsearch reporting cluster."
    )
    parser.add_argument(
        "--ingest-id",
        type=str,
        help="The ID of the ingest to retrieve.",
        required=True,
    )
    args = parser.parse_args()

    event = {"pathParameters": {"ingest_id": args.ingest_id}}
    print(lambda_handler(event, None))
