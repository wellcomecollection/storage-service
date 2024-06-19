import json
import functools

import boto3
from wellcome_storage_service import RequestsOAuthStorageServiceClient, IngestNotFound


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


def lambda_handler(event, context):
    print(f"Starting lambda_handler, got event: {event}")

    ingest_id = event["pathParameters"]["ingest_id"]

    try:
        ingest = get_prod_client().get_ingest(ingest_id=ingest_id)
        environment = "production"
    except IngestNotFound:
        ingest = get_staging_client().get_ingest(ingest_id=ingest_id)
        environment = "staging"

    response = {"environment": environment, "ingest": ingest}

    return {"statusCode": 200, "body": json.dumps(response)}


if __name__ == "__main__":
    event = {"pathParameters": {"ingest_id": "bd62a401-11c6-46c9-bdc3-346b01f1d05b"}}
    print(lambda_handler(event, None))
