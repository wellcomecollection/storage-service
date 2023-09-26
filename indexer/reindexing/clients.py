import uuid

import boto3
from elasticsearch import Elasticsearch

ROLE_ARN = "arn:aws:iam::975596993436:role/storage-developer"


def create_aws_client(service_name):
    sts_client = boto3.client("sts")
    assumed_role_object = sts_client.assume_role(
        RoleArn=ROLE_ARN, RoleSessionName=uuid.uuid1().hex
    )

    credentials = assumed_role_object["Credentials"]

    session = boto3.Session(
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )
    return session.client(service_name)


def create_es_client(*, env, indexer_type):
    if env == "prod":
        prefix = "prod"
    else:
        prefix = "staging"

    username_secret = f"{prefix}/indexer/{indexer_type}/es_username"
    password_secret = f"{prefix}/indexer/{indexer_type}/es_password"
    hostname_secret = f"{prefix}/indexer/es_host"

    secretsmanager_client = create_aws_client("secretsmanager")

    def _get_secret(secret_id):
        response = secretsmanager_client.get_secret_value(SecretId=secret_id)
        return response["SecretString"]

    endpoint = f"https://{_get_secret(hostname_secret)}:9243"
    return Elasticsearch(
        endpoint, http_auth=(_get_secret(username_secret), _get_secret(password_secret))
    )


STAGE_CONFIG = {
    "table_name": "vhs-storage-staging-manifests-2020-07-24",
    "topic_arn": "arn:aws:sns:eu-west-1:975596993436:storage-staging_bag_reindexer_output",
    "es_index": "storage_stage_bags",
}

PROD_CONFIG = {
    "table_name": "vhs-storage-manifests-2020-07-24",
    "topic_arn": "arn:aws:sns:eu-west-1:975596993436:storage-prod_bag_reindexer_output",
    "es_index": "storage_bags",
}
