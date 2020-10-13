from elasticsearch import Elasticsearch
from elasticsearch import helpers
import boto3
import json


def get_aws_resource(resource, *, role_arn):
    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn, RoleSessionName="AssumeRoleSession1"
    )
    credentials = assumed_role_object["Credentials"]
    return boto3.resource(
        resource,
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )


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


def get_secret(secretsmanager_client, secret_id):
    resp = secretsmanager_client.get_secret_value(SecretId=secret_id)

    try:
        # The secret response may be a JSON string of the form
        # {"username": "…", "password": "…", "endpoint": "…"}
        secret = json.loads(resp["SecretString"])
    except ValueError:
        secret = resp["SecretString"]

    return secret


def get_elastic_client(secretsmanager_client, elastic_secret_id):
    secret = get_secret(secretsmanager_client, elastic_secret_id)

    return Elasticsearch(
        secret["endpoint"], http_auth=(secret["username"], secret["password"])
    )


def get_born_digital(elastic_client, elastic_index):
    results = helpers.scan(
        client = elastic_client,
        index = elastic_index,
        size = 5,
        query = {
            "query": {
                "term": {
                    "space": {
                        "value": "born-digital"
                    }
                }
            }
        }
    )

    for result in results:
        print(result['_source'])
        raise Exception('nerp')


if __name__ == "__main__":
    role_arn = 'arn:aws:iam::975596993436:role/storage-developer'
    elastic_secret_id = 'archivematica_bags_migration/credentials'
    index = 'storage_bags'
    sts_client = boto3.client("sts")

    secretsmanager_client = get_aws_client(
        resource = 'secretsmanager',
        role_arn = role_arn
    )

    elastic_client = get_elastic_client(
        secretsmanager_client = secretsmanager_client,
        elastic_secret_id = elastic_secret_id
    )

    get_born_digital(elastic_client, index)