#!/usr/bin/env python3
"""
This script will send every bag in the storage service to the bags indexer
to be re-indexed in Elasticsearch.
"""

import json
import math
import uuid

import boto3
import click
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from tqdm import tqdm


def scan_table(dynamodb_client, *, TableName, **kwargs):
    paginator = dynamodb_client.get_paginator("scan")

    for page in paginator.paginate(TableName=TableName, **kwargs):
        yield from page["Items"]

def create_client(service_name, role_arn):
    def _assumed_role_session(role_arn):
        sts_client = boto3.client('sts')

        assumed_role_object=sts_client.assume_role(
            RoleArn=role_arn,
            RoleSessionName=uuid.uuid1().hex
        )

        credentials=assumed_role_object['Credentials']

        session = boto3.Session(
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken']
        )

        return session

    session = _assumed_role_session(role_arn)
    return session.client(service_name)

# The bag indexer only cares about space, externalIdentifier & version
def fake_known_replicas_payload(space, externalIdentifier, version):
    return {
        'context': {
            'ingestId': "8368b576-8206-4552-870c-005186f0264a",
            'ingestType': {
                "id": "create"
            },
            'storageSpace': space,
            'ingestDate': "2069-11-30T03:09:31.986Z",
            'externalIdentifier': externalIdentifier,
        },
        'version': version,
        'knownReplicas': {
            'location': {
                'provider': {
                    "type": "AmazonS3StorageProvider"
                },
                'prefix': {
                    "namespace": "go3dmZxK",
                    "path": "XrLuVFbL"
                }
            },
            'replicas': []
        }

    }

def publish_payload(sns_client, topic_arn, payload):
    json_payload = json.dumps(payload)

    return sns_client.publish(
        TopicArn=topic_arn,
        Message=json_payload
    )

def get_total_bags(dynamodb_client, table_name):
    resp = dynamodb_client.describe_table(TableName=table_name)
    return resp["Table"]["ItemCount"]

def get_latest_bags(dynamodb_client, table_name):
    total_bags = get_total_bags(dynamodb_client, table_name)

    print(f"\nGetting latest version of bags from {table_name}")

    bags = {}
    seen_bags = 0

    for item in tqdm(scan_table(dynamodb_client, TableName=table_name), total = total_bags):
        dynamo_id = item['id']['S']
        version = int(item['version']['N'])
        stored_version = bags.get(dynamo_id, -1)

        seen_bags = seen_bags + 1

        if(version > stored_version):
            bags[dynamo_id] = version

    print(f"Found {len(bags)} bags.\n")

    return bags


def publish_bags(sns_client, topic_arn, bags):
    published_bags = []
    unique_bags = len(bags)

    print(f"\nPublishing notifications to {topic_arn}")

    for (dynamo_id, version) in tqdm(bags.items(), total = unique_bags):
        space, external_id = dynamo_id.split("/", 1)
        published_bags.append(dynamo_id)

        payload = fake_known_replicas_payload(space, external_id, version)
        #publish_payload(sns_client, topic_arn,  payload)
        publish_count = len(published_bags)

    print(f"Published notifications for {len(published_bags)} bags.\n")

    return published_bags

def confirm_indexed(elastic_client, published_bags, index):
    print(f"\nConfirm indexed to {index}")

    def _chunks(big_list, chunk_length):
        for i in range(0, len(big_list), chunk_length):
            yield big_list[i:i + chunk_length]

    def _query(ids):
        query_body = {
            "query": {
                "ids" : {
                    "values" : ids
                }
            }
        }

        s = Search(index=index).using(elastic_client).update_from_dict(query_body)

        found_ids = [hit.id for hit in s.scan()]

        return set(ids).difference(found_ids)

    chunk_length = 50
    chunk_count = math.ceil(len(published_bags) / chunk_length)

    diff_list = []
    for chunk in tqdm(_chunks(published_bags, chunk_length), total = chunk_count):
        diff_list.append(_query(chunk))

    flat_list = [item for sublist in diff_list for item in sublist]

    print(f"Found {len(flat_list)} not indexed.\n")

    return flat_list

def create_elastic_client(role_arn, es_secrets):
    secretsmanager_client = create_client("secretsmanager", role_arn)

    def _get_secret(secret_id):
        response = secretsmanager_client.get_secret_value(SecretId=secret_id)

        return response['SecretString']

    config = { key:_get_secret(value) for (key, value) in es_secrets.items() }

    return Elasticsearch(
        [config['hostname']],
        http_auth=(config['username'], config['password']),
        scheme="https",
        port=9243,
    )

ROLE_ARN = 'arn:aws:iam::975596993436:role/storage-developer'

ES_SECRETS = {
    'username': 'storage_bags_reindex_script/es_username',
    'password': 'storage_bags_reindex_script/es_password',
    'hostname': 'storage_bags_reindex_script/es_hostname',
}

STAGE_CONFIG = {
    'table_name': 'vhs-storage-staging-manifests',
    'topic_arn': 'arn:aws:sns:eu-west-1:975596993436:storage_staging_bag_register_output',
    'es_index': 'storage_stage_bags',
}

PROD_CONFIG = {
    'table_name': 'vhs-storage-manifests',
    'topic_arn': 'arn:aws:sns:eu-west-1:975596993436:storage_staging_bag_register_output',
    'es_index': 'storage_bags',
}

def get_config(env):
    if(env == 'prod'):
        return PROD_CONFIG
    else:
        return STAGE_CONFIG

@click.group()
def cli():
    pass

@click.command()
@click.option('--env', default='stage', help='Environment to run against (prod|stage)')
@click.option('--role_arn', default=ROLE_ARN, help='AWS Role ARN to run this script with')
def publish(env, role_arn):
    config = get_config(env)

    dynamodb_client = create_client("dynamodb", role_arn)
    sns_client = create_client("sns", role_arn)

    bags_to_publish = get_latest_bags(dynamodb_client, config['table_name'])
    publish_bags(sns_client, config['topic_arn'], bags_to_publish)

@click.command()
@click.option('--env', default='stage', help='Environment to run against (prod|stage)')
@click.option('--role_arn', default=ROLE_ARN, help='AWS Role ARN to run this script with')
def confirm(env, role_arn):
    config = get_config(env)

    dynamodb_client = create_client("dynamodb", role_arn)
    elastic_client = create_elastic_client(role_arn, ES_SECRETS)

    bags_to_publish = get_latest_bags(dynamodb_client, config['table_name'])
    bags_to_confirm = [key for (key, value) in bags_to_publish.items()]

    not_indexed = confirm_indexed(elastic_client, bags_to_confirm, config['es_index'])
    print(f"NOT INDEXED: {not_indexed}")

cli.add_command(publish)
cli.add_command(confirm)

if __name__ == "__main__":
    cli()