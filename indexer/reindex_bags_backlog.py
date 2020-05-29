#!/usr/bin/env python3
"""
This script will send every bag in the storage service to the bags indexer
to be re-indexed in Elasticsearch.
"""

import boto3
import uuid
import json
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

from pprint import pprint


def scan_table(dynamo_client, *, TableName, **kwargs):
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

    print(f"total_bags: {total_bags}")

    bags = {}
    seen_bags = 0

    for item in scan_table(dynamodb_client, TableName=table_name):
        dynamo_id = item['id']['S']
        version = int(item['version']['N'])
        stored_version = bags.get(dynamo_id, -1)

        seen_bags = seen_bags + 1

        if(seen_bags % 10 == 0 or seen_bags > (total_bags - 10)):
            print(f"{seen_bags}/{total_bags}")

        if(version > stored_version):
            bags[dynamo_id] = version

    print(f"latest_bags: {len(bags)}")

    return bags


def publish_bags(sns_client, topic_arn, bags):
    published_bags = []
    unique_bags = len(bags)

    for (dynamo_id, version) in bags.items():
        space, external_id = dynamo_id.split("/", 1)
        published_bags.append(dynamo_id)

        payload = fake_known_replicas_payload(space, external_id, version)
        #publish_payload(sns_client, topic_arn,  payload)
        publish_count = len(published_bags)

        if(publish_count % 10 == 0 or publish_count > (unique_bags - 10)):
            print(f"{publish_count}/{unique_bags}")

    print(f"published_bags: {len(published_bags)}")

    return published_bags

def confirm_indexed(elastic_client, published_bags):
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

        s = Search(index="storage_stage_bags").using(elastic_client).update_from_dict(query_body)

        found_ids = [hit.id for hit in s.scan()]

        return set(ids).difference(found_ids)

    diff_list = [_query(chunk) for chunk in _chunks(published_bags, 50)]
    flat_list = [item for sublist in diff_list for item in sublist]

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

if __name__ == "__main__":
    role_arn = 'arn:aws:iam::975596993436:role/storage-developer'
    table_name = 'vhs-storage-staging-manifests'
    #table_name = 'vhs-storage-manifests'
    topic_arn = 'arn:aws:sns:eu-west-1:975596993436:storage_staging_bag_register_output'

    es_secrets = {
        'username': 'storage_bags_reindex_script/es_username',
        'password': 'storage_bags_reindex_script/es_password',
        'hostname': 'storage_bags_reindex_script/es_hostname',
    }

    dynamodb_client = create_client("dynamodb", role_arn)
    sns_client = create_client("sns", role_arn)
    elastic_client = create_elastic_client(role_arn, es_secrets)

    bags_to_publish = get_latest_bags(dynamodb_client, table_name)
    published_bags = publish_bags(sns_client, topic_arn, bags_to_publish)
    not_indexed = confirm_indexed(elastic_client, published_bags)

    print(not_indexed)