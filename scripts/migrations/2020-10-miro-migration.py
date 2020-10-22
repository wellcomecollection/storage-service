#!/usr/bin/env python3
"""
This is a script to assist in the migration of miro content
into the storage service.
"""

import click
import sys
from pprint import pprint

from common import get_aws_client, get_elastic_client

ROLE_ARN = 'arn:aws:iam::975596993436:role/storage-developer'
MIRO_BUCKET = 'wellcomecollection-assets-workingstorage'
ELASTIC_SECRET_ID = 'miro_storage_migration/credentials'


def filter_s3_objects(s3_client, bucket, prefix):
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for content in page["Contents"]:
            yield content


@click.command()
@click.option('--env', default='stage', help='Environment to run against (prod|stage)')
@click.option(
    '--role_arn', default=ROLE_ARN, help='AWS Role ARN to run this script with'
)
def migrate(env, role_arn):
    miro_prefix = 'miro/Wellcome_Images_Archive'
    index = 'miro_inventory'

    def _build_elastic_query(query_string):
        return {
                "query": {
                    "query_string": {
                        "query": '\"' + query_string + '\"'
                    }
                }
            }

    s3_client = get_aws_client(resource='s3', role_arn=role_arn)

    elastic_client = get_elastic_client(
        role_arn=role_arn,
        elastic_secret_id=ELASTIC_SECRET_ID
    )

    interesting_prefix_paths = [
        'A Images',
        'AS Images',
        'B Images',
        'C Scanned',
        'FP Images',
        'L Images',
        'M Images',
        'N Images',
        'S Images',
        'V Images',
        'W Images',
    ]

    files_viewed = 0
    matched_inventory = {}
    matched_objects = {}
    interesting_objects = {}

    for prefix_path in interesting_prefix_paths:
        filtered_path_prefix = f"{miro_prefix}/{prefix_path}/"
        filtered_s3_objects = filter_s3_objects(s3_client, MIRO_BUCKET, filtered_path_prefix)

        for s3_object in filtered_s3_objects:
            files_viewed = files_viewed + 1

            truncated_path = s3_object['Key'].replace(filtered_path_prefix, '')
            interesting_objects[truncated_path] = s3_object['Key']

            elastic_query = _build_elastic_query(truncated_path)

            query_results = elastic_client.search(index=index, body=elastic_query)
            hits = query_results['hits']['hits']

            if len(hits):
                assert len(hits) == 1
                matched_inventory_hit = query_results['hits']['hits'][0]

                matched_inventory[matched_inventory_hit['_id']] = s3_object
                matched_objects[s3_object['Key']] = matched_inventory_hit

        pprint(files_viewed)
        pprint(len(matched_inventory))
        pprint(len(matched_objects))

        sys.exit(1)

@click.group()
def cli():
    pass


cli.add_command(migrate)

if __name__ == '__main__':
    cli()