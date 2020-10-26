#!/usr/bin/env python3
"""
This is a script to assist in the migration of miro content
into the storage service.
"""

from datetime import datetime
import os

import click
import tqdm

from common import get_aws_client, get_elastic_client, get_local_elastic_client

ROLE_ARN = "arn:aws:iam::975596993436:role/storage-developer"
ELASTIC_SECRET_ID = "miro_storage_migration/credentials"
LOCAL_ELASTIC_HOST = os.getenv("LOCAL_ELASTIC_HOST", "localhost")
RUNNING_IN_COMPOSE = os.getenv("RUNNING_IN_COMPOSE", False)

S3_MIRO_BUCKET = "wellcomecollection-assets-workingstorage"
S3_MIRO_IMAGES_PATH = "miro/Wellcome_Images_Archive"
S3_MIRO_PREFIX_PATHS = [
    "A Images",
    "AS Images",
    "B Images",
    "C Scanned",
    "FP Images",
    "L Images",
    "M Images",
    "N Images",
    "S Images",
    "V Images",
    "W Images",
]


def create_index(elastic_client, index_name):
    elastic_client.indices.create(index=index_name, ignore=400)


def filter_s3_objects(s3_client, bucket, prefix):
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for content in page["Contents"]:
            yield content


def s3_miro_objects(s3_client):
    for prefix_path in S3_MIRO_PREFIX_PATHS:
        filtered_path_prefix = f"{S3_MIRO_IMAGES_PATH}/{prefix_path}/"
        filtered_s3_objects = filter_s3_objects(
            s3_client=s3_client, bucket=S3_MIRO_BUCKET, prefix=filtered_path_prefix
        )

        for s3_object in filtered_s3_objects:
            truncated_path = s3_object["Key"].replace(filtered_path_prefix, "")

            yield {
                "truncated_path": truncated_path,
                "prefix_path": prefix_path,
                "chunk": truncated_path.split("/")[0],
                "s3_object": s3_object,
            }


def reporting_miro_inventory(elastic_client, query_string):
    reporting_index = "miro_inventory"

    query_results = elastic_client.search(
        index=reporting_index,
        body={"query": {"query_string": {"query": '"' + query_string + '"'}}},
    )

    return query_results["hits"]["hits"]


@click.command()
@click.pass_context
def create_files_index(ctx):
    local_elastic_client = ctx.obj["local_elastic_client"]
    reporting_elastic_client = ctx.obj["reporting_elastic_client"]

    expected_file_count = 223_528
    local_file_index = "files"

    click.echo(f"Recreating files index ({local_file_index})")

    local_elastic_client.indices.delete(index=local_file_index, ignore=[400, 404])

    create_index(elastic_client=local_elastic_client, index_name=local_file_index)

    miro_object_iterator = s3_miro_objects(s3_client=ctx.obj["s3_client"])

    if not RUNNING_IN_COMPOSE:
        miro_object_iterator = tqdm.tqdm(
            s3_miro_objects(s3_client=ctx.obj["s3_client"]), total=expected_file_count
        )

    processed_files = 0
    for miro_object in miro_object_iterator:
        miro_object_id = miro_object["truncated_path"]

        results = reporting_miro_inventory(
            elastic_client=reporting_elastic_client, query_string=miro_object_id
        )

        if results:
            assert len(results) == 1
            miro_object["matched_inventory_hit"] = results[0]["_source"]

        local_elastic_client.create(
            index=local_file_index, id=miro_object_id, body=miro_object
        )
        processed_files = processed_files + 1

        # tqdm/tty things do not work properly in Compose
        if RUNNING_IN_COMPOSE and not processed_files % 500:
            current_time = datetime.now().isoformat()
            percent_complete = int((processed_files / expected_file_count) * 100)
            click.echo(
                f"Processed {processed_files} at {current_time} - {percent_complete}%"
            )

    click.echo(f"Total processed {processed_files}")
    assert processed_files == expected_file_count


@click.group()
@click.pass_context
def cli(ctx):
    s3_client = get_aws_client(resource="s3", role_arn=ROLE_ARN)
    local_elastic_client = get_local_elastic_client(host=LOCAL_ELASTIC_HOST)
    reporting_elastic_client = get_elastic_client(
        role_arn=ROLE_ARN, elastic_secret_id=ELASTIC_SECRET_ID
    )

    ctx.obj = {
        "s3_client": s3_client,
        "local_elastic_client": local_elastic_client,
        "reporting_elastic_client": reporting_elastic_client,
    }

    pass


cli.add_command(create_files_index)

if __name__ == "__main__":
    cli()
