#!/usr/bin/env python3
"""
This is a script to assist in the migration of miro content
into the storage service.
"""

from datetime import datetime
import os

import click
import elasticsearch
from elasticsearch import helpers
import tqdm

from common import get_aws_client, get_elastic_client, get_local_elastic_client
from iter_helpers import chunked_iterable

ROLE_ARN = "arn:aws:iam::975596993436:role/storage-developer"
ELASTIC_SECRET_ID = "miro_storage_migration/credentials"
LOCAL_ELASTIC_HOST = os.getenv("LOCAL_ELASTIC_HOST", "localhost")
RUNNING_IN_COMPOSE = os.getenv("RUNNING_IN_COMPOSE", False)

REMOTE_INVENTORY_INDEX = "miro_inventory"
LOCAL_INVENTORY_INDEX = "reporting_miro_inventory"

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


def get_document_count(client, *, index):
    """
    How many documents are there in an Elasticsearch index?
    """
    try:
        return client.count(index=index)["count"]
    except elasticsearch.exceptions.NotFoundError:
        return 0


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


def reporting_miro_inventory(local_elastic_client, *, query_string):
    """
    Return the result of querying the miro_inventory index for the given query string.
    """
    query_results = local_elastic_client.search(
        index=LOCAL_INVENTORY_INDEX,
        body={"query": {"query_string": {"query": '"' + query_string + '"'}}},
    )

    return query_results["hits"]["hits"]


def mirror_miro_inventory_locally(*, local_elastic_client, reporting_elastic_client):
    """
    Create a local mirror of the miro_inventory index in the reporting cluster.
    """
    local_count = get_document_count(
        local_elastic_client, index=LOCAL_INVENTORY_INDEX
    )

    remote_count = get_document_count(
        reporting_elastic_client, index=REMOTE_INVENTORY_INDEX
    )

    if local_count == remote_count:
        click.echo("miro_inventory index has been mirrored locally, nothing to do")
        return
    else:
        click.echo("miro_inventory index has not been mirrored locally")

    click.echo("Downloading the complete miro_inventory index from the reporting cluster")

    helpers.reindex(
        client=reporting_elastic_client,
        source_index=REMOTE_INVENTORY_INDEX,
        target_index=LOCAL_INVENTORY_INDEX,
        target_client=local_elastic_client
    )


def get_documents_for_local_file_index(local_elastic_client, s3_client):
    """
    Generates the documents that should be indexed in the local file index.
    """
    for miro_object in s3_miro_objects(s3_client=s3_client):
        miro_object_id = miro_object["truncated_path"]

        results = reporting_miro_inventory(
            local_elastic_client, query_string=miro_object_id
        )

        if results:
            assert len(results) == 1
            miro_object["matched_inventory_hit"] = results[0]["_source"]

        yield (miro_object_id, miro_object)


@click.command()
@click.pass_context
def create_files_index(ctx):
    local_elastic_client = ctx.obj["local_elastic_client"]
    reporting_elastic_client = ctx.obj["reporting_elastic_client"]

    mirror_miro_inventory_locally(
        local_elastic_client=local_elastic_client,
        reporting_elastic_client=reporting_elastic_client
    )

    expected_file_count = 223_528
    local_file_index = "files"

    if get_document_count(local_elastic_client, index=local_file_index) == expected_file_count:
        click.echo(f"Already created files index {local_file_index}, nothing to do")
        return

    click.echo(f"Recreating files index ({local_file_index})")
    local_elastic_client.indices.delete(index=local_file_index, ignore=[400, 404])
    create_index(elastic_client=local_elastic_client, index_name=local_file_index)

    # We use the Elasticsearch bulk API to index documents, to reduce the number
    # of network requests we need to make.
    # See https://elasticsearch-py.readthedocs.io/en/7.9.1/helpers.html#bulk-helpers
    documents = get_documents_for_local_file_index(
        local_elastic_client=local_elastic_client,
        s3_client=ctx.obj["s3_client"]
    )

    bulk_actions = (
        {
            "_index": local_file_index,
            "_id": id,
            "_source": source
        }
        for (id, source) in documents
    )

    successes, errors = helpers.bulk(
        local_elastic_client,
        actions=tqdm.tqdm(bulk_actions, total=expected_file_count)
    )

    if errors:
        click.echo(f"Errors indexing documents! {errors}")

    assert successes == expected_file_count
    assert get_document_count(local_elastic_client, index=local_file_index) == expected_file_count


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
