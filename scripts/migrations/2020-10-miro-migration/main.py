#!/usr/bin/env python3
"""
This is a script to assist in the migration of miro content
into the storage service.
"""

import json
import os

import click
import elasticsearch
from elasticsearch import helpers

from common import get_aws_client, get_elastic_client, get_local_elastic_client
from transfer_packager import create_transfer_package, upload_transfer_package
from iter_helpers import chunked_iterable

WORKFLOW_ROLE_ARN = "arn:aws:iam::299497370133:role/workflow-developer"
STORAGE_ROLE_ARN = "arn:aws:iam::975596993436:role/storage-developer"
ELASTIC_SECRET_ID = "miro_storage_migration/credentials"
LOCAL_ELASTIC_HOST = os.getenv("LOCAL_ELASTIC_HOST", "localhost")

REMOTE_INVENTORY_INDEX = "miro_inventory"
LOCAL_INVENTORY_INDEX = "reporting_miro_inventory"

S3_ARCHIVEMATICA_BUCKET = "wellcomecollection-archivematica-staging-transfer-source"

S3_MIRO_BUCKET = "wellcomecollection-assets-workingstorage"
S3_MIRO_IMAGES_PATH = "miro/Wellcome_Images_Archive"
S3_MIRO_PREFIX_PATHS = {
    "A Images": 1473,
    "AS Images": 4470,
    "B Images": 9693,
    # "C Scanned": 4997,
    "FP Images": 800,
    "L Images": 88486,
    "M Images": 16881,
    "N Images": 34071,
    "S Images": 18,
    "V Images": 59452,
    "W Images": 2655,
}


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
            truncated_path = os.path.relpath(
                s3_object["Key"], relpath=filtered_path_prefix
            )
            chunk = os.path.basename(truncated_path)

            yield {
                "truncated_path": truncated_path,
                "prefix_path": prefix_path,
                "chunk": chunk,
                "s3_object": s3_object,
            }


def mirror_miro_inventory_locally(*, local_elastic_client, reporting_elastic_client):
    """
    Create a local mirror of the miro_inventory index in the reporting cluster.
    """
    local_count = get_document_count(local_elastic_client, index=LOCAL_INVENTORY_INDEX)

    remote_count = get_document_count(
        reporting_elastic_client, index=REMOTE_INVENTORY_INDEX
    )

    if local_count == remote_count:
        click.echo("miro_inventory index has been mirrored locally, nothing to do")
        return
    else:
        click.echo("miro_inventory index has not been mirrored locally")

    click.echo(
        "Downloading the complete miro_inventory index from the reporting cluster"
    )

    helpers.reindex(
        client=reporting_elastic_client,
        source_index=REMOTE_INVENTORY_INDEX,
        target_index=LOCAL_INVENTORY_INDEX,
        target_client=local_elastic_client,
    )


def get_miro_id(truncated_path):
    """
    Get the Miro ID for a given S3 key.
    """
    # These files won't return any results and don't need to be preserved.
    if truncated_path.endswith("/Thumbs.db"):
        return truncated_path

    # truncated_path instances we need to handle:
    #
    #   L0023499-LH-CS.jp2
    #   B0001840_orig.jp2
    #   M0008000/M0008124EM-LS-LS.jp2
    #
    miro_id = os.path.basename(truncated_path).split("-")[0].replace("_orig", "")

    # e.g. M0008124EM
    try:
        while not miro_id[-1].isnumeric():
            miro_id = miro_id[:-1]
    except IndexError:
        print(truncated_path)
        raise

    return miro_id


def get_documents_for_local_file_index(local_elastic_client, s3_client):
    """
    Generates the documents that should be indexed in the local file index.
    """
    # To reduce the number of searches we have to make, we batch the Miro
    # objects into groups of 500 and use the Multi-Search API to make
    # multiple queries in a single HTTP request.
    #
    # We use a btch of 500 because that means these should be matched to
    # the bulk index requests.
    for batch in chunked_iterable(s3_miro_objects(s3_client=s3_client), size=500):

        # truncated_path instances we need to handle:
        #
        #   L0023499-LH-CS.jp2
        #   B0001840_orig.jp2
        #
        miro_ids = [get_miro_id(miro_object["truncated_path"]) for miro_object in batch]

        queries = [{"query": {"query_string": {"query": f'"{id}"'}}} for id in miro_ids]

        body = "\n".join("{}\n" + json.dumps(q) for q in queries)

        msearch_resp = local_elastic_client.msearch(body, index=LOCAL_INVENTORY_INDEX)
        responses = msearch_resp["responses"]

        assert len(responses) == len(batch), (len(responses), len(batch))

        for miro_object, query_resp in zip(batch, responses):
            hits = query_resp["hits"]["hits"]

            if hits and len(hits) == 1:
                assert len(hits) == 1, (miro_object, hits)
                miro_object["matched_inventory_hit"] = hits[0]["_source"]

            yield (miro_object["truncated_path"], miro_object)


@click.command()
@click.pass_context
def create_files_index(ctx):
    local_elastic_client = ctx.obj["local_elastic_client"]
    reporting_elastic_client = ctx.obj["reporting_elastic_client"]

    mirror_miro_inventory_locally(
        local_elastic_client=local_elastic_client,
        reporting_elastic_client=reporting_elastic_client,
    )

    expected_file_count = sum(S3_MIRO_PREFIX_PATHS.values())
    local_file_index = "files"

    if (
        get_document_count(local_elastic_client, index=local_file_index)
        == expected_file_count
    ):
        click.echo(f"Already created files index {local_file_index}, nothing to do")
        return

    click.echo(f"Recreating files index ({local_file_index})")
    local_elastic_client.indices.delete(index=local_file_index, ignore=[400, 404])

    local_elastic_client.indices.create(index=index_name, ignore=400)

    # We use the Elasticsearch bulk API to index documents, to reduce the number
    # of network requests we need to make.
    # See https://elasticsearch-py.readthedocs.io/en/7.9.1/helpers.html#bulk-helpers
    documents = get_documents_for_local_file_index(
        local_elastic_client=local_elastic_client, s3_client=ctx.obj["storage_s3_client"]
    )

    bulk_actions = (
        {"_index": local_file_index, "_id": id, "_source": source}
        for (id, source) in documents
    )

    successes, errors = helpers.bulk(local_elastic_client, actions=bulk_actions)

    if errors:
        click.echo(f"Errors indexing documents! {errors}")

    assert successes == expected_file_count
    assert (
        get_document_count(local_elastic_client, index=local_file_index)
        == expected_file_count
    )


@click.command()
@click.pass_context
def build_transfer_packages(ctx):
    transfer_package_file_location = create_transfer_package(
        s3_client=ctx.obj["storage_s3_client"],
        group_name="foo",
        s3_bucket=S3_MIRO_BUCKET,
        s3_key_list=[
            "miro/Wellcome_Images_Archive/A Images/A0000000/A0000001-CS-LS.jp2",
            "miro/Wellcome_Images_Archive/A Images/A0000000/A0000003-CS-LS.jp2",
            "miro/Wellcome_Images_Archive/A Images/A0000000/A0000004-CS-LS.jp2",
            "miro/Wellcome_Images_Archive/A Images/A0000000/A0000005-CS-LS.jp2",
        ],
    )

    upload_transfer_package(
        s3_client=ctx.obj["workflow_s3_client"],
        s3_bucket=S3_ARCHIVEMATICA_BUCKET,
        s3_path="born-digital/miro",
        file_location=transfer_package_file_location,
    )


@click.group()
@click.pass_context
def cli(ctx):
    storage_s3_client = get_aws_client(resource="s3", role_arn=STORAGE_ROLE_ARN)
    workflow_s3_client = get_aws_client(resource="s3", role_arn=WORKFLOW_ROLE_ARN)
    local_elastic_client = get_local_elastic_client(host=LOCAL_ELASTIC_HOST)
    reporting_elastic_client = get_elastic_client(
        role_arn=STORAGE_ROLE_ARN, elastic_secret_id=ELASTIC_SECRET_ID
    )

    ctx.obj = {
        "workflow_s3_client": workflow_s3_client,
        "storage_s3_client": storage_s3_client,
        "local_elastic_client": local_elastic_client,
        "reporting_elastic_client": reporting_elastic_client,
    }


cli.add_command(create_files_index)
cli.add_command(build_transfer_packages)

if __name__ == "__main__":
    cli()
