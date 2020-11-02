#!/usr/bin/env python3
"""
This is a script to assist in the migration of miro content
into the storage service.
"""

import os
import re

import click
from decisions import get_decisions, count_decisions
from elastic_helpers import (
    get_elastic_client,
    get_local_elastic_client,
    index_iterator,
    get_document_count,
)
import elasticsearch
from elasticsearch import helpers

STORAGE_ROLE_ARN = "arn:aws:iam::975596993436:role/storage-developer"
ELASTIC_SECRET_ID = "miro_storage_migration/credentials"

REMOTE_INVENTORY_INDEX = "miro_inventory"
LOCAL_INVENTORY_INDEX = "reporting_miro_inventory"


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


@click.command()
@click.pass_context
def create_decisions_index(ctx):
    reporting_elastic_client = ctx.obj["reporting_elastic_client"]
    local_elastic_client = ctx.obj["local_elastic_client"]
    local_decisions_index = ctx.obj["local_decisions_index"]

    # Required for decisions
    mirror_miro_inventory_locally(
        local_elastic_client=local_elastic_client,
        reporting_elastic_client=reporting_elastic_client,
    )

    expected_decision_count = count_decisions()

    def _documents():
        for decision in get_decisions():
            yield decision.s3_key, decision.asdict()

    index_iterator(
        elastic_client=local_elastic_client,
        index_name=local_decisions_index,
        expected_doc_count=expected_decision_count,
        documents=_documents(),
    )


@click.command()
@click.pass_context
def create_chunks(ctx):
    local_elastic_client = ctx.obj["local_elastic_client"]
    local_decisions_index = ctx.obj["local_decisions_index"]

    all_results = elasticsearch.helpers.scan(
        local_elastic_client,
        query={"query": {"match_all": {}}},
        index=local_decisions_index,
    )

    groups = {}

    for result in all_results:
        defer = result["_source"]["defer"]
        skip = result["_source"]["skip"]

        if not (defer or skip):
            s3_key = result["_source"]["s3_key"]
            destinations = result["_source"]["destinations"]

            miro_id = result["_source"]["miro_id"]
            matched = re.search("([A-Z]{1,2})(\d{4,})", miro_id)

            letter_prefix = matched.group(1)
            numeric_id_chunk = matched.group(2)[:4] + "000"

            for destination in destinations:
                chunk_id = letter_prefix + numeric_id_chunk + "/" + destination

                if chunk_id in groups:
                    groups[chunk_id].append(s3_key)
                else:
                    groups[chunk_id] = [s3_key]

    import pprint

    pprint.pprint(groups.keys())


@click.group()
@click.pass_context
def cli(ctx):
    reporting_elastic_client = get_elastic_client(
        role_arn=STORAGE_ROLE_ARN, elastic_secret_id=ELASTIC_SECRET_ID
    )

    local_elastic_client = get_local_elastic_client()

    ctx.obj = {
        "local_decisions_index": "decisions",
        "local_elastic_client": local_elastic_client,
        "reporting_elastic_client": reporting_elastic_client,
    }


cli.add_command(create_chunks)
cli.add_command(create_decisions_index)

if __name__ == "__main__":
    cli()
