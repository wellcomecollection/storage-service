#!/usr/bin/env python3
"""
This is a script to assist in the migration of miro content
into the storage service.
"""

import click
from elasticsearch import helpers

from decisions import get_decisions, count_decisions
from elastic_helpers import get_elastic_client, get_local_elastic_client, index_iterator, get_document_count


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
    reporting_elastic_client = get_elastic_client(
        role_arn=STORAGE_ROLE_ARN, elastic_secret_id=ELASTIC_SECRET_ID
    )

    local_elastic_client = get_local_elastic_client()

    # Required for decisions
    mirror_miro_inventory_locally(
        local_elastic_client=local_elastic_client,
        reporting_elastic_client=reporting_elastic_client,
    )

    local_file_index = "decisions"

    expected_decision_count = count_decisions()

    def _documents():
        for decision in get_decisions():
            yield decision.s3_key, decision.as_dict()

    index_iterator(
        elastic_client=local_elastic_client,
        index_name=local_file_index,
        expected_doc_count=expected_decision_count,
        documents=_documents(),
    )


@click.group()
@click.pass_context
def cli(ctx):
    pass


cli.add_command(create_decisions_index)

if __name__ == "__main__":
    cli()
