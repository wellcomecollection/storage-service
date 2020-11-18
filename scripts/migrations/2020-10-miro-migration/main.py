#!/usr/bin/env python3
"""
This is a script to assist in the migration of miro content
into the storage service.
"""

import attr
import click

from decisions import get_decisions, count_decisions
from chunks import gather_chunks
from elastic_helpers import (
    get_elastic_client,
    get_local_elastic_client,
    index_iterator,
    get_document_count,
    save_index_to_disk,
    load_index_from_disk,
)
from chunk_transfer import (
    get_chunks,
    check_chunk_uploaded,
    create_chunk_package,
    upload_chunk_package,
    update_chunk_record,
)
from uploads import (
    check_package_upload
)

DECISIONS_INDEX = "decisions"
CHUNKS_INDEX = "chunks"


@click.command()
@click.option('--overwrite', '-o', is_flag=True)
@click.pass_context
def create_decisions_index(ctx, overwrite):
    local_elastic_client = get_local_elastic_client()
    expected_decision_count = count_decisions()

    def _documents():
        for decision in get_decisions():
            yield decision.s3_key, attr.asdict(decision)

    index_iterator(
        elastic_client=local_elastic_client,
        index_name=DECISIONS_INDEX,
        expected_doc_count=expected_decision_count,
        documents=_documents(),
        overwrite=overwrite
    )


@click.command()
@click.option('--overwrite', '-o', is_flag=True)
@click.pass_context
def create_chunks_index(ctx, overwrite):
    local_elastic_client = get_local_elastic_client()
    chunks = gather_chunks(DECISIONS_INDEX)
    expected_chunk_count = len(chunks)

    def _documents():
        for chunk in iter(chunks):
            yield chunk.chunk_id(), attr.asdict(chunk)

    index_iterator(
        elastic_client=local_elastic_client,
        index_name=CHUNKS_INDEX,
        expected_doc_count=expected_chunk_count,
        documents=_documents(),
        overwrite=overwrite
    )


@click.command()
@click.option('--index-name', required=True)
@click.option('--overwrite', '-o', is_flag=True)
@click.pass_context
def save_index(ctx, index_name, overwrite):
    local_elastic_client = get_local_elastic_client()
    save_index_to_disk(
        elastic_client=local_elastic_client,
        index_name=index_name,
        overwrite=overwrite
    )


@click.command()
@click.option('--index-name', required=True)
@click.option('--target-index-name', required=False)
@click.option('--overwrite', '-o', is_flag=True)
@click.pass_context
def load_index(ctx, index_name, target_index_name, overwrite):
    if not target_index_name:
        target_index_name = index_name

    local_elastic_client = get_local_elastic_client()

    load_index_from_disk(
        elastic_client=local_elastic_client,
        index_name=index_name,
        target_index_name=target_index_name,
        overwrite=overwrite
    )


@click.command()
@click.pass_context
def transfer_package_chunks(ctx):
    chunks = get_chunks(CHUNKS_INDEX)

    for chunk in chunks:
        if chunk.is_uploaded():
            try:
                check_chunk_uploaded(chunk)
                click.echo("Transfer package has S3 Location, skipping.")
                continue
            except AssertionError as e:
                click.echo(f"Uploaded chunk check failed: {e}")
                click.echo(f"Retrying chunk: {chunk.chunk_id()}")

        created_transfer_package = create_chunk_package(chunk)

        update_chunk_record(
            CHUNKS_INDEX,
            chunk.chunk_id(),
            {"transfer_package": attr.asdict(created_transfer_package)},
        )

        updated_transfer_package = upload_chunk_package(created_transfer_package)

        update_chunk_record(
            CHUNKS_INDEX,
            chunk.chunk_id(),
            {"transfer_package": attr.asdict(updated_transfer_package)},
        )


@click.command()
@click.pass_context
def upload_transfer_packages(ctx):
    chunks = get_chunks(CHUNKS_INDEX)

    for chunk in chunks:
        check_package_upload(chunk)


@click.group()
@click.pass_context
def cli(ctx):
    pass


cli.add_command(create_chunks_index)
cli.add_command(create_decisions_index)
cli.add_command(transfer_package_chunks)
cli.add_command(upload_transfer_packages)
cli.add_command(save_index)
cli.add_command(load_index)


if __name__ == "__main__":
    cli()
