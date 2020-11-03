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
)
from chunk_transfer import get_chunks, create_chunk_package, upload_chunk_package

DECISIONS_INDEX = "decisions"
CHUNKS_INDEX = "chunks"


@click.command()
@click.pass_context
def create_decisions_index(ctx):
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
    )


@click.command()
@click.pass_context
def create_chunks_index(ctx):
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
    )


@click.command()
@click.pass_context
def transfer_package_chunks(ctx):
    # TODO: Perform some partitioning on chunks to distribute work
    for chunk in get_chunks(CHUNKS_INDEX):
        chunk_package_file_location = create_chunk_package(chunk)
        upload_chunk_package(chunk_package_file_location)


@click.group()
@click.pass_context
def cli(ctx):
    pass


cli.add_command(create_chunks_index)
cli.add_command(create_decisions_index)
cli.add_command(transfer_package_chunks)

if __name__ == "__main__":
    cli()
