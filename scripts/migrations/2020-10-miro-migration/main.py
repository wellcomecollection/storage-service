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
from chunk_transfer import (
    get_chunks,
    create_chunk_package,
    upload_chunk_package,
    update_chunk_record,
)

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
@click.option('--index-name', required=True)
@click.pass_context
def save_index_to_disk(ctx, index_name):
    import json
    import os

    import elasticsearch
    from tqdm import tqdm

    local_elastic_client = get_local_elastic_client()
    document_count = get_document_count(local_elastic_client, index=index_name)

    query_body = {
        "query": {
            "match_all": {}
        }
    }

    all_documents = elasticsearch.helpers.scan(
        local_elastic_client, query=query_body, index=index_name
    )

    save_location = f"_cache/index_{index_name}.json"
    click.echo(f"Saving index {index_name} to {save_location}")

    if os.path.isfile(save_location):
        if not click.confirm(f"File exists at {save_location}, overwrite?"):
            return

        with open(f"_cache/index_{index_name}.json", 'a') as f:
            f.truncate(0)

    with open(f"_cache/index_{index_name}.json", 'a') as f:
        for document in tqdm(all_documents, total=document_count):
            f.write(f"{json.dumps(document)}\n")


@click.command()
@click.option('--index-name', required=True)
@click.option('--target-index-name', required=False)
@click.pass_context
def load_index_from_disk(ctx, index_name, target_index_name):
    import json
    import os

    if not target_index_name:
        target_index_name = index_name

    local_elastic_client = get_local_elastic_client()
    save_location = f"_cache/index_{index_name}.json"

    if not os.path.isfile(save_location):
        click.echo(f"No index file found at {save_location}")
        return

    line_count = sum(1 for _ in open(save_location))

    with open(f"_cache/index_{index_name}.json", 'r') as f:
        def _documents():
            for line in f:
                doc = json.loads(line)
                print(doc)
                return doc['_id'], doc['_source']

        index_iterator(
            elastic_client=local_elastic_client,
            index_name=target_index_name,
            expected_doc_count=line_count,
            documents=_documents(),
        )


@click.command()
@click.pass_context
def transfer_package_chunks(ctx):
    chunks = get_chunks(CHUNKS_INDEX)

    for chunk in chunks:
        if chunk.transfer_package:
            if chunk.transfer_package.s3_location:
                click.echo("Transfer package has S3 Location, skipping.")
                continue

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


@click.group()
@click.pass_context
def cli(ctx):
    pass


cli.add_command(create_chunks_index)
cli.add_command(create_decisions_index)
cli.add_command(transfer_package_chunks)
cli.add_command(save_index_to_disk)
cli.add_command(load_index_from_disk)

if __name__ == "__main__":
    cli()
