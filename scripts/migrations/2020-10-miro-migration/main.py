#!/usr/bin/env python3
"""
This is a script to assist in the migration of miro content
into the storage service.
"""

import attr
import click

from decisions import (
    get_decisions,
    count_decisions
)
from chunks import gather_chunks
from elastic_helpers import (
    get_elastic_client,
    get_local_elastic_client,
    index_iterator,
    get_document_by_id,
    get_document_count,
    save_index_to_disk,
    load_index_from_disk,
)
from chunk_transfer import (
    get_chunks,
    get_chunk,
    check_chunk_uploaded,
    create_chunk_package,
    upload_chunk_package,
    update_chunk_record,
)
from sourcedata import (
    gather_sourcedata,
    count_sourcedata
)
from mirofiles import (
    count_mirofiles,
    gather_mirofiles
)

from uploads import check_package_upload, copy_transfer_package

DECISIONS_INDEX = "decisions"
CHUNKS_INDEX = "chunks"
SOURCEDATA_INDEX = "sourcedata"
TRANSFERS_INDEX = "transfers"
FILES_INDEX = "files"

@click.command()
@click.option("--overwrite", "-o", is_flag=True)
@click.pass_context
def create_sourcedata_index(ctx, overwrite):
    local_elastic_client = get_local_elastic_client()
    expected_sourcedata_count = count_sourcedata()

    def _documents():
        for sourcedata in gather_sourcedata():
            yield sourcedata.id, attr.asdict(sourcedata)

    index_iterator(
        elastic_client=local_elastic_client,
        index_name=SOURCEDATA_INDEX,
        expected_doc_count=expected_sourcedata_count,
        documents=_documents(),
        overwrite=overwrite,
    )


@click.command()
@click.option("--overwrite", "-o", is_flag=True)
@click.pass_context
def create_files_index(ctx, overwrite):
    local_elastic_client = get_local_elastic_client()
    expected_mirofiles_count = count_mirofiles()

    def _documents():
        for mirofiles in gather_mirofiles():
            yield mirofiles['_id'], mirofiles['_source']

    index_iterator(
        elastic_client=local_elastic_client,
        index_name=FILES_INDEX,
        expected_doc_count=expected_mirofiles_count,
        documents=_documents(),
        overwrite=overwrite,
    )


@click.command()
@click.option("--overwrite", "-o", is_flag=True)
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
        overwrite=overwrite,
    )


@click.command()
@click.option("--overwrite", "-o", is_flag=True)
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
        overwrite=overwrite,
    )


@click.command()
@click.option("--index-name", required=True)
@click.option("--overwrite", "-o", is_flag=True)
@click.pass_context
def save_index(ctx, index_name, overwrite):
    local_elastic_client = get_local_elastic_client()
    save_index_to_disk(
        elastic_client=local_elastic_client, index_name=index_name, overwrite=overwrite
    )


@click.command()
@click.option("--index-name", required=True)
@click.option("--target-index-name", required=False)
@click.option("--overwrite", "-o", is_flag=True)
@click.pass_context
def load_index(ctx, index_name, target_index_name, overwrite):
    if not target_index_name:
        target_index_name = index_name

    local_elastic_client = get_local_elastic_client()

    load_index_from_disk(
        elastic_client=local_elastic_client,
        index_name=index_name,
        target_index_name=target_index_name,
        overwrite=overwrite,
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


def _upload_package(chunk, overwrite, skip_upload):
    upload = check_package_upload(chunk)
    chunk_id = chunk.chunk_id()


    if upload is not None:
        if (upload["upload_transfer"] is None or overwrite) and not skip_upload:
            new_upload_transfer = copy_transfer_package(chunk)

            s3_bucket = new_upload_transfer["s3_bucket"]
            s3_key = new_upload_transfer["s3_key"]

            click.echo(
                f"Not found. Copying '{chunk_id}' to s3://{s3_bucket}/{s3_key}"
            )
        else:
            s3_bucket = upload["upload_transfer"]["s3_bucket"]
            s3_key = upload["upload_transfer"]["s3_key"]

            click.echo(f"Found '{chunk_id}' at s3://{s3_bucket}/{s3_key}")

    return upload


@click.command()
@click.option("--skip-upload", "-s", is_flag=True)
@click.option("--chunk-id", required=False)
@click.option("--limit", required=False, default=100)
@click.option("--overwrite", "-o", is_flag=True)
@click.pass_context
def upload_transfer_packages(ctx, skip_upload, chunk_id, limit, overwrite):
    local_elastic_client = get_local_elastic_client()
    local_elastic_client.indices.create(index=TRANSFERS_INDEX, ignore=400)

    missing_bags = []

    if chunk_id:
        chunk = get_chunk(CHUNKS_INDEX, chunk_id)

        if chunk is None:
            click.echo(f"No chunk found matching id: '{chunk_id}'")
            return

        chunks = [chunk]
    else:
        chunks = get_chunks(CHUNKS_INDEX)

    for chunk in chunks[:limit]:
        chunk_id = chunk.chunk_id()
        click.echo(f"Looking at '{chunk_id}':")

        upload = get_document_by_id(
            elastic_client=local_elastic_client,
            index_name=TRANSFERS_INDEX,
            id=chunk_id
        )

        has_ingest = False
        has_bag = False
        if upload is not None:
            has_ingest = upload["storage_service"]["ingest"] is not None
            has_bag = upload["storage_service"]["bag"] is not None

        if upload is None or not has_bag:
            upload = _upload_package(chunk, overwrite, skip_upload)

            local_elastic_client.index(
                index=TRANSFERS_INDEX,
                body=upload,
                id=chunk_id
            )

        if has_ingest:
            ingest_id = upload["storage_service"]["ingest"]["id"]
            ingest_status = upload["storage_service"]["ingest"]["status"]["id"]
            click.echo(f"Found ingest {ingest_id}, with status: {ingest_status}")

        if has_bag:
            bag_id = upload["storage_service"]["bag"]["id"]
            bag_internal_id = upload["storage_service"]["bag"]["info"]["internalSenderIdentifier"]
            version = upload["storage_service"]["bag"]["version"]
            click.echo(f"Found bag {bag_id}, (v{version}) with internal id: {bag_internal_id}")
        else:
            missing_bags.append(chunk_id)
        click.echo("")

    click.echo(f"Found {limit - len(missing_bags)} bags from {limit} packages.")
    click.echo(f"No bags found for: {missing_bags}")

@click.group()
@click.pass_context
def cli(ctx):
    pass


cli.add_command(create_chunks_index)
cli.add_command(create_files_index)
cli.add_command(create_sourcedata_index)
cli.add_command(create_decisions_index)
cli.add_command(transfer_package_chunks)
cli.add_command(upload_transfer_packages)
cli.add_command(save_index)
cli.add_command(load_index)


if __name__ == "__main__":
    cli()
