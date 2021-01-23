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
    index_updater,
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
from dlcs import (
    get_registrations,
    get_dlcs_image_id,
    get_image_error,
    update_registrations,
    register_image_batch,
    check_batch_successful,
    check_image_successful,
    NO_BATCH_QUERY,
    WITH_BATCH_QUERY,
    NOT_SUCCEEDED_QUERY,
    ONLY_FAILED_QUERY,
    ONLY_SUCCEEDED_QUERY,
    RegistrationUpdate,
)
from iter_helpers import chunked_iterable
from sourcedata import gather_sourcedata, count_sourcedata
from mirofiles import count_mirofiles, gather_mirofiles
from registrations import gather_registrations

from uploads import check_package_upload, copy_transfer_package

SOURCEDATA_INDEX = "sourcedata"
TRANSFERS_INDEX = "transfers"
REGISTRATIONS_INDEX = "registrations"
FILES_INDEX = "files"

S3_LOCATIONS = {
    "archive": {
        "s3_prefix": "miro/Wellcome_Images_Archive",
        "decisions_index": "decisions",
    },
    "derivative": {
        "s3_prefix": "miro/jpg_derivatives",
        "decisions_index": "decisions_derivatives",
    },
}

CHUNKSETS = {
    "chunks": {
        "s3_location": S3_LOCATIONS["archive"],
        "max_chunk_size": 15_000_000_000,
        "query_body": {
            "query": {
                "bool": {
                    "must_not": [{"term": {"skip": True}}],
                    "must": [{"exists": {"field": "destinations"}}],
                }
            }
        },
    },
    "chunks_no_miro_id": {
        "s3_location": S3_LOCATIONS["archive"],
        "max_chunk_size": 15_000_000_000,
        "query_body": {
            "query": {
                "bool": {
                    "must_not": [
                        {"exists": {"field": "destinations"}},
                        {"term": {"skip": True}},
                    ],
                    "must": [{"exists": {"field": "miro_id"}}],
                }
            }
        },
    },
    "chunks_movies_and_corporate": {
        "s3_location": S3_LOCATIONS["archive"],
        "max_chunk_size": 15_000_000_000,
        "query_body": {
            "query": {
                "bool": {
                    "must_not": [
                        {"exists": {"field": "destinations"}},
                        {"term": {"skip": True}},
                        {"exists": {"field": "miro_id"}},
                    ]
                }
            }
        },
    },
    "chunks_derivatives": {
        "s3_location": S3_LOCATIONS["derivative"],
        "max_chunk_size": 500_000_000,
        "query_body": {"query": {"bool": {"must_not": [{"term": {"skip": True}}]}}},
    },
}


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
            yield mirofiles["_id"], mirofiles["_source"]

    index_iterator(
        elastic_client=local_elastic_client,
        index_name=FILES_INDEX,
        expected_doc_count=expected_mirofiles_count,
        documents=_documents(),
        overwrite=overwrite,
    )


@click.command()
@click.option("--location", required=True)
@click.option("--overwrite", "-o", is_flag=True)
@click.pass_context
def create_decisions_index(ctx, location, overwrite):
    click.echo("Attempting to create decisions index.")
    local_elastic_client = get_local_elastic_client()

    s3_prefix = S3_LOCATIONS[location]["s3_prefix"]
    es_index = S3_LOCATIONS[location]["decisions_index"]

    expected_decision_count = count_decisions(s3_prefix=s3_prefix)

    def _documents():
        for decision in get_decisions(s3_prefix=s3_prefix):
            yield decision.s3_key, attr.asdict(decision)

    index_iterator(
        elastic_client=local_elastic_client,
        index_name=es_index,
        expected_doc_count=expected_decision_count,
        documents=_documents(),
        overwrite=overwrite,
    )


@click.command()
@click.option("--chunkset", default="chunks")
@click.option("--overwrite", "-o", is_flag=True)
@click.pass_context
def create_chunks_index(ctx, chunkset, overwrite):
    local_elastic_client = get_local_elastic_client()

    query_body = CHUNKSETS[chunkset]["query_body"]
    max_chunk_size = CHUNKSETS[chunkset]["max_chunk_size"]
    decisions_index = CHUNKSETS[chunkset]["s3_location"]["decisions_index"]

    chunks = gather_chunks(
        query_body=query_body,
        decisions_index=decisions_index,
        max_chunk_size=max_chunk_size,
    )

    expected_chunk_count = len(chunks)

    def _documents():
        for chunk in iter(chunks):
            yield chunk.chunk_id(), attr.asdict(chunk)

    index_iterator(
        elastic_client=local_elastic_client,
        index_name=chunkset,
        expected_doc_count=expected_chunk_count,
        documents=_documents(),
        overwrite=overwrite,
    )


@click.command()
@click.option("--location", required=True)
@click.option("--overwrite", "-o", is_flag=True)
@click.pass_context
def create_registrations_index(ctx, location, overwrite):
    local_elastic_client = get_local_elastic_client()
    registrations = gather_registrations(
        sourcedata_index=SOURCEDATA_INDEX,
        decisions_index=S3_LOCATIONS[location]["decisions_index"],
    )
    expected_registrations_count = len(registrations)

    def _documents():
        for miro_id, file_id in registrations.items():
            yield miro_id, {"file_id": file_id, "miro_id": miro_id}

    if overwrite:
        index_iterator(
            elastic_client=local_elastic_client,
            index_name=REGISTRATIONS_INDEX,
            expected_doc_count=expected_registrations_count,
            documents=_documents(),
            overwrite=True,
        )
    else:
        index_updater(
            elastic_client=local_elastic_client,
            index_name=REGISTRATIONS_INDEX,
            expected_doc_count=expected_registrations_count,
            documents=_documents(),
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
@click.option("--chunkset", default="chunks")
@click.option("--chunk-id", required=False)
@click.option("--overwrite", "-o", is_flag=True)
@click.pass_context
def transfer_package_chunks(ctx, chunkset, chunk_id, overwrite):
    if chunk_id:
        chunk = get_chunk(chunkset, chunk_id)

        if chunk is None:
            click.echo(f"No chunk found matching id: '{chunk_id}'")
            return

        chunks = [chunk]
    else:
        chunks = get_chunks(chunkset)

    for chunk in chunks:
        chunk_id = chunk.chunk_id()

        if chunk.is_uploaded() and not overwrite:
            try:
                check_chunk_uploaded(chunk)
                click.echo(f"{chunk_id}: Transfer package has S3 Location, skipping.")
                continue
            except AssertionError as e:
                click.echo(f"Uploaded chunk check failed: {e}")
                click.echo(f"Retrying chunk: {chunk_id}")

        created_transfer_package = create_chunk_package(
            chunk=chunk, s3_prefix=CHUNKSETS[chunkset]["s3_location"]["s3_prefix"]
        )

        update_chunk_record(
            index_name=chunkset,
            chunk_id=chunk_id,
            update={"transfer_package": attr.asdict(created_transfer_package)},
        )

        updated_transfer_package = upload_chunk_package(created_transfer_package)

        update_chunk_record(
            index_name=chunkset,
            chunk_id=chunk_id,
            update={"transfer_package": attr.asdict(updated_transfer_package)},
        )


def _upload_package(chunk, overwrite, skip_upload):
    upload = check_package_upload(chunk)
    chunk_id = chunk.chunk_id()

    if upload is not None:
        if upload["upload_transfer"] is None or overwrite:
            if not skip_upload:
                new_upload_transfer = copy_transfer_package(chunk.transfer_package)

                s3_bucket = new_upload_transfer["s3_bucket"]
                s3_key = new_upload_transfer["s3_key"]

                click.echo(
                    f"Not found. Copying '{chunk_id}' to s3://{s3_bucket}/{s3_key}"
                )
                return check_package_upload(chunk)
            else:
                print("Skipping upload!")
        else:
            s3_bucket = upload["upload_transfer"]["s3_bucket"]
            s3_key = upload["upload_transfer"]["s3_key"]

            click.echo(f"Found '{chunk_id}' at s3://{s3_bucket}/{s3_key}")

    return upload


# WORKFLOW_ROLE_ARN = "arn:aws:iam::299497370133:role/workflow-developer"
# from common import get_aws_client

# s3_client = get_aws_client('s3', role_arn=WORKFLOW_ROLE_ARN)


@click.command()
@click.option("--skip-upload", "-s", is_flag=True)
@click.option("--chunk-id", required=False)
@click.option("--index-name", default="chunks")
@click.option("--limit", required=False, default=100)
@click.option("--overwrite", "-o", is_flag=True)
@click.pass_context
def upload_transfer_packages(ctx, skip_upload, chunk_id, index_name, limit, overwrite):
    local_elastic_client = get_local_elastic_client()
    local_elastic_client.indices.create(index=TRANSFERS_INDEX, ignore=400)

    missing_bags = []
    has_bags = []

    if chunk_id:
        chunk = get_chunk(index_name, chunk_id)

        if chunk is None:
            click.echo(f"No chunk found matching id: '{chunk_id}'")
            return

        chunks = [chunk]
    else:
        chunks = get_chunks(index_name)

    for chunk in chunks[:limit]:
        chunk_id = chunk.chunk_id()
        click.echo(f"Looking at '{chunk_id}':")

        upload = get_document_by_id(
            elastic_client=local_elastic_client, index_name=TRANSFERS_INDEX, id=chunk_id
        )

        has_ingest = False
        has_bag = False
        has_upload = False

        if upload is not None:
            has_upload = upload["upload_transfer"] is not None

        if has_upload:
            has_ingest = upload["storage_service"]["ingest"] is not None
            has_bag = upload["storage_service"]["bag"] is not None

        # Hack edit: only ever do one upload and then stop
        if not has_upload:
            upload = _upload_package(chunk, overwrite, skip_upload)
            local_elastic_client.index(index=TRANSFERS_INDEX, body=upload, id=chunk_id)
            break
        # Hack edit2: For resetting index and files that are not bagged
        # if not has_bag:
        #    empty_upload = {
        #        'upload_transfer': None,
        #        'storage_service': {
        #            'ingest': None,
        #            'bag': None
        #        }
        #      }

        #    upload = _upload_package(chunk, overwrite, skip_upload)

        #    s3_bucket = upload['upload_transfer']['s3_bucket']
        #    s3_key = upload['upload_transfer']['s3_key']

        #    print(s3_bucket, s3_key)

        #    response = s3_client.delete_object(
        #            Bucket=s3_bucket,
        #            Key=s3_key
        #    )

        #    print(response)

        #    local_elastic_client.index(index=TRANSFERS_INDEX, body=empty_upload, id=chunk_id)

        #    #assert True is False

        # continue

        if not has_upload or not has_bag:
            upload = _upload_package(chunk, overwrite, skip_upload)

            local_elastic_client.index(index=TRANSFERS_INDEX, body=upload, id=chunk_id)

            has_ingest = upload["storage_service"]["ingest"] is not None
            has_bag = upload["storage_service"]["bag"] is not None

        if has_ingest:
            ingest_id = upload["storage_service"]["ingest"]["id"]
            ingest_status = upload["storage_service"]["ingest"]["status"]["id"]
            succeeded = ingest_status == "succeeded"
            if succeeded:
                ingest_color = "green"
            else:
                ingest_color = "yellow"
            click.echo(
                click.style(
                    f"Found ingest {ingest_id}, with status: {ingest_status}",
                    fg=ingest_color,
                )
            )

        if has_bag:
            bag_id = upload["storage_service"]["bag"]["id"]
            bag_internal_id = upload["storage_service"]["bag"]["info"][
                "internalSenderIdentifier"
            ]
            version = upload["storage_service"]["bag"]["version"]
            has_bags.append(bag_id)
            click.echo(
                click.style(
                    f"Found bag {bag_id}, (v{version}) with internal id: {bag_internal_id}",
                    fg="bright_green",
                )
            )
        else:
            missing_bags.append(chunk_id)
            click.echo(click.style(f"No bag!", fg="cyan"))
        click.echo("")

    click.echo(f"Found {len(has_bags)} bags from {len(chunks)} packages.")
    click.echo(f"Missing {len(missing_bags)} bags.")
    click.echo(f"No bags found for: {missing_bags}")


@click.command()
@click.option("--retry-failed", is_flag=True)
@click.option("--chunk-size", required=False, default=50)
@click.option("--limit", required=False, default=50)
@click.pass_context
def dlcs_send_registrations(ctx, retry_failed, chunk_size, limit):
    registrations_query = ONLY_FAILED_QUERY if retry_failed else NO_BATCH_QUERY

    chunked_registrations = chunked_iterable(
        iterable=get_registrations(
            registrations_index=REGISTRATIONS_INDEX, query=registrations_query
        ),
        size=chunk_size,
    )

    batch_counter = 0
    for registrations_chunk in chunked_registrations:
        batch = register_image_batch(registrations=registrations_chunk)

        registration_updates = [
            RegistrationUpdate(
                miro_id=reg["miro_id"], update_doc={"dlcs": {"batch_id": batch["@id"]}}
            )
            for reg in registrations_chunk
        ]

        update_registrations(
            registrations_index=REGISTRATIONS_INDEX,
            registration_updates=registration_updates,
        )

        click.echo(f"Requesting batch {batch_counter}: {batch['@id']}")
        batch_counter = batch_counter + 1
        if batch_counter >= limit:
            break


@click.command()
@click.option("--chunk-size", required=False, default=10000)
@click.option("--limit", required=False, default=10000)
@click.option("--overwrite", "-o", is_flag=True)
@click.pass_context
def dlcs_update_registrations(ctx, chunk_size, limit, overwrite):
    # If overwrite check everything with a batch, otherwise
    # only check things with a batch that haven't succeeded yet

    registrations_query = WITH_BATCH_QUERY if overwrite else NOT_SUCCEEDED_QUERY

    chunked_registrations = chunked_iterable(
        iterable=get_registrations(
            registrations_index=REGISTRATIONS_INDEX, query=registrations_query
        ),
        size=chunk_size,
    )

    success = {}
    waiting = {}
    failure = {}

    batch_counter = 0
    for registrations_chunk in chunked_registrations:
        for reg in registrations_chunk:
            miro_id = reg["miro_id"]

            image_id = get_dlcs_image_id(miro_id)
            batch_id = reg.get("dlcs", {}).get("batch_id")

            batch_successful = (
                check_batch_successful(batch_id) if batch_id is not None else False
            )
            image_successful = (
                True if batch_successful else check_image_successful(image_id)
            )
            image_error = None if image_successful else get_image_error(image_id)

            update = {
                "dlcs": {
                    "batch_id": batch_id,
                    "image_id": image_id,
                    "batch_successful": batch_successful,
                    "image_successful": image_successful,
                    "image_error": image_error,
                }
            }

            if image_successful:
                success[miro_id] = update
            elif image_error is None:
                waiting[miro_id] = update
            else:
                failure[miro_id] = update

        click.echo(f"Refreshing batch {batch_counter}/{limit}")

        all_updates = {**success, **waiting, **failure}

        registration_updates = [
            RegistrationUpdate(miro_id=miro_id, update_doc=update_doc)
            for miro_id, update_doc in all_updates.items()
        ]

        update_registrations(
            registrations_index=REGISTRATIONS_INDEX,
            registration_updates=registration_updates,
        )

        click.echo(f"Updates: {len(all_updates)}")
        click.echo(f"Success: {len(success)} images")
        click.echo(f"Waiting: {len(waiting)} images")
        click.echo(f"Failure: {len(failure)} images")

        click.echo()
        batch_counter = batch_counter + 1
        if batch_counter >= limit:
            break


@click.command()
@click.pass_context
def dlcs_summarise_registrations(ctx):
    local_elastic_client = get_local_elastic_client()

    total = get_document_count(
        elastic_client=local_elastic_client, index=REGISTRATIONS_INDEX
    )
    succeeded = get_document_count(
        elastic_client=local_elastic_client,
        index=REGISTRATIONS_INDEX,
        query=ONLY_SUCCEEDED_QUERY,
    )
    with_batch_id = get_document_count(
        elastic_client=local_elastic_client,
        index=REGISTRATIONS_INDEX,
        query=WITH_BATCH_QUERY,
    )
    not_succeeded = get_document_count(
        elastic_client=local_elastic_client,
        index=REGISTRATIONS_INDEX,
        query=NOT_SUCCEEDED_QUERY,
    )

    click.echo(f"Found {total} registrations.")
    click.echo(f"{succeeded} successfully registered")
    click.echo(f"{with_batch_id} with a batch identifier")
    click.echo(f"{not_succeeded} not yet registered")


@click.group()
@click.pass_context
def cli(ctx):
    pass


cli.add_command(create_chunks_index)
cli.add_command(create_files_index)
cli.add_command(create_sourcedata_index)
cli.add_command(create_decisions_index)
cli.add_command(create_registrations_index)
cli.add_command(transfer_package_chunks)
cli.add_command(upload_transfer_packages)
cli.add_command(dlcs_send_registrations)
cli.add_command(dlcs_update_registrations)
cli.add_command(dlcs_summarise_registrations)
cli.add_command(save_index)
cli.add_command(load_index)


if __name__ == "__main__":
    cli()
