import os

import click
import elasticsearch

from chunks import Chunk
from common import get_aws_client
from elastic_helpers import get_local_elastic_client
from s3 import get_s3_object_size
from transfer_packager import create_transfer_package, upload_transfer_package

STORAGE_ROLE_ARN = "arn:aws:iam::975596993436:role/storage-read_only"
WORKFLOW_ROLE_ARN = "arn:aws:iam::299497370133:role/workflow-developer"
S3_ARCHIVEMATICA_BUCKET = os.getenv(
    "S3_ARCHIVEMATICA_BUCKET",
    "wellcomecollection-archivematica-staging-transfer-source"
)
S3_MIRO_BUCKET = "wellcomecollection-assets-workingstorage"
S3_PREFIX = "miro/Wellcome_Images_Archive"

storage_s3_client = get_aws_client("s3", role_arn=STORAGE_ROLE_ARN)
workflow_s3_client = get_aws_client("s3", role_arn=WORKFLOW_ROLE_ARN)


def get_chunks(chunks_index):
    local_elastic_client = get_local_elastic_client()

    results = elasticsearch.helpers.scan(
        local_elastic_client, query={"query": {"match_all": {}}}, index=chunks_index
    )

    return [Chunk(**result["_source"]) for result in results]


def create_chunk_package(chunk):
    click.echo(f"Creating transfer package for {chunk.chunk_id()}")
    if chunk.transfer_package:
        file_location = chunk.transfer_package.local_location
        expected_content_length = chunk.transfer_package.content_length

        if os.path.isfile(file_location):
            assert os.path.getsize(file_location) == expected_content_length, (
                f"Local transfer package content length mismatch: "
                f"{os.path.getsize(file_location)} != {expected_content_length}"
            )
            click.echo(
                f"Local transfer package for {chunk.chunk_id()} found: "
                f"{file_location} - skipping download."
            )
            return chunk.transfer_package

    transfer_package = create_transfer_package(
        s3_client=storage_s3_client,
        group_name=chunk.chunk_id(),
        s3_bucket=S3_MIRO_BUCKET,
        s3_key_list=chunk.s3_keys,
        prefix=S3_PREFIX,
    )

    click.echo(
        f"Local transfer package created:\n"
        f"  Source: {transfer_package.local_location}\n"
        f"  Content-Length: {transfer_package.content_length} bytes"
    )

    return transfer_package


def upload_chunk_package(transfer_package):
    click.echo(f"Uploading transfer package.")

    if transfer_package.s3_location:
        s3_content_length = get_s3_object_size(
            s3_client=storage_s3_client,
            s3_bucket=S3_ARCHIVEMATICA_BUCKET,
            s3_key=transfer_package.s3_location,
        )
        assert s3_content_length == transfer_package.content_length, (
            f"Content length mismatch for {transfer_package.s3_location}: "
            f"{s3_content_length} != {transfer_package.content_length}"
        )
        click.echo(
            f"Found uploaded transfer package: \n"
            f"{transfer_package.s3_location} - skipping upload."
        )
        return transfer_package

    transfer_package = upload_transfer_package(
        s3_client=workflow_s3_client,
        s3_bucket=S3_ARCHIVEMATICA_BUCKET,
        s3_path="born-digital/miro",
        transfer_package=transfer_package,
    )

    click.echo(
        f"Transfer package uploaded:\n"
        f"  Source: {transfer_package.local_location}\n"
        f"  Destination: s3://{S3_ARCHIVEMATICA_BUCKET}/{transfer_package.s3_location}\n"
        f"  Content-Length: {transfer_package.content_length} bytes"
    )

    return transfer_package


def update_chunk_record(chunks_index, chunk_id, update):
    local_elastic_client = get_local_elastic_client()
    local_elastic_client.update(index=chunks_index, id=chunk_id, body={"doc": update})
