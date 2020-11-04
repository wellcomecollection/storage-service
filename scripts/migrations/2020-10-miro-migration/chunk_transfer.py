import elasticsearch

from chunks import Chunk
from common import get_aws_client
from elastic_helpers import get_local_elastic_client
from transfer_packager import create_transfer_package, upload_transfer_package

STORAGE_ROLE_ARN = "arn:aws:iam::975596993436:role/storage-read_only"
WORKFLOW_ROLE_ARN = "arn:aws:iam::299497370133:role/workflow-developer"
S3_ARCHIVEMATICA_BUCKET = "wellcomecollection-archivematica-staging-transfer-source"
S3_MIRO_BUCKET = "wellcomecollection-assets-workingstorage"

storage_s3_client = get_aws_client("s3", role_arn=STORAGE_ROLE_ARN)
workflow_s3_client = get_aws_client("s3", role_arn=WORKFLOW_ROLE_ARN)


def get_chunks(chunks_index):
    local_elastic_client = get_local_elastic_client()

    results = elasticsearch.helpers.scan(
        local_elastic_client, query={"query": {"match_all": {}}}, index=chunks_index
    )

    return [Chunk(**result["_source"]) for result in results]


def create_chunk_package(chunk):
    # TODO: check if has uploaded time and skip if so
    # TODO: check if zipped size matches an existing file
    # TODO: record zipped size in chunks index
    return create_transfer_package(
        s3_client=storage_s3_client,
        group_name=chunk.chunk_id(),
        s3_bucket=S3_MIRO_BUCKET,
        s3_key_list=chunk.s3_keys,
    )


def upload_chunk_package(file_location):
    # TODO: record uploaded time in chunks index
    return upload_transfer_package(
        s3_client=workflow_s3_client,
        s3_bucket=S3_ARCHIVEMATICA_BUCKET,
        s3_path="born-digital/miro",
        file_location=file_location,
    )
