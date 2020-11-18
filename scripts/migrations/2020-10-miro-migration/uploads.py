from chunk_transfer import (
    get_chunks,
    check_chunk_uploaded,
)
from common import (
    get_aws_client,
    get_storage_client
)
from elastic_helpers import (
    get_elastic_client,
)
from s3 import get_s3_object_size

S3_ARCHIVEMATICA_BUCKET = "wellcomecollection-archivematica-staging-transfer-source"
STORAGE_ROLE_ARN = "arn:aws:iam::975596993436:role/storage-developer"
WORKFLOW_ROLE_ARN = "arn:aws:iam::299497370133:role/workflow-developer"
STORAGE_API_URL = "https://api-stage.wellcomecollection.org/storage/v1"

ELASTIC_SECRET_ID = "miro_storage_migration/credentials"
REPORTING_INDEX = "storage_stage_bags"
CHUNKS_INDEX = "chunks"


def check_package_transferred(chunk):
    s3_client = get_aws_client("s3", role_arn=WORKFLOW_ROLE_ARN)
    s3_location = chunk.transfer_package.s3_location

    object_size = get_s3_object_size(
        s3_client=s3_client,
        s3_bucket=S3_ARCHIVEMATICA_BUCKET,
        s3_key=s3_location['s3_key']
    )

    if object_size is not None:
        assert object_size == chunk.transfer_package.content_length

    if object_size is None:
        return False
    else:
        return True


def check_storage_service(chunk):
    #storage_client = get_storage_client(api_url=STORAGE_API_URL)
    elastic_client = get_elastic_client(
        role_arn=STORAGE_ROLE_ARN,
        elastic_secret_id=ELASTIC_SECRET_ID
    )

    chunk_id = chunk.chunk_id()

    elastic_query = {
        "query": {
            "bool": {
                "must": {"prefix": {"space": {"value": "miro"}}},
                #"must": {"prefix": {"bag.info.externalIdentifier": {"value": chunk_id}}},
            }
        }
    }

    initial_query = elastic_client.search(
        index=REPORTING_INDEX,
        body=elastic_query
    )

    from pprint import pprint
    for hit in initial_query['hits']['hits']:
        pprint(hit['_id'])


def copy_transfer_package(chunk, overwrite=False):
    s3_client = get_aws_client("s3", role_arn=WORKFLOW_ROLE_ARN)
    chunk_id = chunk.chunk_id()
    s3_location = chunk.transfer_package.s3_location

    assert chunk.is_uploaded()
    if not check_package_transferred(chunk) or overwrite:
        print(f"Copying {chunk_id} to s3://{S3_ARCHIVEMATICA_BUCKET}/{s3_location['s3_key']}")

        s3_client.copy_object(
            CopySource={'Bucket': s3_location['s3_bucket'], 'Key': s3_location['s3_key']},
            ACL='bucket-owner-full-control',
            Bucket=S3_ARCHIVEMATICA_BUCKET,
            Key=s3_location['s3_key']
        )

    else:
        print(f"Found {chunk_id} at s3://{S3_ARCHIVEMATICA_BUCKET}/{s3_location['s3_key']} (not copying)")


def check_package_upload(chunk):
    if chunk.is_uploaded():
        copy_transfer_package(chunk)
        check_storage_service(chunk)

        import sys
        sys.exit(1)
