from common import get_aws_client, get_storage_client
from storage_service import get_latest_ingest, get_bag
from s3 import get_s3_object_size

S3_ARCHIVEMATICA_BUCKET = "wellcomecollection-archivematica-staging-transfer-source"
WORKFLOW_ROLE_ARN = "arn:aws:iam::299497370133:role/workflow-developer"
STORAGE_SPACE = "miro"


def check_package_transferred(chunk):
    s3_client = get_aws_client("s3", role_arn=WORKFLOW_ROLE_ARN)
    s3_location = chunk.transfer_package.s3_location

    object_size = get_s3_object_size(
        s3_client=s3_client,
        s3_bucket=S3_ARCHIVEMATICA_BUCKET,
        s3_key=s3_location["s3_key"],
    )

    if object_size is not None:
        assert object_size == chunk.transfer_package.content_length
        return {
            "s3_bucket": S3_ARCHIVEMATICA_BUCKET,
            "s3_key": s3_location["s3_key"],
            "size": object_size,
        }
    else:
        return None


def check_storage_service(chunk):
    external_identifier = chunk.chunk_id()

    ingest = get_latest_ingest(
        space=STORAGE_SPACE, external_identifier=external_identifier
    )

    bag = get_bag(space=STORAGE_SPACE, external_identifier=external_identifier)

    return {"ingest": ingest, "bag": bag}


def copy_transfer_package(chunk):
    s3_client = get_aws_client("s3", role_arn=WORKFLOW_ROLE_ARN)
    s3_location = chunk.transfer_package.s3_location

    s3_client.copy_object(
        CopySource={"Bucket": s3_location["s3_bucket"], "Key": s3_location["s3_key"]},
        ACL="bucket-owner-full-control",
        Bucket=S3_ARCHIVEMATICA_BUCKET,
        Key=s3_location["s3_key"],
    )

    return check_package_transferred(chunk)


def check_package_upload(chunk, overwrite):
    if chunk.is_uploaded():
        upload_transfer = check_package_transferred(chunk)

        if upload_transfer is None or overwrite:
            copy_transfer_package(chunk)

        storage_service = check_storage_service(chunk)

        return {"upload_transfer": upload_transfer, "storage_service": storage_service}
    else:
        return None
