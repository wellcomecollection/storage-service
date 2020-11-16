import os

from iam import create_aws_client_from_role_arn, READ_ONLY_ROLE_ARN, DEV_ROLE_ARN


def list_objects_under(*, bucket, prefix):
    """
    Lists all the objects under a given S3 prefix.
    """
    s3_read_client = create_aws_client_from_role_arn("s3", role_arn=READ_ONLY_ROLE_ARN)

    paginator = s3_read_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for s3_obj in page["Contents"]:
            yield {"bucket": bucket, "key": s3_obj["Key"]}


def s3_sync(src_bucket, src_prefix, dst_bucket, dst_prefix):
    """
    Syncs all the objects between two locations in S3.
    """
    s3_write_client = create_aws_client_from_role_arn("s3", role_arn=DEV_ROLE_ARN)

    for src_obj in list_objects_under(bucket=src_bucket, prefix=src_prefix):
        dst_key = os.path.join(
            dst_prefix, os.path.relpath(src_obj["key"], start=src_prefix)
        )
        s3_write_client.copy(
            CopySource={"Bucket": src_obj["bucket"], "Key": src_obj["key"]},
            Bucket=dst_bucket,
            Key=dst_key,
        )
