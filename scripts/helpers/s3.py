import os


def list_s3_keys_in(s3_client, *, bucket, prefix=""):
    """
    Lists all the keys in a given S3 bucket/prefix.
    """
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        try:
            for s3_obj in page["Contents"]:
                yield s3_obj["Key"]
        except KeyError:
            return


def copy_s3_prefix(s3_client, *, src_bucket, src_prefix, dst_bucket, dst_prefix):
    """
    Copies all the objects between two prefixes in S3.
    """
    for src_key in list_s3_keys_in(s3_client, bucket=src_bucket, prefix=src_prefix):
        dst_key = os.path.join(
            dst_prefix, os.path.relpath(src_key, start=src_prefix)
        )
        s3_client.copy(
            CopySource={"Bucket": src_bucket, "Key": src_key},
            Bucket=dst_bucket,
            Key=dst_key,
        )
