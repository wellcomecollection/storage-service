import os

from .iterators import chunked_iterable
from .concurrently import concurrently


def list_s3_prefix(s3_client, *, bucket, prefix=""):
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
    for _ in concurrently(
        handler=lambda src_key: s3_client.copy(
            CopySource={"Bucket": src_bucket, "Key": src_key},
            Bucket=dst_bucket,
            Key=os.path.join(dst_prefix, os.path.relpath(src_key, start=src_prefix)),
        ),
        inputs=list_s3_prefix(s3_client, bucket=src_bucket, prefix=src_prefix)
    ):
        pass


def delete_s3_prefix(*, s3_list_client, s3_delete_client, bucket, prefix=""):
    """
    Delete all the objects in a given S3 bucket/prefix.
    """
    # We can delete up to 1000 objects in a single DeleteObjects request.
    for batch in chunked_iterable(
        list_s3_prefix(s3_list_client, bucket=bucket, prefix=prefix), size=1000
    ):
        assert all(s3_key.startswith(prefix) for s3_key in batch)
        s3_delete_client.delete_objects(
            Bucket=bucket, Delete={"Objects": [{"Key": s3_key} for s3_key in batch]}
        )
