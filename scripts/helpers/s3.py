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
