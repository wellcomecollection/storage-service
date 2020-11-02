import datetime
import json
import os

from common import get_aws_client


def get_s3_objects_from(*, bucket, prefix=""):
    """
    Generates all the objects under a given bucket/prefix.
    """
    os.makedirs("_cache", exist_ok=True)
    out_path = f"_cache/s3--{bucket}--{prefix.replace('/', '-')}.json"

    try:
        for line in open(out_path):
            s3_obj = json.loads(line)
            s3_obj["LastModified"] = datetime.datetime.fromisoformat(
                s3_obj["LastModified"]
            )
            yield s3_obj
    except FileNotFoundError:
        s3 = get_aws_client(
            "s3", role_arn="arn:aws:iam::760097843905:role/platform-read_only"
        )

        paginator = s3.get_paginator("list_objects_v2")
        tmp_path = out_path + ".tmp"

        with open(tmp_path, "w") as cache_file:
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for s3_obj in page["Contents"]:
                    yield s3_obj
                    s3_obj["LastModified"] = s3_obj["LastModified"].isoformat()
                    cache_file.write(json.dumps(s3_obj) + "\n")
                break

        os.rename(tmp_path, out_path)
