import boto3

from tag_chooser import choose_tags


def get_objects(event):
    """
    Given an S3 event notification, find all the S3 objects it refers to.
    """
    # You can see an example of an the event notification structure in the docs:
    # https://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html
    #
    # The bits we care about are as follows:
    #
    #       {
    #           "Records": [
    #               {
    #                   "s3": {
    #                       "bucket": {"name": "BUCKET", …}
    #                       "object": {"key": "KEY", …},
    #                       …
    #                   },
    #                   …
    #               },
    #               …
    #           ]
    #       }
    #
    for record in event["Records"]:
        s3_obj = record["s3"]
        yield (s3_obj["bucket"]["name"], s3_obj["object"]["key"])


def apply_tags(s3_client, *, bucket, key, tags):
    """
    Apply a given set of tags to an object in S3.
    """
    s3_uri = f"s3://{bucket}/{key}"

    if not tags:
        print(f"{s3_uri}: no tags to apply")
        return

    print(f"{s3_uri}: applying tags {tags}")
    s3_client.put_object_tagging(
        Bucket=bucket,
        Key=key,
        Tagging={
            "TagSet": [
                {"Key": tag_key, "Value": tag_value} for tag_key, tag_value in tags
            ]
        },
    )


def main(event, *args):
    s3_client = boto3.client("s3")

    for bucket, key in get_objects(event):
        tags = choose_tags(bucket=bucket, key=key)
        apply_tags(s3_client, bucket=bucket, key=key, tags=tags)
