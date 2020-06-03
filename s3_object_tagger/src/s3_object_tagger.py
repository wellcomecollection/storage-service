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


def add_tags(s3_client, *, bucket, key, tags):
    """
    Add a given set of tags to an object in S3.
    """
    s3_uri = f"s3://{bucket}/{key}"

    if not tags:
        print(f"{s3_uri}: no tags to apply")
        return

    # There is no "UpdateTags" API for S3, so we have to read the existing
    # tags, then add the new tags and Put all of them back.
    #
    # Otherwise any existing tags will be deleted.
    existing_tag_resp = s3_client.get_object_tagging(Bucket=bucket, Key=key)
    tag_set = existing_tag_resp["TagSet"]
    print(f"{s3_uri}: existing tags are {tag_set}")

    for tag_key, tag_value in tags.items():
        tag_set.append({"Key": tag_key, "Value": tag_value})

    print(f"{s3_uri}: setting tags to {tags}")

    s3_client.put_object_tagging(Bucket=bucket, Key=key, Tagging={"TagSet": tag_set})


def main(event, *args):
    s3_client = boto3.client("s3")

    for bucket, key in get_objects(event):
        tags = choose_tags(bucket=bucket, key=key)
        add_tags(s3_client, bucket=bucket, key=key, tags=tags)
