import secrets

import boto3
from moto import mock_s3
import pytest

from s3_object_tagger import apply_tags


def _get_s3_object(s3_client, *, bucket, key):
    """Retrieves an object from S3, returns (body, tags)"""
    s3_obj = s3_client.get_object(Bucket=bucket, Key=key)
    body = s3_obj["Body"].read()

    tagging_resp = s3_client.get_object_tagging(Bucket=bucket, Key=key)
    tags = [(t["Key"], t["Value"]) for t in tagging_resp["TagSet"]]

    return (body, tags)


@pytest.fixture
def s3_client():
    with mock_s3():
        yield boto3.client("s3")


@pytest.fixture
def s3_bucket(s3_client):
    bucket_name = f"bucket-{secrets.token_hex(5)}"

    s3_client.create_bucket(Bucket=bucket_name)

    return bucket_name


def test_apply_tags_no_op(s3_bucket, s3_client):
    s3_client.put_object(Bucket=s3_bucket, Key="example.txt", Body=b"Hello world")

    apply_tags(s3_client, bucket=s3_bucket, key="example.txt", tags=[])

    body, tags = _get_s3_object(s3_client, bucket=s3_bucket, key="example.txt")
    assert body == b"Hello world"
    assert tags == []


def test_can_apply_tags(s3_bucket, s3_client):
    s3_client.put_object(Bucket=s3_bucket, Key="example.txt", Body=b"Hello world")

    tags_to_set = [("Content-Type", "text/plain"), ("Language", "English")]
    apply_tags(s3_client, bucket=s3_bucket, key="example.txt", tags=tags_to_set)

    body, tags = _get_s3_object(s3_client, bucket=s3_bucket, key="example.txt")
    assert body == b"Hello world"
    assert tags == tags_to_set
