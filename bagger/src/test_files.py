# -*- encoding: utf-8

import random

import boto3
import pytest

from files import download_s3_object


@pytest.fixture(scope="session")
def s3_resource():
    return boto3.resource(
        "s3",
        aws_access_key_id="accessKey1",
        aws_secret_access_key="verySecretKey1",
        endpoint_url="http://localhost:33333/",
    )


@pytest.fixture
def bucket(s3_resource):
    bucket_name = "test-python-bucket-%d" % random.randint(0, 10000)
    bucket = s3_resource.Bucket(bucket_name)
    bucket.create()
    yield bucket


@pytest.mark.parametrize("b_number, upload_key", [
    ("b1234567x", "hello.txt"),
    ("b1234567x", "b1234567x.xml"),
    ("b1234567x", "b1234567X.xml"),
    ("b1234567x", "b1234567x/b1234567X.xml"),
    ("b1234567x", "b1234567X/b1234567x.xml"),
])
def test_downloads_object_correctly(bucket, tmp_path, upload_key, b_number):
    dst_path = str(tmp_path / "hello.txt")

    bucket.put_object(
        Body=b"hello world",
        Key=upload_key
    )

    download_s3_object(
        b_number=b_number,
        source_bucket=bucket,
        source=upload_key,
        destination=dst_path
    )

    assert open(dst_path, "rb").read() == b"hello world"
