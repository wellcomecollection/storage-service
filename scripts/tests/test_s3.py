import boto3
import moto
import pytest

from helpers import list_s3_keys_in


@pytest.fixture
def client():
    with moto.mock_s3():
        yield boto3.client("s3", region_name="us-east-1")


def test_list_s3_keys_in(client):
    client.create_bucket(Bucket="my-bukkit")

    for folder_id in range(2):
        prefix = f"folder-{folder_id}"

        # The S3 ListObjectsV2 API is meant to fetch up to 1000 objects in one go,
        # so create just enough that we don't get everything in one page.
        for object_id in range(1001):
            client.put_object(Bucket="my-bukkit", Key=f"{prefix}/object-{object_id}")

    assert len(list(list_s3_keys_in(client, bucket="my-bukkit"))) == 2002
    assert all(
        key.startswith("folder-0/")
        for key in list_s3_keys_in(client, bucket="my-bukkit", prefix="folder-0")
    )

    result = list_s3_keys_in(client, bucket="my-bukkit", prefix="folder-0/")
    assert len(result) == 1001
    assert list(result)[:10] == [
        "folder-0/object-0",
        "folder-0/object-1",
        "folder-0/object-10",
        "folder-0/object-100",
        "folder-0/object-1000",
        "folder-0/object-101",
        "folder-0/object-102",
        "folder-0/object-103",
        "folder-0/object-104",
        "folder-0/object-105",
    ]


def test_list_s3_keys_in_empty_bucket(client):
    client.create_bucket(Bucket="my-bukkit")

    assert list(list_s3_keys_in(client, bucket="my-bukkit")) == []
