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

    for folder_id in range(3):
        prefix = f"folder-{folder_id}"

        for object_id in range(2000):
            client.put_object(Bucket="my-bukkit", Key=f"{prefix}/object-{object_id}")

    assert len(list(list_s3_keys_in(client, bucket="my-bukkit"))) == 6000
    assert all(
        key.startswith("folder-0/")
        for key in list_s3_keys_in(client, bucket="my-bukkit", prefix="folder-0")
    )

    result = list_s3_keys_in(client, bucket="my-bukkit", prefix="folder-0/")
    assert list(result)[:10] == [
        "folder-0/object-0",
        "folder-0/object-1",
        "folder-0/object-10",
        "folder-0/object-100",
        "folder-0/object-1000",
        "folder-0/object-1001",
        "folder-0/object-1002",
        "folder-0/object-1003",
        "folder-0/object-1004",
        "folder-0/object-1005",
    ]
