import boto3
import moto
import pytest

from helpers import copy_s3_prefix, delete_s3_prefix, list_s3_prefix


@pytest.fixture
def client():
    with moto.mock_s3():
        yield boto3.client("s3", region_name="us-east-1")


def test_list_s3_prefix(client):
    client.create_bucket(Bucket="my-bukkit")

    for folder_id in range(2):
        prefix = f"folder-{folder_id}"

        # The S3 ListObjectsV2 API is meant to fetch up to 1000 objects in one go,
        # so create just enough that we don't get everything in one page.
        for object_id in range(1001):
            client.put_object(Bucket="my-bukkit", Key=f"{prefix}/object-{object_id}")

    assert len(list(list_s3_prefix(client, bucket="my-bukkit"))) == 2002
    assert all(
        key.startswith("folder-0/")
        for key in list_s3_prefix(client, bucket="my-bukkit", prefix="folder-0")
    )

    result = list(list_s3_prefix(client, bucket="my-bukkit", prefix="folder-0/"))
    assert len(result) == 1001
    assert result[:10] == [
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

    # If we now delete a batch of objects, we can still retrieve the remaining
    # objects.
    delete_s3_prefix(client, bucket="my-bukkit", prefix="folder-0/")
    result = list(list_s3_prefix(client, bucket="my-bukkit"))
    assert len(result) == 1001
    assert all(key.startswith("folder-1/") for key in result)


def test_list_s3_prefix_empty_bucket(client):
    client.create_bucket(Bucket="my-bukkit")

    assert list(list_s3_prefix(client, bucket="my-bukkit")) == []


def test_copy_s3_prefix(client):
    client.create_bucket(Bucket="bukkit-1")
    client.create_bucket(Bucket="bukkit-2")

    files = {
        "greeting.en.txt": b"Hello world",
        "greeting.fr.txt": b"Bonjour le monde",
        "greeting.de.txt": b"Hallo Weld",
    }

    for name, body in files.items():
        client.put_object(Bucket="bukkit-1", Key=f"dir1/{name}", Body=body)

    copy_s3_prefix(
        client,
        src_bucket="bukkit-1",
        src_prefix="dir1/",
        dst_bucket="bukkit-2",
        dst_prefix="dir2/",
    )

    dst_objects = {}
    for dst_key in list_s3_prefix(client, bucket="bukkit-2"):
        body = client.get_object(Bucket="bukkit-2", Key=dst_key)["Body"].read()
        dst_objects[dst_key] = body

    assert dst_objects == {f"dir2/{name}": body for name, body in files.items()}
