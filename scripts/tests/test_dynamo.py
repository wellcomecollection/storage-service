import secrets

import boto3
import moto
import pytest

from helpers import bulk_delete_dynamo_items, delete_dynamo_item


@pytest.fixture
def client():
    with moto.mock_dynamodb2():
        yield boto3.resource("dynamodb", region_name="eu-west-1").meta.client


def test_can_delete_single_item(client):
    client.create_table(
        TableName="my-table",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "N"}],
    )

    for i in range(5):
        client.put_item(
            TableName="my-table", Item={"id": i, "value": secrets.token_bytes()}
        )

    # We've written 5 items to the table
    assert len(client.scan(TableName="my-table")["Items"]) == 5

    # After we delete a single item, there are four items in the table
    delete_dynamo_item(client, table_name="my-table", key={"id": 4})
    assert len(client.scan(TableName="my-table")["Items"]) == 4

    actual_items = {item["id"] for item in client.scan(TableName="my-table")["Items"]}
    assert actual_items == {0, 1, 2, 3}


def test_can_delete_multiple_items(client):
    client.create_table(
        TableName="my-table",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "N"}],
    )

    for i in range(50):
        client.put_item(
            TableName="my-table", Item={"id": i, "value": secrets.token_bytes()}
        )

    # We've written 50 items to the table
    assert len(client.scan(TableName="my-table")["Items"]) == 50

    # We can delete up to 25 items in a single batch, so delete more than 25
    bulk_delete_dynamo_items(
        client, table_name="my-table", keys=[{"id": i} for i in range(30)]
    )

    # 50 - 30 = 20
    assert len(client.scan(TableName="my-table")["Items"]) == 20

    actual_items = {item["id"] for item in client.scan(TableName="my-table")["Items"]}
    assert actual_items == set(range(30, 50))
