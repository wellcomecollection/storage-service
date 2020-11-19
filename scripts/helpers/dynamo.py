from .iterators import chunked_iterable


def delete_dynamo_item(dynamo_client, *, table_name, key):
    """
    Delete a single item from a DynamoDB table.
    """
    dynamo_client.delete_item(TableName=table_name, Key=key)


def bulk_delete_dynamo_items(dynamo_client, *, table_name, keys):
    """
    Bulk delete items in DynamoDB.
    """
    # We can delete up to 25 items in a single BatchWriteItem call.
    for batch in chunked_iterable(keys, size=25):
        dynamo_client.batch_write_item(
            RequestItems={
                table_name: [{"DeleteRequest": {"Key": key}} for key in batch]
            }
        )


def list_dynamo_tables(dynamo_client):
    """
    Lists all the DynamoDB tables in an AWS account.
    """
    for page in dynamo_client.get_paginator("list_tables").paginate():
        yield from page["TableNames"]


def find_manifests_dynamo_table(dynamo_client, *, table_prefix):
    """
    The VHS manifests table varies -- in particular it typically has a date suffix.

    Return the name of the manifests table.
    """
    matching_tables = [
        t
        for t in list_dynamo_tables(dynamo_client)
        if t.startswith(f"vhs-{table_prefix}-manifests-")
    ]

    if len(matching_tables) == 0:
        raise RuntimeError("Could not work out the VHS manifests table!")
    elif len(matching_tables) == 1:
        return matching_tables[0]
    else:
        raise RuntimeError(
            f"Ambiguous choice of VHS manifests table: {', '.join(matching_tables)}"
        )
