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
