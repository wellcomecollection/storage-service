import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from _aws import get_dynamo_client, scan_table  # noqa


dynamodb = get_dynamo_client()


def has_been_replicated_to_azure(backfill_table, *, space, externalIdentifier, version):
    """
    Returns True if a bag has been replicated to Azure -- that is, if it has
    an entry in the VHS created for the backfill process.
    """
    resp = dynamodb.get_item(
        TableName=backfill_table,
        Key={"id": f"{space}/{externalIdentifier}", "version": version},
    )

    return "Item" in resp


def get_bags(vhs_table):
    """
    Generates all the bags in the VHS table that have not been replicated to Azure.
    """
    for item in scan_table(TableName=vhs_table):
        space, externalIdentifier = item["id"].split("/", 1)

        assert int(item["version"]) == item["version"]
        version = int(item["version"])
        yield (space, externalIdentifier, version)
