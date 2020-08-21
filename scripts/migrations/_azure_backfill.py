import functools
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from _aws import scan_table  # noqa


def has_been_replicated_to_azure(backfill_table, *, space, externalIdentifier, version):
    """
    Returns True if a bag has been replicated to Azure -- that is, if it has
    an entry in the VHS created for the backfill process.
    """
    backfilled_bags = get_already_backfilled_bags(backfill_table)

    return (space, externalIdentifier, version) in backfilled_bags


def get_bags(vhs_table):
    """
    Generates all the bags in the VHS table.
    """
    for item in scan_table(TableName=vhs_table):
        space, externalIdentifier = item["id"].split("/", 1)

        assert int(item["version"]) == item["version"]
        version = int(item["version"])
        yield (space, externalIdentifier, version)


@functools.lru_cache()
def get_already_backfilled_bags(backfill_table):
    return set(get_bags(backfill_table))
