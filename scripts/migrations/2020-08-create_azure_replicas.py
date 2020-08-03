#!/usr/bin/env python
"""
This is the skeleton of a script that can be used to add a replica in Azure to
bags that only have replicas in S3 and Glacier.

Part of https://github.com/wellcomecollection/platform/issues/4640
"""

from pprint import pprint

from common import scan_table


def find_incomplete_replicas(replicas_table):
    """
    Find all the bags that don't have a replica in Azure.
    """
    for item in scan_table(TableName=replicas_table):

        # Check the structure is sensible: we expect an S3 primary replica,
        # and one S3 secondary replica and (optionally) an Azure replica,
        # depending on whether this bag has already been replicated to Azure.
        #
        # Flag any replica records that don't match this structure; something
        # weird is going on.
        try:
            assert item["payload"]["location"].keys() == {"PrimaryS3ReplicaLocation"}
            assert item["payload"]["replicas"][0].keys() == {
                "SecondaryS3ReplicaLocation"
            }
            assert 1 <= len(item["payload"]["replicas"]) <= 2

            if len(item["payload"]["replicas"]) == 2:
                assert item["payload"]["replicas"][1].keys() == {
                    "SecondaryAzureReplicaLocation"
                }
        except AssertionError:
            pprint(item)
            raise

        # If the bag has two replicas, we know one of them is in Glacier and
        # one is in Azure, so it's complete.
        if len(item["payload"]["replicas"]) == 2:
            continue

        yield item


if __name__ == "__main__":
    namespace_config = {
        "stage": "storage-staging",
        "prod": "storage",
    }

    namespace = namespace_config["stage"]

    replicas_table = f"{namespace}_replicas_table"

    for replica in find_incomplete_replicas(replicas_table):
        pprint(replica)
        break
