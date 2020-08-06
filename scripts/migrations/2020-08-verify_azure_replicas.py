#!/usr/bin/env python
"""
This is the skeleton of a script that verifies we have an Azure replica for every
bag in the storage service.

Part of https://github.com/wellcomecollection/platform/issues/4640
"""

from pprint import pprint

import termcolor

from common import get_aws_resource, get_storage_client, scan_table


dynamodb = get_aws_resource("dynamodb")

prod_client = get_storage_client("https://api.wellcomecollection.org/storage/v1")
stage_client = get_storage_client("https://api-stage.wellcomecollection.org/storage/v1")


def get_bag_identifiers():
    """
    Find every bag in the storage service.
    """
    for label, TableName in [
        ("stage", "vhs-storage-staging-manifests-2020-07-24"),
        ("prod", "vhs-storage-manifests-2020-07-24"),
    ]:
        for item in scan_table(TableName=TableName):
            space, external_identifier = item["id"].split("/", 1)
            yield {
                "label": label,
                "space": space,
                "external_identifier": external_identifier,
                "version": f"v{int(item['version'])}"
            }


def count_replicas(label, space, external_identifier, version):
    """
    How many replicas of this bag do we have?
    """
    # First look in the replica aggregator table.  This is a fast lookup that we
    # can use to see if a bag only has two replicas; if so, we can skip getting
    # the full storage manifest.
    replica_table_name = {
        "prod": "storage_replicas_table",
        "stage": "storage-staging_replicas_table",
    }[label]

    # Handle a trailing slash in the external identifier.  This is disallowed for
    # new bags, but we have a couple of pre-existing bags that use it.
    replica_id = f"{space}/{external_identifier}/{version}".replace("//", "/")

    try:
        item = dynamodb.Table(replica_table_name).get_item(
            Key={"id": replica_id}
        )["Item"]
    except KeyError:
        raise RuntimeError(f"Cannot find replica aggregator result for {replica_id}???")

    location = item["payload"]["location"]
    replicas = item["payload"]["replicas"]

    try:
        # There should be 1 or 2 secondary replicas (without or with Azure)
        assert len(replicas) in {1, 2}

        # The first replica is an S3 replica
        assert location.keys() == {"PrimaryS3ReplicaLocation"}

        # The second replica is S3
        assert replicas[0].keys() == {"SecondaryS3ReplicaLocation"}

        # The primary and secondary S3 buckets should be different
        assert (
            location["PrimaryS3ReplicaLocation"]["prefix"]["bucket"] !=
            replicas[0]["SecondaryS3ReplicaLocation"]["prefix"]["bucket"]
        )

        # The third replica (if present) is Azure
        if len(replicas) == 2:
            assert replicas[1].keys() == {"SecondaryAzureReplicaLocation"}
    except AssertionError:
        print("Something is wrong with this item:")
        pprint(item)
        raise

    # If there's only one secondary replica in the replica aggregator table, we
    # know there will only be one replica in the storage manifest.  We can skip
    # actually looking it up.
    if len(replicas) == 1:
        return 2

    # If there are three replicas in the aggregator table, we need to check they
    # all made it to the storage manifest.
    client = {
        "prod": prod_client,
        "stage": stage_client,
    }[label]

    bag = client.get_bag(space=space, external_identifier=external_identifier, version=version)

    try:
        assert bag["id"] == f"{space}/{external_identifier}"
        assert bag["version"] == version

        location = bag["location"]
        replicas = bag["replicaLocations"]

        assert len(replicas) in {1, 2}
        assert location["provider"]["id"] == "amazon-s3"
        assert replicas[0]["provider"]["id"] == "amazon-s3"

        assert location["bucket"] != replicas[0]["bucket"]

        if len(replicas) == 2:
            assert replicas[1]["provider"]["id"] == "azure-blob-storage"

        return len([location] + replicas)
    except AssertionError:
        print("Something is wrong with this bag:")
        pprint(bag)
        raise


if __name__ == "__main__":
    summary_replica_counts = {
        "stage": {2: 0, 3: 0},
        "prod": {2: 0, 3: 0},
    }

    for bag in get_bag_identifiers():
        # TODO: Add some caching here.  Once we've seen that a bag has three replicas,
        # we don't have to check again.
        result = count_replicas(**bag)

        if result == 2:
            print(termcolor.colored("✗", "red"), end="")
        elif result == 3:
            print(termcolor.colored("✓", "green"), end="")
        else:
            print(f"Unexpected replica count for bag: {result}, {bag}", file=sys.stderr)
            assert 0

        print(f" {bag['label']}: {bag['space']}/{bag['external_identifier']}/{bag['version']}")
        summary_replica_counts[bag["label"]][result] += 1

        # Only check the bags in staging for now; that's what we'll migrate first.
        if bag["label"] != "stage":
            break

    print("\n== SUMMARY ==")
    print("staging:")
    print(
        termcolor.colored("  %6d" % summary_replica_counts["stage"][2], "red"),
        "awaiting Azure replica"
    )
    print(
        termcolor.colored("  %6d" % summary_replica_counts["stage"][3], "green"),
        "replicated to Azure"
    )

    print("\nprod:")
    print(
        termcolor.colored("  %6d" % summary_replica_counts["prod"][2], "red"),
        "awaiting Azure replica"
    )
    print(
        termcolor.colored("  %6d" % summary_replica_counts["prod"][3], "green"),
        "replicated to Azure"
    )
