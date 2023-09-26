from tqdm import tqdm

from messaging import publish_notifications


def get_table_count(dynamodb_client, *, table_name):
    resp = dynamodb_client.describe_table(TableName=table_name)
    return resp["Table"]["ItemCount"]


def scan_table(dynamodb_client, *, TableName, **kwargs):
    paginator = dynamodb_client.get_paginator("scan")

    for page in paginator.paginate(TableName=TableName, **kwargs):
        yield from page["Items"]


def get_latest_bags(dynamodb_client, *, table_name):
    total_bags = get_table_count(dynamodb_client, table_name=table_name)

    print(f"\nGetting latest version of bags from {table_name}")

    bags = {}
    seen_bags = 0

    for item in tqdm(
        scan_table(dynamodb_client, TableName=table_name), total=total_bags
    ):
        dynamo_id = item["id"]["S"]
        version = int(item["version"]["N"])

        # Don't index Chemist & Druggist with this script.  It's too big to index
        # normally, and we have a separate script that breaks it into smaller,
        # indexable-sized pieces.
        #
        # Trying to index C&D like other bags causes an outage in the bags API.
        if dynamo_id == "digitised/b19974760":
            continue

        stored_version = bags.get(dynamo_id, -1)

        seen_bags = seen_bags + 1

        if version > stored_version:
            bags[dynamo_id] = version

    print(f"Found {len(bags)} bags.\n")

    return bags


def gather_bags(dynamodb_client, *, table_name, bag_ids):
    split_bag_ids = bag_ids.split(",")
    bags = [
        get_bag(dynamodb_client, table_name=table_name, bag_id=bag_id)
        for bag_id in split_bag_ids
    ]

    bags_to_publish = {}
    for bag in bags:
        bags_to_publish.update(bag)

    return bags_to_publish


def get_bag(dynamodb_client, *, table_name, bag_id):
    response = dynamodb_client.query(
        TableName=table_name,
        KeyConditionExpression="id = :bag_id",
        ExpressionAttributeValues={":bag_id": {"S": bag_id}},
    )

    max_version = max(int(item["version"]["N"]) for item in response["Items"])

    return {bag_id: max_version}


def fake_notification(space, externalIdentifier, version):
    return {
        "space": space,
        "externalIdentifier": externalIdentifier,
        "version": f"v{version}",
        "type": "RegisteredBagNotification",
    }


def publish_bags(sns_client, *, topic_arn, bags, dry_run=False):
    unique_bags = len(bags)

    print(f"\nGenerating notifications for {unique_bags} bags.")
    payloads = []
    for dynamo_id, version in tqdm(bags.items(), total=unique_bags):
        space, external_id = dynamo_id.split("/", 1)
        payload = fake_notification(space, external_id, version)
        payloads.append(payload)

    print(f"Prepared notifications for {len(payloads)} bags.\n")
    publish_notifications(
        sns_client, topic_arn=topic_arn, payloads=payloads, dry_run=dry_run
    )
    print(f"Published notifications for {len(payloads)} bags.\n")
