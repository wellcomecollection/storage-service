#!/usr/bin/env python3
"""
This script will send every bag in the storage service to the bags indexer
to be re-indexed in Elasticsearch.
"""
from pprint import pprint

import click
from elasticsearch.helpers import scan

from bags import get_latest_bags, gather_bags, publish_bags
from clients import create_aws_client, create_es_client
from chunked_diff import chunked_diff

STAGE_CONFIG = {
    "table_name": "vhs-storage-staging-manifests-2020-07-24",
    "topic_arn": "arn:aws:sns:eu-west-1:975596993436:storage-staging_bag_reindexer_output",
    "es_index": "storage_stage_bags",
}

PROD_CONFIG = {
    "table_name": "vhs-storage-manifests-2020-07-24",
    "topic_arn": "arn:aws:sns:eu-west-1:975596993436:storage-prod_bag_reindexer_output",
    "es_index": "storage_bags",
}


def get_config(env):
    if env == "prod":
        return PROD_CONFIG
    else:
        return STAGE_CONFIG


def confirm_indexed(elastic_client, published_bags, index):
    print(f"\nConfirm indexed to {index}")

    def _query(ids):
        query_body = {"query": {"ids": {"values": ids}}}
        scan_response = scan(
            elastic_client, index=index, query=query_body, _source=False
        )
        found_ids = [hit["_id"] for hit in scan_response]

        return set(ids).difference(found_ids)

    flat_list = chunked_diff(diff_for_chunk=_query, all_entries=published_bags)
    print(f"Found {len(flat_list)} not indexed.\n")
    return flat_list


@click.group()
def cli():
    pass


@click.command()
@click.option("--env", default="stage", help="Environment to run against (prod|stage)")
@click.option(
    "--ids",
    default=[],
    help="Specific Bag to reindex (will not scan for all bags)",
    multiple=True,
)
@click.option("--dry_run", default=False, is_flag=True, help="Do not publish messages")
def publish(env, ids, dry_run):
    config = get_config(env)

    dynamodb_client = create_aws_client("dynamodb")
    sns_client = create_aws_client("sns")

    if not ids:
        bags_to_publish = get_latest_bags(
            dynamodb_client, table_name=config["table_name"]
        )
    else:
        bags_to_publish = gather_bags(
            dynamodb_client, table_name=config["table_name"], bag_ids=ids
        )

    publish_bags(sns_client, config["topic_arn"], bags_to_publish, dry_run)


@click.command()
@click.option("--env", default="stage", help="Environment to run against (prod|stage)")
@click.option(
    "--ids",
    default=[],
    help="Specific Bag to confirm (will not scan for all bags)",
    multiple=True,
)
@click.option(
    "--republish", default=False, is_flag=True, help="If not indexed, republish"
)
def confirm(env, ids, republish):
    config = get_config(env)

    dynamodb_client = create_aws_client("dynamodb")
    sns_client = create_aws_client("sns")
    elastic_client = create_es_client(env=env, indexer_type="bags")

    if not ids:
        latest_bags = get_latest_bags(dynamodb_client, table_name=config["table_name"])
    else:
        latest_bags = gather_bags(
            dynamodb_client, table_name=config["table_name"], bag_ids=ids
        )

    bags_to_confirm = [key for (key, value) in latest_bags.items()]

    not_indexed = confirm_indexed(elastic_client, bags_to_confirm, config["es_index"])

    if not_indexed:
        print(f"NOT INDEXED: {len(not_indexed)}")
        if republish:
            print(f"Republishing {len(not_indexed)} missing bags.")
            latest_bags = {bag_id: latest_bags[bag_id] for bag_id in not_indexed}
            publish_bags(sns_client, config["topic_arn"], latest_bags)
        else:
            pprint(not_indexed)
    else:
        print(f"{len(latest_bags)} bags published.\n")


cli.add_command(publish)
cli.add_command(confirm)

if __name__ == "__main__":
    cli()
