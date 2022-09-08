#!/usr/bin/env python3
"""
This script will send every bag in the storage service to the file indexer
to be re-indexed in Elasticsearch.
"""
import math
from pprint import pprint

import click
from tqdm import tqdm

from bags import gather_bags, get_latest_bags, publish_bags
from clients import create_es_client, create_aws_client

STAGE_CONFIG = {
    "table_name": "vhs-storage-staging-manifests-2020-07-24",
    "topic_arn": "arn:aws:sns:eu-west-1:975596993436:storage-staging_file_reindexer_output",
    "es_index": "storage_stage_files",
}

PROD_CONFIG = {
    "table_name": "vhs-storage-manifests-2020-07-24",
    "topic_arn": "arn:aws:sns:eu-west-1:975596993436:storage-prod_file_reindexer_output",
    "es_index": "storage_files",
}


def get_config(env):
    if env == "prod":
        return PROD_CONFIG
    else:
        return STAGE_CONFIG


def confirm_indexed(elastic_client, published_bags, index):
    print(f"\nConfirm indexed to {index}")

    def _chunks(big_list, chunk_length):
        for i in range(0, len(big_list), chunk_length):
            yield big_list[i : i + chunk_length]

    def _query(bag_ids):
        external_identifiers = {id.split("/", 1)[1]: id for id in bag_ids}
        response = elastic_client.search(
            index=index,
            query={"terms": {"externalIdentifier": list(external_identifiers.keys())} },
            size=0,
            aggregations={
                "externalIdentifier": {
                    "terms": {
                        "field": "externalIdentifier",
                        "size": len(external_identifiers)
                    }
                }
            }
        )
        agg_buckets = response.body["aggregations"]["externalIdentifier"]["buckets"]
        found_external_ids = set(b["key"] for b in agg_buckets)
        missing_external_ids = set(external_identifiers.keys()).difference(found_external_ids)
        return set(external_identifiers[id] for id in missing_external_ids)


    chunk_length = 500
    chunk_count = math.ceil(len(published_bags) / chunk_length)

    diff_list = []
    for chunk in tqdm(_chunks(published_bags, chunk_length), total=chunk_count):
        diff_list.append(_query(chunk))

    flat_list = [item for sublist in diff_list for item in sublist]

    print(f"Found {len(flat_list)} not indexed.\n")

    return flat_list

@click.group()
def cli():
    pass


@click.command()
@click.option("--env", default="stage", help="Environment to run against (prod|stage)")
@click.option(
    "--ids", default=[], help="Specific Bag to confirm (will not scan for all bags)", multiple=True
)
@click.option(
    "--republish", default=False, is_flag=True, help="If not indexed, republish"
)
def confirm(env, ids, republish):
    config = get_config(env)
    dynamodb_client = create_aws_client("dynamodb")
    sns_client = create_aws_client("sns")
    elastic_client = create_es_client(env=env, indexer_type="files")

    if not ids:
        latest_bags = get_latest_bags(dynamodb_client, table_name=config["table_name"])
    else:
        latest_bags = gather_bags(dynamodb_client, table_name=config["table_name"], bag_ids=ids)

    bags_to_confirm = [key for (key, value) in latest_bags.items()]
    not_indexed = confirm_indexed(elastic_client, bags_to_confirm, config["es_index"])

    if not_indexed:
        print(f"NOT INDEXED: {len(not_indexed)}")
        if republish:
            print(f"Republishing missing files from {len(not_indexed)} bags.")
            latest_bags = {bag_id: latest_bags[bag_id] for bag_id in not_indexed}
            publish_bags(sns_client, topic_arn=config["topic_arn"], bags=latest_bags)
        else:
            pprint(not_indexed)
    else:
        print(f"{len(latest_bags)} bags published.\n")


cli.add_command(confirm)

if __name__ == "__main__":
    cli()
