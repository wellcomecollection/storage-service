#!/usr/bin/env python
"""
This script will send every ingest in the storage service to the ingests indexer
to be re-indexed in Elasticsearch.
"""
from boto3.dynamodb.types import TypeDeserializer
from pprint import pprint
import click
import datetime
from tqdm import tqdm
from elasticsearch.helpers import scan

from bags import scan_table, get_table_count
from clients import create_aws_client, create_es_client
from chunked_diff import chunked_diff
from messaging import publish_notifications

STAGE_CONFIG = {
    "table_name": "storage-staging-ingests",
    "topic_arn": "arn:aws:sns:eu-west-1:975596993436:storage-staging_updated_ingests",
    "es_index": "storage_stage_ingests",
}

PROD_CONFIG = {
    "table_name": "storage-ingests",
    "topic_arn": "arn:aws:sns:eu-west-1:975596993436:storage-prod_updated_ingests",
    "es_index": "storage_ingests",
}


def get_config(env):
    if env == "prod":
        return PROD_CONFIG
    else:
        return STAGE_CONFIG


@click.group()
def cli():
    pass


def as_date(decimal_value):
    return datetime.datetime.fromtimestamp(decimal_value / 1000).isoformat() + "Z"


def get_first_key(dict):
    return next(iter(dict))


def get_ingests(dynamodb_client, *, table_name):
    """
    Generates all the ingests from a DynamoDB table.
    """
    print(f"Getting latest ingests from {table_name}")
    total_ingests = get_table_count(dynamodb_client, table_name=table_name)

    deserializer = TypeDeserializer()
    for item in tqdm(
        scan_table(dynamodb_client, TableName=table_name), total=total_ingests
    ):
        ingest = {k: deserializer.deserialize(v) for k, v in item.items()}["payload"]

        # Modify the structure to match the JSON serialisation of the
        # Scala libraries.  Ideally we'd use Scala directly, but getting that
        # to work is more effort than I care to spend right now.
        try:
            ingest["callback"]["status"] = {
                "type": get_first_key(ingest["callback"]["status"])
            }
        except KeyError:
            pass

        try:
            ingest["version"] = int(ingest["version"])
        except KeyError:
            pass

        # Note: ingest_events can be ``None``
        ingest_events = ingest.get("events", []) or []
        for event in ingest_events:
            event["createdDate"] = as_date(event["createdDate"])

        ingest["createdDate"] = as_date(ingest["createdDate"])
        ingest["ingestType"] = {"id": ingest["ingestType"]}
        ingest["status"] = {"type": get_first_key(ingest["status"])}

        source_location_type = get_first_key(ingest["sourceLocation"])
        ingest["sourceLocation"] = {
            "type": source_location_type,
            "location": ingest["sourceLocation"][source_location_type]["location"],
        }

        if ingest["status"]["type"] == "Completed":
            ingest["status"]["type"] = "Succeeded"

        yield ingest


def confirm_indexed(elastic_client, *, ingests_to_confirm, index):
    print(f"\nConfirm indexed to {index}")

    def _query(ids):
        query_body = {"query": {"ids": {"values": ids}}}
        scan_response = scan(
            elastic_client, index=index, query=query_body, _source=False
        )
        found_ids = [hit["_id"] for hit in scan_response]

        return set(ids).difference(found_ids)

    flat_list = chunked_diff(diff_for_chunk=_query, all_entries=ingests_to_confirm)
    print(f"Found {len(flat_list)} not indexed.\n")
    return flat_list


@click.command()
@click.option("--env", default="stage", help="Environment to run against (prod|stage)")
@click.option(
    "--republish", default=False, is_flag=True, help="If not indexed, republish"
)
def confirm(env, republish):
    config = get_config(env)
    dynamodb_client = create_aws_client("dynamodb")
    sns_client = create_aws_client("sns")
    elastic_client = create_es_client(env=env, indexer_type="ingests")

    latest_ingests = list(get_ingests(dynamodb_client, table_name=config["table_name"]))
    ingest_ids = [ingest["id"] for ingest in latest_ingests]

    not_indexed = set(
        confirm_indexed(
            elastic_client, ingests_to_confirm=ingest_ids, index=config["es_index"]
        )
    )
    if not_indexed:
        print(f"NOT INDEXED: {len(not_indexed)}")
        if republish:
            print(f"Republishing {len(not_indexed)} missing ingests.")
            missing_ingests = [
                ingest for ingest in latest_ingests if ingest["id"] in not_indexed
            ]
            publish_notifications(
                sns_client, topic_arn=config["topic_arn"], payloads=missing_ingests
            )
        else:
            pprint(not_indexed)
    else:
        print(f"{len(latest_ingests)} ingests published.\n")


cli.add_command(confirm)

if __name__ == "__main__":
    cli()
