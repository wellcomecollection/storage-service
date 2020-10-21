#!/usr/bin/env python3
"""
This script will send every bag in the storage service to the file indexer
to be re-indexed in Elasticsearch.
"""

import json

import click
import tqdm

from reindex_bags_backlog import ROLE_ARN, get_config, create_client, scan_table


@click.command()
@click.option("--env", default="stage", help="Environment to run against (prod|stage)")
def main(env):
    config = get_config(env)
    dynamodb_client = create_client("dynamodb", role_arn=ROLE_ARN)
    sns_client = create_client("sns", role_arn=ROLE_ARN)

    table_name = config["table_name"]

    if env == "prod":
        topic_arn = "arn:aws:sns:eu-west-1:975596993436:storage_prod_file_reindexer_output"
    else:
        topic_arn = "arn:aws:sns:eu-west-1:975596993436:storage_staging_file_reindexer_output"

    for item in tqdm.tqdm(scan_table(dynamodb_client, TableName=table_name)):
        space, externalIdentifier = item["id"]["S"].split("/", 1)
        version = int(item["version"]["N"])
        notification = {
            "space": space,
            "externalIdentifier": externalIdentifier,
            "version": f"v{version}",
            "type": "RegisteredBagNotification",
        }

        sns_client.publish(TopicArn=topic_arn, Message=json.dumps(notification))


if __name__ == "__main__":
    main()
