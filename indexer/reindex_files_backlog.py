#!/usr/bin/env python3
"""
This script will send every bag in the storage service to the file indexer
to be re-indexed in Elasticsearch.
"""

import concurrent.futures
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

    messages = []

    for item in tqdm.tqdm(scan_table(dynamodb_client, TableName=table_name)):
        space, externalIdentifier = item["id"]["S"].split("/", 1)
        version = int(item["version"]["N"])
        notification = {
            "space": space,
            "externalIdentifier": externalIdentifier,
            "version": f"v{version}",
            "type": "RegisteredBagNotification",
        }

        messages.append(json.dumps(notification))

    def publish(message):
        return sns_client.publish(TopicArn=topic_arn, Message=message)

    message_count = len(messages)
    messages = iter(messages)

    print(f"\nPublishing {message_count} notifications to {topic_arn}")
    with tqdm.tqdm(total=message_count) as progress_bar:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Schedule the first N futures.  We don't want to schedule them all
            # at once, to avoid consuming excessive amounts of memory.
            futures = {
                executor.submit(publish, msg)
                for msg in itertools.islice(messages, max_parallel_notifications)
            }

            while futures:
                # Wait for the next future to complete.
                done, futures = concurrent.futures.wait(
                    futures, return_when=concurrent.futures.FIRST_COMPLETED
                )

                for fut in done:
                    fut.result()

                progress_bar.update(len(done))

                # Schedule the next set of futures.  We don't want more than N futures
                # in the pool at a time, to keep memory consumption down.
                for msg in itertools.islice(messages, len(done)):
                    futures.add(executor.submit(publish, msg))

    print(f"Published notifications for {message_count} bags.\n")


if __name__ == "__main__":
    main()
