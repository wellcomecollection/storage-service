#!/usr/bin/env python

import itertools
import json
import math

import tqdm

from reindex_bags_backlog import create_client, READ_ONLY_ROLE_ARN, ROLE_ARN, PROD_CONFIG


def chunked_iterable(iterable, size):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk


def get_chemist_druggist_bag():
    try:
        return json.load(open("b19974760.json"))
    except FileNotFoundError:
        pass

    dynamodb = create_client("dynamodb", role_arn=READ_ONLY_ROLE_ARN)
    s3 = create_client("s3", role_arn=READ_ONLY_ROLE_ARN)

    table_name = PROD_CONFIG["table_name"]

    item = dynamodb.get_item(
        TableName=table_name,
        Key={"id": {"S": "digitised/b19974760"}, "version": {"N": "1"}}
    )["Item"]

    bucket = item["payload"]["M"]["namespace"]["S"]
    key = item["payload"]["M"]["path"]["S"]

    s3.download_file(
        Bucket=bucket,
        Key=key,
        Filename="b19974760.json"
    )

    return json.load(open("b19974760.json"))


if __name__ == "__main__":
    bag = get_chemist_druggist_bag()

    sns = create_client("sns", role_arn=ROLE_ARN)

    for file_group in tqdm.tqdm(
        chunked_iterable(bag["manifest"]["files"], size=140),
        total=int(math.ceil(len(bag["manifest"]["files"]) / 140))
    ):
        contexts = [
            {
                "space": bag["space"],
                "externalIdentifier": bag["info"]["externalIdentifier"],
                "hashingAlgorithm": bag["manifest"]["checksumAlgorithm"],
                "bagLocation": bag["location"],
                "file": f,
                "createdDate": bag["createdDate"]
            }
            for f in file_group
        ]

        sns.publish(
            TopicArn="arn:aws:sns:eu-west-1:975596993436:storage_prod_file_finder_output",
            Message=json.dumps(contexts)
        )

    print(f"Sent {len(bag['manifest']['files'])} for indexing")
