#!/usr/bin/env python3
"""
This is a script to track the Azure verifier progress of Chemist & Druggist,
which has ~1M files in the bag.
"""

import itertools
import json
import math
import sys

import boto3
import tqdm


dynamodb = boto3.resource("dynamodb").meta.client


def chunked_iterable(iterable, *, size):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk


def get_chemist_and_druggist_files():
    files = []

    for line in open("chemist_druggist.txt"):  # tag manifest from S3
        checksum, filename = line.strip().split("  ")
        files.append(filename)

    return files


def get_verified_files(files):
    for batch in tqdm.tqdm(
        chunked_iterable(files, size=100),
        total=int(math.ceil(len(files) / 100))
    ):
        resp = dynamodb.batch_get_item(
            RequestItems={
                'storage-prod_azure_verifier_tags': {
                    'Keys': [
                        {'id': f'azure://wellcomecollection-storage-replica-netherlands/digitised/b19974760/v1/{filename}'}
                        for filename in batch
                    ]
                }
            }
        )

        for item in resp["Responses"]["storage-prod_azure_verifier_tags"]:
            yield item["id"][len('azure://wellcomecollection-storage-replica-netherlands/digitised/b19974760/v1/'):]


if __name__ == "__main__":
    filenames = set(get_chemist_and_druggist_files())

    try:
        already_seen_filenames = set(json.load(open("seen_filenames.json")))
    except FileNotFoundError:
        already_seen_filenames = set()

    total_files = len(filenames)

    seen_filenames = set(get_verified_files(filenames - already_seen_filenames))

    with open("seen_filenames.json", "w") as outfile:
        outfile.write(json.dumps(list(seen_filenames) + list(already_seen_filenames)))

    files_verified = len(seen_filenames) + len(already_seen_filenames)

    print(
        f"This means {round(files_verified / total_files * 100, 1)}% "
        f"({files_verified} / {total_files}) of files have been verified"
    )
