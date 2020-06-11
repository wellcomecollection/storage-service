#!/usr/bin/env python
"""
Chemist & Druggist is massive. Enormous. Ridiculous.

The indexed bag is ~200MB of JSON, which is way bigger than what Elasticsearch
will let us write in a single request.  Rather than storing it as a single bag,
we split it into per-volume bags.
"""

import collections
import json
import re
import uuid

import boto3
from elasticsearch import Elasticsearch
from tenacity import stop_after_attempt, retry, wait_exponential
import tqdm


def create_boto3_client(service_name, *, role_arn):
    """
    Get a client for an AWS service with the given role ARN.
    """
    sts_client = boto3.client("sts")

    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn, RoleSessionName=uuid.uuid1().hex
    )

    credentials = assumed_role_object["Credentials"]

    session = boto3.Session(
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )

    return session.client(service_name)


def create_elastic_client():
    """
    Get an Elasticsearch client that has permission to write to the reporting cluster.
    """
    secretsmanager_client = create_boto3_client(
        "secretsmanager", role_arn="arn:aws:iam::975596993436:role/storage-developer"
    )

    def _get_secret(secret_id):
        response = secretsmanager_client.get_secret_value(SecretId=secret_id)

        return response["SecretString"]

    hostname = _get_secret(secret_id="prod/bag_indexer/es_host")
    username = _get_secret(secret_id="prod/bag_indexer/es_username")
    password = _get_secret(secret_id="prod/bag_indexer/es_password")
    protocol = _get_secret(secret_id="prod/bag_indexer/es_protocol")
    port = int(_get_secret(secret_id="prod/bag_indexer/es_port"))

    return Elasticsearch(
        hostname,
        http_auth=(username, password),
        scheme=protocol,
        port=port,
        # We need a long timeout here, because Elasticsearch gets slow when
        # running the update operations.
        timeout=300,
    )


def get_chemist_and_druggist_payload():
    """
    Get a copy of the JSON for the indexed C&D from S3.
    """
    s3_client = create_boto3_client(
        "s3", role_arn="arn:aws:iam::975596993436:role/storage-read_only"
    )

    s3_obj = s3_client.get_object(
        Bucket="wellcomecollection-storage-infra", Key="b19974760_indexed.json"
    )

    return json.load(s3_obj["Body"])


def decide_volume(filename):
    """
    Given the name of a file in C&D, which volume does it belong to?
    """
    # Root METS file
    if filename == "data/b19974760.xml":
        return "b19974760"

    # Individual METS files, e.g. data/b19974760_1.xml
    elif filename.startswith("data/b19974760_"):
        volume_no = int(
            re.match(r"^data/b19974760_(?P<volume>\d+)\.xml$", filename).group("volume")
        )

    # ALTO files, e.g. data/alto/b19974760M0001_0001.xml
    elif filename.startswith("data/alto/"):
        volume_no = int(
            re.match(r"^data/alto/b19974760M(?P<volume>\d{4})_\d{4}\.xml$", filename)
            .group("volume")
            .lstrip("0")
        )

    # JP2 files, e.g. data/objects/b19974760M0001_0001.jp2
    elif filename.startswith("data/objects/"):
        volume_no = int(
            re.match(r"^data/objects/b19974760M(?P<volume>\d{4})_\d{4}\.jp2$", filename)
            .group("volume")
            .lstrip("0")
        )

    else:
        raise ValueError(filename)

    return f"b19974760.{volume_no}"


def split_into_per_volume_bags(chemist_and_druggist):
    bag = chemist_and_druggist.copy()
    files_by_volume = collections.defaultdict(list)

    files = chemist_and_druggist["files"]

    for f in files:
        volume = decide_volume(f["name"])
        files_by_volume[volume].append(f)

    for volume_id, volume_files in files_by_volume.items():
        bag["files"] = volume_files
        bag["filesCount"] = len(bag["files"])
        bag["filesTotalSize"] = sum(f["size"] for f in bag["files"])
        yield volume_id, bag


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10))
def index_bag(es_client, *, elastic_id, bag):
    es_client.index(index="storage_bags", id=elastic_id, body=bag)


if __name__ == "__main__":
    es_client = create_elastic_client()

    chemist_and_druggist = get_chemist_and_druggist_payload()

    with tqdm.tqdm(total=len(chemist_and_druggist["files"])) as progress_bar:
        for elastic_id, bag in split_into_per_volume_bags(chemist_and_druggist):
            index_bag(es_client, elastic_id=elastic_id, bag=bag)
            progress_bar.update(len(bag["files"]))
