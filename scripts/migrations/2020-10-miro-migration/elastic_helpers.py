#!/usr/bin/env python3

import json
import os
import time
import sys

import click
import elasticsearch
from elasticsearch import helpers, Elasticsearch
from elasticsearch.exceptions import NotFoundError
from tqdm import tqdm

from common import get_secret
from iter_helpers import chunked_iterable

LOCAL_ELASTIC_HOST = os.getenv("LOCAL_ELASTIC_HOST", "localhost")


def get_document_by_id(elastic_client, index_name, id):
    try:
        return elastic_client.get(index=index_name, id=id)['_source']
    except NotFoundError:
        return None

def get_elastic_client(role_arn, elastic_secret_id):
    secret = get_secret(role_arn, elastic_secret_id)

    return Elasticsearch(
        secret["endpoint"], http_auth=(secret["username"], secret["password"])
    )


def get_local_elastic_client(host=LOCAL_ELASTIC_HOST, port=9200):
    elastic_client = Elasticsearch(host=host, port=port)

    interval_time = 5
    max_retry_attempts = 12

    retry_attempts = 0

    while retry_attempts < max_retry_attempts:
        try:
            elastic_client.cluster.health()
            return elastic_client
        except elasticsearch.exceptions.ConnectionError:
            retry_attempts = retry_attempts + 1

            click.echo(
                f"{LOCAL_ELASTIC_HOST} not yet available, "
                f"tried {retry_attempts} times. "
                f"Retrying in {interval_time} seconds."
            )
            time.sleep(interval_time)

    click.echo(f"Elasticsearch host {LOCAL_ELASTIC_HOST} not available!")
    sys.exit(1)


def get_document_count(elastic_client, *, index):
    """
    How many documents are there in an Elasticsearch index?
    """
    try:
        return elastic_client.count(index=index)["count"]
    except elasticsearch.exceptions.NotFoundError:
        return 0


def index_iterator(
    elastic_client, index_name, documents, expected_doc_count=None, overwrite=False
):
    """
    Indexes documents from an iterator into elasticsearch
    """

    click.echo(f"Indexing {expected_doc_count} docs into {index_name}")
    elastic_client.indices.create(index=index_name, ignore=400)
    actual_doc_count = get_document_count(elastic_client, index=index_name)

    if not overwrite and actual_doc_count == expected_doc_count:
        click.echo(f"Already created index {index_name}, nothing to do")
        return

    click.echo(f"Recreating index ({index_name})")
    elastic_client.indices.delete(index=index_name, ignore=[400, 404])
    elastic_client.indices.create(index=index_name, ignore=400)

    bulk_actions = (
        {"_index": index_name, "_id": id, "_source": source}
        for batch in chunked_iterable(documents, size=500)
        for (id, source) in batch
    )

    successes, errors = helpers.bulk(elastic_client, actions=bulk_actions)

    if errors:
        click.echo(f"Errors indexing documents! {errors}")

    assert (
        successes == expected_doc_count
    ), f"Unexpected index success count: {successes}"


def save_index_to_disk(elastic_client, index_name, overwrite):
    """
    Saves an index to disk
    """

    document_count = get_document_count(elastic_client, index=index_name)

    query_body = {"query": {"match_all": {}}}

    all_documents = elasticsearch.helpers.scan(
        elastic_client, query=query_body, index=index_name
    )

    save_location = f"_cache/index_{index_name}.json"
    click.echo(f"Saving index {index_name} to {save_location}")

    if os.path.isfile(save_location):
        if not overwrite and not click.confirm(
            f"File exists at {save_location}, overwrite?"
        ):
            return None

        with open(f"_cache/index_{index_name}.json", "a") as f:
            f.truncate(0)

    with open(f"_cache/index_{index_name}.json", "a") as f:
        for document in tqdm(all_documents, total=document_count):
            f.write(f"{json.dumps(document)}\n")

    return save_location


def load_index_from_disk(elastic_client, index_name, target_index_name, overwrite):
    """
    Loads an index from disk
    """

    save_location = f"_cache/index_{index_name}.json"

    if not os.path.isfile(save_location):
        click.echo(f"No index file found at {save_location}")
        return

    line_count = sum(1 for _ in open(save_location))

    with open(f"_cache/index_{index_name}.json", "r") as f:

        def _documents():
            for line in f:
                doc = json.loads(line)
                yield doc["_id"], doc["_source"]

        index_iterator(
            elastic_client=elastic_client,
            index_name=target_index_name,
            expected_doc_count=line_count,
            documents=_documents(),
            overwrite=overwrite,
        )
