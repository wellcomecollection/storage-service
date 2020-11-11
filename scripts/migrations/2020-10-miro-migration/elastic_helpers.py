#!/usr/bin/env python3

import os
import time
import sys

import click
import elasticsearch
from elasticsearch import helpers, Elasticsearch

from common import get_secret
from iter_helpers import chunked_iterable

LOCAL_ELASTIC_HOST = os.getenv("LOCAL_ELASTIC_HOST", "localhost")


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


def index_iterator(elastic_client, index_name, documents, expected_doc_count=None, overwrite=False):
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
