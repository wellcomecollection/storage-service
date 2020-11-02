#!/usr/bin/env python3

import os

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
    return Elasticsearch(host=host, port=port)


def get_document_count(elastic_client, *, index):
    """
    How many documents are there in an Elasticsearch index?
    """
    try:
        return elastic_client.count(index=index)["count"]
    except elasticsearch.exceptions.NotFoundError:
        return 0


def index_iterator(elastic_client, index_name, documents, expected_doc_count=None):
    """
    Indexes documents from an iterator into elasticsearch
    """

    click.echo(f"Indexing {expected_doc_count} docs into {index_name}")
    elastic_client.indices.create(index=index_name, ignore=400)
    actual_doc_count = get_document_count(elastic_client, index=index_name)

    if actual_doc_count == expected_doc_count:
        click.echo(f"Already created index {index_name}, nothing to do")
        return

    click.echo(f"Recreating files index ({index_name})")
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

    updated_doc_count = get_document_count(elastic_client, index=index_name)

    assert (
        successes == expected_doc_count
    ), f"Unexpected index success count: {successes}"
    assert (
        updated_doc_count == expected_doc_count
    ), f"Unexpected index doc count: {updated_doc_count}"
