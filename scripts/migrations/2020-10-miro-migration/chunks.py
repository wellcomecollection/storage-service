#!/usr/bin/env python3
"""
Functions related to gathering migration chunk information
for creating transfer packages
"""

import collections
import json

import attr
import click
import elasticsearch

from decisions import Decision
from elastic_helpers import get_local_elastic_client


@attr.s
class Chunk:
    group_name = attr.ib()
    destination = attr.ib()
    s3_keys = attr.ib(default=list)
    transfer_package = attr.ib(default=None)

    def merge_chunk(self, other):
        assert other.chunk_id() == self.chunk_id()
        self.s3_keys = self.s3_keys + other.s3_keys

    def chunk_id(self):
        return f"{self.destination}/{self.group_name}"


def gather_chunks(local_decisions_index):
    local_elastic_client = get_local_elastic_client()

    query_body = {
        "query": {
            "bool": {"must_not": [{"term": {"skip": True}}]}
        }
    }

    total_chunkable_decisions = local_elastic_client.count(
        body=query_body, index=local_decisions_index
    )["count"]

    chunkable_decisions = elasticsearch.helpers.scan(
        local_elastic_client, query=query_body, index=local_decisions_index
    )

    click.echo(f"Gathering chunks from {total_chunkable_decisions} decisions.")

    # Dict (shard, destination) -> set(s3 keys)
    groups = collections.defaultdict(set)

    for result in chunkable_decisions:
        decision = Decision(**result["_source"])
        for destination in decision.destinations:
            groups[(decision.group_name, destination)].add(decision.s3_key)

    click.echo(f"Found {len(groups)} chunks.")

    return [Chunk(
        group_name=group_name,
        destination=destination,
        s3_keys=s3_keys,
    ) for (group_name, destination), s3_keys in groups.items()]


if __name__ == "__main__":
    for chunk in gather_chunks('decisions'):
        print(json.dumps(attr.asdict(chunk)))
