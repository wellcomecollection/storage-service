#!/usr/bin/env python3
"""
Functions related to gathering migration chunk information
for creating transfer packages
"""

import collections

import attr
import click
import elasticsearch

from decisions import Decision
from elastic_helpers import get_local_elastic_client


@attr.s
class Chunk:
    miro_shard = attr.ib()
    destination = attr.ib()
    s3_keys = attr.ib(default=list)
    transfer_package = attr.ib(default=None)

    def merge_chunk(self, other):
        assert other.chunk_id() == self.chunk_id()
        self.s3_keys = self.s3_keys + other.s3_keys

    def chunk_id(self):
        return f"{self.miro_shard}-{self.destination}"


def gather_chunks(local_decisions_index):
    local_elastic_client = get_local_elastic_client()

    query_body = {
        "query": {
            "bool": {"must_not": [{"term": {"skip": True}}, {"term": {"defer": True}}]}
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
            groups[(decision.miro_shard, destination)].add(decision.s3_key)

    click.echo(f"Found {len(groups)} chunks.")

    return [Chunk(
        miro_shard=miro_shard,
        destination=destination,
        s3_keys=s3_keys,
    ) for (miro_shard, destination), s3_keys in groups.items()]
