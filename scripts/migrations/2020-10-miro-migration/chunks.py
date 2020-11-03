#!/usr/bin/env python3
"""
Functions related to gathering migration chunk information
for creating transfer packages
"""

import attr
import click
import elasticsearch
import tqdm

from decisions import Decision
from elastic_helpers import get_local_elastic_client


@attr.s
class Chunk:
    miro_shard = attr.ib()
    destination = attr.ib()
    s3_keys = attr.ib(default=[])

    def merge_chunk(self, chunk):
        assert chunk.chunk_id() == self.chunk_id()
        self.s3_keys = self.s3_keys + chunk.s3_keys

    def chunk_id(self):
        return f"{self.miro_shard}/{self.destination}"


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

    groups = {}

    click.echo(f"Gathering chunks from {total_chunkable_decisions} decisions.")
    for result in tqdm.tqdm(chunkable_decisions, total=total_chunkable_decisions):
        decision = Decision(**result["_source"])

        for destination in decision.destinations:
            new_chunk = Chunk(
                miro_shard=decision.miro_shard,
                destination=destination,
                s3_keys=[decision.s3_key],
            )

            chunk_id = new_chunk.chunk_id()

            if chunk_id in groups:
                groups[chunk_id].merge_chunk(new_chunk)
            else:
                groups[chunk_id] = new_chunk

    click.echo(f"Found {len(groups)} chunks.")

    return groups.values()
