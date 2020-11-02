#!/usr/bin/env python3
"""
This is a script to assist in the migration of miro content
into the storage service.
"""

import click

from decisions import get_decisions, count_decisions
from elastic_helpers import get_local_elastic_client, index_iterator


@click.command()
@click.pass_context
def create_decisions_index(ctx):
    local_elastic_client = get_local_elastic_client()
    local_file_index = "decisions"

    expected_decision_count = count_decisions()

    def _documents():
        for decision in get_decisions():
            yield decision.s3_key, decision.as_dict()

    index_iterator(
        elastic_client=local_elastic_client,
        index_name=local_file_index,
        expected_doc_count=expected_decision_count,
        documents=_documents(),
    )


@click.group()
@click.pass_context
def cli(ctx):
    pass


cli.add_command(create_decisions_index)

if __name__ == "__main__":
    cli()
