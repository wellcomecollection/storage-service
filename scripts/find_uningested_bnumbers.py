#!/usr/bin/env python
# -*- encoding: utf-8
"""
Given a dump of the DynamoDB ingests table and all the b numbers known by the
bagger, print a list of b numbers which haven't been ingested at all.
"""

import sys

from dynamodb import cached_scan_iterator


def get_ingested_bnumbers(ingests_json):
    for ingest in cached_scan_iterator(ingests_json):
        if (
            ingest["payload"]["space"] == "digitised"
            and ingest["payload"]["status"] == "Completed"
        ):
            yield ingest["payload"]["externalIdentifier"]


def get_mets_bnumbers(bnumbers_txt):
    for line in open(bnumbers_txt):
        yield line.strip()


if __name__ == "__main__":
    try:
        ingests_json = sys.argv[1]
        bnumbers_txt = sys.argv[2]
    except IndexError:
        sys.exit(f"Usage: {__file__} <INGESTS_JSON> <BNUMBERS_TXT>")

    ingested_bnumbers = set(get_ingested_bnumbers(ingests_json))
    mets_bnumbers = set(get_mets_bnumbers(bnumbers_txt))

    assert (
        not ingested_bnumbers - mets_bnumbers
    ), f"We've ingested b numbers the bagger doesn't recognise: {ingested_bnumbers - mets_bnumbers}"

    for b_number in sorted(mets_bnumbers - ingested_bnumbers):
        print(b_number)
