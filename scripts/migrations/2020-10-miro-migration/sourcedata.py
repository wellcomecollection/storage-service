#!/usr/bin/env python3
"""
Load miro-source-data from dynamodb
"""

import attr
from dynamo import gather_table_items

PLATFORM_ROLE_ARN = "arn:aws:iam::760097843905:role/platform-read_only"
MIRO_SOURCEDATA_TABLE = "vhs-sourcedata-miro"


@attr.s
class MiroSourceItem:
    id = attr.ib()
    cleared = attr.ib()


def count_sourcedata():
    return 421608


def gather_sourcedata():
    items = gather_table_items(
        role_arn=PLATFORM_ROLE_ARN, table_name=MIRO_SOURCEDATA_TABLE
    )

    for item in items:
        yield MiroSourceItem(id=item["id"], cleared=item["isClearedForCatalogueAPI"])
