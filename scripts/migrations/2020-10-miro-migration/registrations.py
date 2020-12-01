#!/usr/bin/env python3
"""
Build registrations index
"""

import json

import elasticsearch
from collections import defaultdict
from elastic_helpers import (
    get_local_elastic_client,
    get_elastic_client,
)

FILES_REMOTE_INDEX = 'storage_files'
FILES_LOCAL_INDEX = 'files'
MIRO_FILES_QUERY = {"query": {"bool":
    {"must": [
        {"term": {"space": "miro"}}
    ]}
}}


def get_cleared_miro_ids(sourcedata_index):
    local_elastic_client = get_local_elastic_client()

    query_body = {"query": {"bool":
        {"must_not": [
            {"term": {"cleared": False}}
        ]}
    }}

    scan = elasticsearch.helpers.scan(
        local_elastic_client, query=query_body, index=sourcedata_index
    )

    ids = []
    for result in scan:
        ids.append(result["_source"]["id"])

    return set(ids)


def gather_registrations(decisions_index, cleared_miro_ids):
    local_elastic_client = get_local_elastic_client()

    query_body = {"query": {"bool":
                                {"must_not": [
                                    {"term": {"skip": True}}
                                ],
                                "must": [
                                    {"exists": {"field": "miro_id"}}
                                ]}
                            }}

    filtered_decisions_count = local_elastic_client.count(
        body=query_body, index=decisions_index
    )["count"]

    print(f"decision count: {filtered_decisions_count}")

    filtered_decisions = elasticsearch.helpers.scan(
        local_elastic_client, query=query_body, index=decisions_index
    )

    acc_miro_ids = defaultdict(list)
    miro_ids = []
    for result in filtered_decisions:
        miro_id = result["_source"]['miro_id']
        acc_miro_ids[miro_id].append(result["_source"])
        miro_ids.append(result["_source"]['miro_id'])

    miro_ids = set(miro_ids)

    print(f"unique miro_ids in decisions: {len(miro_ids)}")

    interesting_miro_ids = miro_ids.intersection(cleared_miro_ids)

    print(f"miro_ids.intersection(cleared_miro_ids): {len(interesting_miro_ids)}")

    missing_miro_ids = cleared_miro_ids - interesting_miro_ids

    print(f"missing miro_ids: {len(missing_miro_ids)}")

    with open('missing_miro_ids.json', 'w') as outfile:
        json.dump(list(missing_miro_ids), outfile)

    ambig_decisions = {}
    clear_decisions = {}

    for imi in interesting_miro_ids:
        if len(acc_miro_ids[imi]) > 1:
            ambig_decisions[imi] = acc_miro_ids[imi]
        else:
            clear_decisions[imi] = acc_miro_ids[imi]

    print(f"found clear_decisions: {len(clear_decisions)}")

    for k,v in clear_decisions.items():
        item = v[0]
        found_key = item['s3_key']
        expected_key = found_key.replace('miro/Wellcome_Images_Archive', 'data/objects').replace(" ","_")
        print(expected_key, item['miro_id'])
        assert True is False

    print(f"found ambig_decisions: {len(ambig_decisions)}")

    byte_identical = 0
    for k,v in ambig_decisions.items():
        if len(set([poss_img['s3_size'] for poss_img in v])) == 1:
            byte_identical = byte_identical + 1

    print(f"{byte_identical} ambig_decisions are byte identical")

    tif_jp2_pair = 0
    dt_tif_pair = 0
    jp2_pair = 0

    more_than_two = 0

    for k,v in ambig_decisions.items():
        if len(v) == 2:
            extensions = set([img['s3_key'].split('.')[-1].lower() for img in v])
            if extensions == {'tif', 'dt'}:
                dt_tif_pair = dt_tif_pair + 1
            elif extensions == {'jp2'}:
                jp2_pair = jp2_pair + 1
            elif extensions == {'tif', 'jp2'}:
                tif_jp2_pair = tif_jp2_pair + 1
            else:
                raise Exception(f"Found unexpected extensions {extensions}")
        else:
            extensions = set([img['s3_key'].split('.')[-1].lower() for img in v])
            print(extensions)
            more_than_two = more_than_two + 1

    print(f"{more_than_two} ambig_decisions are more_than_two")

    print(f"{dt_tif_pair} ambig_decisions are dt_tif_pairs")
    print(f"{jp2_pair} ambig_decisions are jp2_pairs")
    print(f"{tif_jp2_pair} ambig_decisions are tif_jp2_pair")

    with open('ambig_decisions.json', 'w') as outfile:
        json.dump(ambig_decisions, outfile)

    unique_interesting_ids = interesting_miro_ids - set(ambig_decisions.keys())

    print(f"found unique_interesting_ids: {len(unique_interesting_ids)}")

    with open('unique_interesting_ids.json', 'w') as outfile:
        json.dump(list(unique_interesting_ids), outfile)

if __name__ == "__main__":
    cleared_miro_ids = get_cleared_miro_ids(
        sourcedata_index='sourcedata'
    )
    gather_registrations(
        decisions_index='decisions',
        cleared_miro_ids=cleared_miro_ids
    )