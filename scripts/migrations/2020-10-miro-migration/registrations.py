#!/usr/bin/env python3
"""
Build registrations index
"""

import json

import attr
import elasticsearch
from collections import defaultdict
from elastic_helpers import (
    get_local_elastic_client,
    get_elastic_client
)


FILES_INDEX = "files"


@attr.s
class Registration:
    file_id = attr.ib()
    miro_id = attr.ib()

def stored_files():
    local_elastic_client = get_local_elastic_client()
    match_all_query = {
        "query": {
            "match_all": {}
        }
    }

    return elasticsearch.helpers.scan(
        local_elastic_client, query=match_all_query, index=FILES_INDEX
    )


def _get_cleared_miro_ids(sourcedata_index):
    local_elastic_client = get_local_elastic_client()

    query_body = {"query": {"bool": {"must_not": [{"term": {"cleared": False}}]}}}

    scan = elasticsearch.helpers.scan(
        local_elastic_client, query=query_body, index=sourcedata_index
    )

    ids = []
    for result in scan:
        ids.append(result["_source"]["id"])

    return set(ids)


def _get_miro_ids_from_decisions(decisions_index):
    local_elastic_client = get_local_elastic_client()

    query_body = {
        "query": {
            "bool": {
                "must_not": [{"term": {"skip": True}}],
                "must": [{"exists": {"field": "miro_id"}}],
            }
        }
    }

    filtered_decisions = elasticsearch.helpers.scan(
        local_elastic_client, query=query_body, index=decisions_index
    )

    acc_miro_ids = defaultdict(list)
    miro_ids = []

    for result in filtered_decisions:
        miro_id = result["_source"]["miro_id"]
        acc_miro_ids[miro_id].append(result["_source"])
        miro_ids.append(result["_source"]["miro_id"])

    return set(miro_ids), acc_miro_ids


def _gather_registrations(decisions_index, cleared_miro_ids):
    miro_ids, acc_miro_ids = _get_miro_ids_from_decisions(decisions_index)

    print(f"Unique miro ids from decisions: {len(miro_ids)}")
    print(f"Cleared miro ids from sourcedata: {len(cleared_miro_ids)}")

    interesting_miro_ids = miro_ids.intersection(cleared_miro_ids)

    print(f"Cleared miro ids from decisions: {len(interesting_miro_ids)}")

    # These may only be recoverable from derivatives
    missing_miro_ids = cleared_miro_ids - interesting_miro_ids
    print(f"Missing miro ids not found in decisions: {len(missing_miro_ids)}")

    with open("registration_clearup/missing_miro_ids.json", "w") as outfile:
        json.dump(list(missing_miro_ids), outfile)

    ambig_decisions = {}
    clear_decisions = {}

    for imi in interesting_miro_ids:
        if len(acc_miro_ids[imi]) > 1:
            ambig_decisions[imi] = acc_miro_ids[imi]
        else:
            clear_decisions[imi] = acc_miro_ids[imi]

    print(f"Found clear decisions: {len(clear_decisions)}")
    print(f"Found ambiguous decisions: {len(ambig_decisions)}")

    with open("registration_clearup/ambiguous_decisions.json", "w") as outfile:
        json.dump(ambig_decisions, outfile)

    disambiguated_decisions = _disambiguate_decisions(ambig_decisions)

    print(f"Cleared up decisions: {len(ambig_decisions)}")

    final_decisions = {}
    for miro_id, decisions in clear_decisions.items():
        final_decisions[miro_id] = decisions[0]
    for miro_id, decision in disambiguated_decisions.items():
        final_decisions[miro_id] = decision

    print(f"Final decisions: {len(final_decisions)}")

    return _gather_files_for_registration(final_decisions)


def _gather_files_for_registration(clear_decisions):
    clear_decisions_lookup = {}
    for miro_id, decision in clear_decisions.items():
        found_key = decision["s3_key"]

        expected_key = found_key.replace(
            "miro/Wellcome_Images_Archive", "data/objects"
        ).replace(" ", "_")

        clear_decisions_lookup[expected_key] = decision["miro_id"]

    files_for_registration = {}
    for stored_file_result in stored_files():
        stored_file_name = stored_file_result['_source']['name']
        if stored_file_name in clear_decisions_lookup:
            files_for_registration[clear_decisions_lookup[stored_file_name]] = stored_file_result['_id']

    print(f"Found files for registration: {len(files_for_registration)}")

    with open("registration_clearup/files_for_registration.json", "w") as outfile:
        json.dump(files_for_registration, outfile)

    return files_for_registration


def _pick_largest(decisions):
    largest_decision = None
    largest_size = 0

    for decision in decisions:
        if decision["s3_size"] > largest_size:
            largest_size = decision["s3_size"]
            largest_decision = decision

    return largest_decision


def _disambiguate_decisions(ambig_decisions):
    clear_decisions = {}

    def _get_ext_from_key(decision):
        return decision["s3_key"].split(".")[-1].lower()

    for miro_id, decisions in ambig_decisions.items():
        extensions_decisions = defaultdict(list)
        num_decisions = len(decisions)

        if num_decisions < 2:
            raise Exception(f"Found unexpected number of decisions (<2) for {miro_id}")
        elif num_decisions >= 2 or num_decisions <= 3:
            for decision in decisions:
                extensions_decisions[_get_ext_from_key(decision)].append(decision)

            extensions = set(extensions_decisions.keys())

            if "jp2" in extensions:
                clear_decisions[miro_id] = _pick_largest(extensions_decisions['jp2'])
            elif "tif" in extensions:
                clear_decisions[miro_id] = _pick_largest(extensions_decisions['tif'])
            else:
                raise Exception(f"No usable extensions found for {miro_id}: {extensions}")

        # Check for unhandled
        elif len(decisions) > 3:
            raise Exception(f"Found unexpected number of decisions (>3) for {miro_id}")

    return clear_decisions


def gather_registrations(sourcedata_index, decisions_index):
    cleared_miro_ids = _get_cleared_miro_ids(
        sourcedata_index=sourcedata_index
    )
    return _gather_registrations(
        decisions_index=decisions_index,
        cleared_miro_ids=cleared_miro_ids
    )
