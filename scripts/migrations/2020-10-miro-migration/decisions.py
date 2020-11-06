#!/usr/bin/env python3
"""
Functions related to gathering migration information
about where things were stored after miro decommission
"""

import collections
import functools
import json
import os
import re

import attr
import click
import elasticsearch
import tqdm

from elastic_helpers import (
    get_local_elastic_client,
    get_elastic_client,
    get_document_count,
)
from miro_ids import (
    parse_miro_id,
    NotMiroAssetError,
    IsMiroMoviesError,
    IsCorporatePhotographyError,
)
from file_groups import choose_group_name
from s3 import get_s3_object, list_s3_objects_from

REMOTE_INVENTORY_INDEX = "miro_inventory"
LOCAL_INVENTORY_INDEX = "reporting_miro_inventory"

STORAGE_ROLE_ARN = "arn:aws:iam::975596993436:role/storage-developer"
ELASTIC_SECRET_ID = "miro_storage_migration/credentials"
S3_PREFIX = "miro/Wellcome_Images_Archive"


@attr.s
class Decision:
    s3_key = attr.ib()
    skip = attr.ib()
    miro_id = attr.ib()
    group_name = attr.ib()
    destinations = attr.ib()
    notes = attr.ib()

    @classmethod
    def from_skip(cls, *, s3_key, reason):
        return cls(
            s3_key=s3_key,
            skip=True,
            miro_id=None,
            group_name=None,
            destinations=[],
            notes=[f"Skipped because: {reason}"],
        )


def mirror_miro_inventory_locally():
    """
    Create a local mirror of the miro_inventory index in the reporting cluster.
    """
    local_elastic_client = get_local_elastic_client()
    reporting_elastic_client = get_elastic_client(
        role_arn=STORAGE_ROLE_ARN, elastic_secret_id=ELASTIC_SECRET_ID
    )

    local_count = get_document_count(local_elastic_client, index=LOCAL_INVENTORY_INDEX)

    remote_count = get_document_count(
        reporting_elastic_client, index=REMOTE_INVENTORY_INDEX
    )

    if local_count == remote_count:
        click.echo("miro_inventory index has been mirrored locally, nothing to do")
        return
    else:
        click.echo("miro_inventory index has not been mirrored locally")

    click.echo(
        "Downloading the complete miro_inventory index from the reporting cluster"
    )

    elasticsearch.helpers.reindex(
        client=reporting_elastic_client,
        source_index=REMOTE_INVENTORY_INDEX,
        target_index=LOCAL_INVENTORY_INDEX,
        target_client=local_elastic_client,
    )


def find_inventory_hits_for_query(query_string):
    local_elastic_client = get_local_elastic_client()

    results = local_elastic_client.search(
        body={"query": {"query_string": {"query": f'"{query_string}"'}}},
        index=LOCAL_INVENTORY_INDEX,
    )

    if len(results["hits"]["hits"]) == 1:
        return results["hits"]["hits"][0]["_source"]


def decide_based_on_reporting_inventory(s3_key, miro_id):
    for name in (
        os.path.basename(s3_key).split(".")[0].split("-")[0],
        os.path.basename(s3_key).split(".")[0],
        os.path.basename(s3_key),
        os.path.basename(s3_key).replace("_orig", ""),
    ):
        resp = find_inventory_hits_for_query(name)
        if resp is not None:
            destinations = []
            notes = []

            if resp["catalogue_api_derivative"] or resp["catalogue_api_master"]:
                destinations.append("library")
            if resp["cold_store_master"]:
                destinations.append("cold_store")
            if resp["tandem_vault_master"]:
                destinations.append("tandem_vault")

            notes.append(f"Matched to miro_inventory record ID={resp['id']!r}")
            return Decision(
                s3_key=s3_key,
                miro_id=miro_id,
                group_name=choose_group_name(S3_PREFIX, s3_key),
                skip=False,
                destinations=destinations,
                notes=notes,
            )


@functools.lru_cache()
def get_wellcome_images_by_size():
    wc_images_by_size = collections.defaultdict(lambda: collections.defaultdict(set))

    for s3_obj in list_s3_objects_from(bucket="wellcomecollection-images"):
        wc_images_by_size[s3_obj["Size"]][s3_obj["ETag"]].add(s3_obj["Key"])

    return wc_images_by_size


def decide_based_on_wellcome_images_bucket(s3_obj, miro_id):
    wc_images_by_size = get_wellcome_images_by_size()

    matching_images = wc_images_by_size[s3_obj["Size"]][s3_obj["ETag"]]
    if matching_images:
        destinations = []
        notes = []

        for mi in matching_images:
            if mi.startswith("library/"):
                destinations.append("library")
                notes.append(f"library: matched to s3://wellcomecollection-images/{mi}")
            elif mi.startswith("cold_store/"):
                destinations.append("cold_store")
                notes.append(
                    f"cold_store: matched to s3://wellcomecollection-images/{mi}"
                )
            elif mi.startswith("tandem_vault/"):
                destinations.append("tandem_vault")
                notes.append(
                    f"tandem_vault: matched to s3://wellcomecollection-images/{mi}"
                )
            else:
                assert 0, mi

        return Decision(
            s3_key=s3_obj["Key"],
            miro_id=miro_id,
            group_name=choose_group_name(S3_PREFIX, s3_obj["Key"]),
            skip=False,
            destinations=destinations,
            notes=notes,
        )


@functools.lru_cache()
def get_trimmed_metadata_for_prefix(prefix):
    """
    Get a subset of the Miro metadata that contains all the Miro-like IDs in
    the original metadata.

    This allows us to do 'if miro_id in metadata' but improve the performance.
    """
    images_xml = get_s3_object(
        bucket="wellcomecollection-assets-workingstorage",
        key=f"miro/source_data/images-{prefix}.xml",
    )

    interesting_lines = []

    numeric_bytes = {ord(c) for c in "0123456789"}

    for line in images_xml:
        if prefix.encode("utf8") not in line:
            continue
        if not any(char in numeric_bytes for char in line):
            continue
        interesting_lines.append(line.strip())

    result = b"\n".join(interesting_lines)

    # Throw away a bunch of stuff we know is going to be XML tags, which
    # aren't actually useful for determining if we know about a Miro ID.
    result = re.sub(b"</?[a-z_]+>", b"", result)

    # Miro IDs are only ASCII, so discard non-ASCII data
    return result.decode("ascii", errors="ignore")


def decide_based_on_miro_metadata(s3_key, miro_id):
    if miro_id.startswith(("AS", "FP")):
        prefix = miro_id[:2]
    else:
        prefix = miro_id[0].upper()

    metadata = get_trimmed_metadata_for_prefix(prefix)

    if miro_id not in metadata:
        return Decision(
            s3_key=s3_key,
            miro_id=miro_id,
            group_name=choose_group_name(S3_PREFIX, s3_key),
            skip=False,
            destinations=["none"],
            notes=[
                f"There is no mention of Miro ID {miro_id} in the metadata for prefix {prefix}"
            ],
        )


def make_decision(s3_obj):
    """
    Decide how a Miro asset should be handled.
    """
    try:
        miro_id = parse_miro_id(s3_obj["Key"])
    except NotMiroAssetError:
        return Decision.from_skip(
            s3_key=s3_obj["Key"], reason="This isn't a Miro asset we want to keep"
        )
    except IsMiroMoviesError:
        return Decision(
            s3_key=s3_obj["Key"],
            miro_id=None,
            group_name=choose_group_name(S3_PREFIX, s3_obj["Key"]),
            skip=False,
            destinations=[],
            notes=["This is a movie"],
        )
    except IsCorporatePhotographyError:
        return Decision(
            s3_key=s3_obj["Key"],
            miro_id=None,
            group_name=choose_group_name(S3_PREFIX, s3_obj["Key"]),
            skip=False,
            destinations=[],
            notes=["Corporate photography"],
        )

    # Can we find a matching entry in the miro_reporting inventory?  If so, we
    # use that as the basis for decision making.
    # Note: This assumes the miro_reporting inventory has been mirrored locally.
    decision = decide_based_on_reporting_inventory(
        s3_key=s3_obj["Key"], miro_id=miro_id
    )
    if decision is not None:
        return decision

    # Can we find a matching entry in the wellcomecollection-images bucket?  If so,
    # we can use that as the basis for decision making.
    decision = decide_based_on_wellcome_images_bucket(s3_obj=s3_obj, miro_id=miro_id)
    if decision is not None:
        return decision

    # Is this image completely missing from the Miro metadata?
    decision = decide_based_on_miro_metadata(s3_key=s3_obj["Key"], miro_id=miro_id)
    if decision is not None:
        return decision

    return Decision(
        s3_key=s3_obj["Key"],
        miro_id=miro_id,
        group_name=choose_group_name(S3_PREFIX, s3_obj["Key"]),
        skip=False,
        destinations=[],
        notes=["??? I don't know how to handle this object"],
    )


def count_decisions():
    decision_count = 0
    for _ in list_s3_objects_from(
        bucket="wellcomecollection-assets-workingstorage", prefix=S3_PREFIX
    ):
        decision_count = decision_count + 1

    return decision_count


def get_decisions():
    mirror_miro_inventory_locally()

    for s3_obj in tqdm.tqdm(
        list_s3_objects_from(
            bucket="wellcomecollection-assets-workingstorage", prefix=S3_PREFIX
        ),
        total=368_392,
    ):
        try:
            yield make_decision(s3_obj)
        except Exception:
            print(s3_obj["Key"])
            raise


if __name__ == "__main__":
    for d in get_decisions():
        print(json.dumps(attr.asdict(d)))
