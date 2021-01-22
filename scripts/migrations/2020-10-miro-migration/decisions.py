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

from common import get_aws_client
from elastic_helpers import (
    get_local_elastic_client,
    get_elastic_client,
    get_document_count,
    mirror_index_locally,
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
PLATFORM_ROLE_ARN = "arn:aws:iam::760097843905:role/platform-read_only"
ELASTIC_SECRET_ID = "miro_storage_migration/credentials"


def load_existing_corporate():
    cache_location = "_cache/existing_corporate_ids.json"

    if os.path.isfile(cache_location):
        with open(cache_location) as infile:
            return json.loads(infile.read())

    s3_client = get_aws_client("s3", role_arn=PLATFORM_ROLE_ARN)

    editorial_photography = list_s3_objects_from(
        s3_client=s3_client, bucket="wellcomecollection-editorial-photography"
    )

    existing_corporate_ids = []

    for result in editorial_photography:
        key = result["Key"]
        if key.endswith(".tif"):
            object_id = _get_object_id(key)
            existing_corporate_ids.append(object_id)

    with open(cache_location, "w") as outfile:
        json.dump(existing_corporate_ids, outfile)

    return existing_corporate_ids


EXISTING_CORPORATE_IDS = load_existing_corporate()


@attr.s
class Decision:
    s3_key = attr.ib()
    skip = attr.ib()
    miro_id = attr.ib()
    group_name = attr.ib()
    destinations = attr.ib()
    notes = attr.ib()
    s3_size = attr.ib(default=None)

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

    reporting_elastic_client = get_elastic_client(
        role_arn=STORAGE_ROLE_ARN, elastic_secret_id=ELASTIC_SECRET_ID
    )

    mirror_index_locally(
        remote_client=reporting_elastic_client,
        remote_index_name=REMOTE_INVENTORY_INDEX,
        local_index_name=LOCAL_INVENTORY_INDEX,
        overwrite=False,
    )


def find_inventory_hits_for_query(query_string):
    local_elastic_client = get_local_elastic_client()

    results = local_elastic_client.search(
        body={"query": {"query_string": {"query": f'"{query_string}"'}}},
        index=LOCAL_INVENTORY_INDEX,
    )

    if len(results["hits"]["hits"]) == 1:
        return results["hits"]["hits"][0]["_source"]


def decide_based_on_reporting_inventory(s3_key, s3_size, miro_id, s3_prefix):
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
                s3_size=s3_size,
                miro_id=miro_id,
                group_name=choose_group_name(s3_prefix, s3_key),
                skip=False,
                destinations=destinations,
                notes=notes,
            )


@functools.lru_cache()
def get_wellcome_images_by_size():
    wc_images_by_size = collections.defaultdict(lambda: collections.defaultdict(set))

    s3_client = get_aws_client("s3", role_arn=PLATFORM_ROLE_ARN)

    for s3_obj in list_s3_objects_from(
        s3_client=s3_client, bucket="wellcomecollection-images"
    ):
        wc_images_by_size[s3_obj["Size"]][s3_obj["ETag"]].add(s3_obj["Key"])

    return wc_images_by_size


def decide_based_on_wellcome_images_bucket(s3_obj, miro_id, s3_prefix):
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
            s3_size=s3_obj["Size"],
            miro_id=miro_id,
            group_name=choose_group_name(s3_prefix, s3_obj["Key"]),
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

    s3_client = get_aws_client("s3", role_arn=PLATFORM_ROLE_ARN)

    images_xml = get_s3_object(
        s3_client=s3_client,
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


def decide_based_on_miro_metadata(s3_key, s3_size, miro_id, s3_prefix):
    if miro_id.startswith(("AS", "FP")):
        prefix = miro_id[:2]
    else:
        prefix = miro_id[0].upper()

    metadata = get_trimmed_metadata_for_prefix(prefix)

    if miro_id not in metadata:
        return Decision(
            s3_key=s3_key,
            s3_size=s3_size,
            miro_id=miro_id,
            group_name=choose_group_name(s3_prefix, s3_key),
            skip=False,
            destinations=["none"],
            notes=[
                f"There is no mention of Miro ID {miro_id} in the metadata for prefix {prefix}"
            ],
        )


def make_decision(s3_obj, s3_prefix):
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
            s3_size=s3_obj["Size"],
            miro_id=None,
            group_name=choose_group_name(s3_prefix, s3_obj["Key"]),
            skip=False,
            destinations=[],
            notes=["This is a movie"],
        )
    except IsCorporatePhotographyError:
        object_id = _get_object_id(s3_obj["Key"])

        skip = False
        if object_id in EXISTING_CORPORATE_IDS:
            skip = True

        return Decision(
            s3_key=s3_obj["Key"],
            s3_size=s3_obj["Size"],
            miro_id=None,
            group_name=choose_group_name(s3_prefix, s3_obj["Key"]),
            skip=skip,
            destinations=[],
            notes=["Corporate photography"],
        )

    # Can we find a matching entry in the miro_reporting inventory?  If so, we
    # use that as the basis for decision making.
    # Note: This assumes the miro_reporting inventory has been mirrored locally.
    decision = decide_based_on_reporting_inventory(
        s3_key=s3_obj["Key"],
        s3_size=s3_obj["Size"],
        miro_id=miro_id,
        s3_prefix=s3_prefix
    )
    if decision is not None:
        return decision

    # Can we find a matching entry in the wellcomecollection-images bucket?  If so,
    # we can use that as the basis for decision making.
    decision = decide_based_on_wellcome_images_bucket(
        s3_obj=s3_obj,
        miro_id=miro_id,
        s3_prefix=s3_prefix
    )
    if decision is not None:
        return decision

    # Is this image completely missing from the Miro metadata?
    decision = decide_based_on_miro_metadata(
        s3_key=s3_obj["Key"],
        s3_size=s3_obj["Size"],
        miro_id=miro_id,
        s3_prefix=s3_prefix
    )

    if decision is not None:
        return decision

    return Decision(
        s3_key=s3_obj["Key"],
        s3_size=s3_obj["Size"],
        miro_id=miro_id,
        group_name=choose_group_name(s3_prefix, s3_obj["Key"]),
        skip=False,
        destinations=[],
        notes=["??? I don't know how to handle this object"],
    )


def _get_object_id(s3_key):
    key_parts = s3_key.split(".")
    object_id = None

    if len(key_parts) > 1:
        object_id = s3_key.split(".")[-2]
        object_id = object_id.split("/")[-1]

    return object_id



def count_decisions(s3_prefix):
    decision_count = 0

    s3_client = get_aws_client("s3", role_arn=PLATFORM_ROLE_ARN)

    for _ in list_s3_objects_from(
            s3_client=s3_client,
            bucket="wellcomecollection-assets-workingstorage",
            prefix=s3_prefix,
    ):
        decision_count = decision_count + 1

    return decision_count


def get_decisions(s3_prefix):
    s3_client = get_aws_client("s3", role_arn=PLATFORM_ROLE_ARN)
    mirror_miro_inventory_locally()

    decision_count = count_decisions(s3_prefix)

    for s3_obj in tqdm.tqdm(
        list_s3_objects_from(
            s3_client=s3_client,
            bucket="wellcomecollection-assets-workingstorage",
            prefix=s3_prefix,
        ),
        total=decision_count,
    ):
        try:
            yield make_decision(
                s3_obj=s3_obj,
                s3_prefix=s3_prefix
            )
        except Exception:
            print(s3_obj["Key"])
            raise


# if __name__ == "__main__":
#     for d in get_decisions():
#         print(json.dumps(attr.asdict(d)))
