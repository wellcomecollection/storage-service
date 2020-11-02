import collections
import functools
import os
import re

import attr
import httpx

from miro_ids import (
    parse_miro_id,
    NotMiroAssetError,
    IsMiroMoviesError,
    IsCorporatePhotographyError,
)
from s3 import get_s3_object, list_s3_objects_from


@attr.s
class Decision:
    s3_key = attr.ib()
    skip = attr.ib()
    defer = attr.ib()
    miro_id = attr.ib()
    destinations = attr.ib()
    notes = attr.ib()

    @classmethod
    def from_skip(cls, *, s3_key, reason):
        return cls(
            s3_key=s3_key,
            skip=True,
            defer=False,
            miro_id=None,
            destinations=[],
            notes=[f"Skipped because: {reason}"],
        )

    @classmethod
    def from_defer(cls, *, s3_key, reason):
        return cls(
            s3_key=s3_key,
            skip=False,
            defer=True,
            miro_id=None,
            destinations=[],
            notes=[f"Deferred because: {reason}"],
        )


def find_inventory_hits_for_query(query_string):
    resp = httpx.request(
        "GET",
        "http://localhost:9200/reporting_miro_inventory/_search",
        json={"query": {"query_string": {"query": f'"{query_string}"'}}},
    )

    if len(resp.json()["hits"]["hits"]) == 1:
        return resp.json()["hits"]["hits"][0]["_source"]


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
                skip=False,
                defer=False,
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
            skip=False,
            defer=False,
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

    result = b"\n".join(interesting_lines).decode("utf8")

    # Throw away a bunch of stuff we know is going to be XML tags, which
    # aren't actually useful for determining if we know about a Miro ID.
    result = re.sub(r"</?[a-z_]+>", "", result)

    return result


def decide_based_on_miro_metadata(s3_key, miro_id):
    if miro_id.startswith(("AS", "FP")):
        prefix = miro_id[:2]
    else:
        prefix = miro_id[0]

    metadata = get_trimmed_metadata_for_prefix(prefix)

    if miro_id not in metadata:
        return Decision(
            s3_key=s3_key,
            miro_id=miro_id,
            skip=False,
            defer=False,
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
        return Decision.from_defer(
            s3_key=s3_obj["Key"], reason="This isn't a Miro asset we want to keep"
        )
    except IsMiroMoviesError:
        return Decision.from_defer(
            s3_key=s3_obj["Key"], reason="We're doing Movies later"
        )
    except IsCorporatePhotographyError:
        return Decision.from_defer(
            s3_key=s3_obj["Key"], reason="We're doing Corporate_Photography later"
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
        skip=False,
        defer=True,
        destinations=[],
        notes=["??? I don't know how to handle this object"],
    )


def get_decisions():
    for s3_obj in list_s3_objects_from(
        bucket="wellcomecollection-assets-workingstorage",
        prefix="miro/Wellcome_Images_Archive",
    ):
        yield make_decision(s3_obj)


if __name__ == "__main__":
    for d in get_decisions():
        print(d)
