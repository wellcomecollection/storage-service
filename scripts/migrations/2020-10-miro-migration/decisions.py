import os

import attr
import httpx

from miro_ids import parse_miro_id, NotMiroAssetError, IsMiroMoviesError, IsCorporatePhotographyError


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
        'http://localhost:9200/reporting_miro_inventory/_search',
        json={'query': {'query_string': {'query': f'"{query_string}"'}}}
    )

    if len(resp.json()["hits"]["hits"]) == 1:
        return resp.json()["hits"]["hits"][0]["_source"]


def decide_based_on_reporting_inventory(s3_key, miro_id):
    for name in (
        os.path.basename(s3_key).split(".")[0].split("-")[0],
        os.path.basename(s3_key).split(".")[0],
        os.path.basename(s3_key),
        os.path.basename(s3_key).replace("_orig", "")
    ):
        resp = find_inventory_hits_for_query(name)
        if resp is not None:
            destinations = []
            notes = []

            if inventory_match['catalogue_api_derivative'] or inventory_match['catalogue_api_master']:
                destinations.append("library")
            if inventory_match["cold_store_master"]:
                destinations.append("cold_store")
            if inventory_match["tandem_vault_master"]:
                destinations.append("tandem_vault")

            notes.append(f"Matched to miro_inventory record ID={inventory_match['id']!r}")
            return Decision(
                s3_key=s3_key,
                miro_id=miro_id,
                skip=False,
                defer=False,
                destinations=destinations,
                notes=notes
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
        return Decision.from_defer(s3_key=s3_obj["Key"], reason="We're doing Movies later")
    except IsCorporatePhotographyError:
        return Decision.from_defer(
            s3_key=s3_obj["Key"], reason="We're doing Corporate_Photography later"
        )

    # Can we find a matching entry in the miro_reporting inventory?  If so, we
    # use that as the basis for decision making.
    # Note: This assumes the miro_reporting inventory has been mirrored locally.
    decision = decide_based_on_reporting_inventory(s3_key=s3_obj["Key"], miro_id=miro_id)
    if decision is not None:
        return decision

    assert 0, s3_obj["Key"]



if __name__ == "__main__":
    from s3 import get_s3_objects_from

    for s3_obj in get_s3_objects_from(
        bucket="wellcomecollection-assets-workingstorage",
        prefix="miro/Wellcome_Images_Archive",
    ):
        print(s3_obj["Key"], make_decision(s3_obj))
