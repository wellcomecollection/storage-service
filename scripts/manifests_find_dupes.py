#!/usr/bin/env python
# -*- encoding: utf-8

import collections
import json
import sys

from common import get_read_only_client
from dynamodb import cached_scan_iterator


s3 = get_read_only_client("s3")


if __name__ == "__main__":
    try:
        manifests_dir = sys.argv[1]
    except IndexError:
        sys.exit(f"Usage: {__file__} <MANIFESTS_DIR>")

    manifests_by_bnumber = collections.defaultdict(list)

    for item in cached_scan_iterator(manifests_dir):
        manifests_by_bnumber[item["id"]].append(item)

    for bnumber, manifests in manifests_by_bnumber.items():
        if len(manifests) == 1:
            # assert manifests[0]["version"] == 1.0, manifests[0]
            continue

        s3_locations = [m["payload"]["typedStoreId"] for m in manifests]

        s3_json = [
            s3.get_object(Bucket=loc["namespace"], Key=loc["path"])["Body"].read()
            for loc in s3_locations
        ]

        earliest_version = b"v%d" % int(manifests[0]["version"])
        earliest_manifest = json.loads(s3_json[0])

        for json_block, manifest in zip(s3_json[1:], manifests[1:]):
            normalised_json_block = json_block.replace(
                b"v%d/" % int(manifest["version"]), earliest_version + b"/"
            )

            normalised_manifest = json.loads(normalised_json_block)

            assert normalised_manifest.keys() == earliest_manifest.keys()
            for key, value in normalised_manifest.items():
                if key in {"version", "createdDate", "ingestId"}:
                    continue
                assert normalised_manifest[key] == earliest_manifest[key]

            print(
                "%s - %s / %s" % (
                    normalised_manifest["ingestId"],
                    normalised_manifest["info"]["externalIdentifier"],
                    normalised_manifest["version"]
                )
            )

            assert 0

        print(s3_json)


        print(bnumber)
        print(manifests)
        break