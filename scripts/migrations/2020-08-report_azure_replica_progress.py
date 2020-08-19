#!/usr/bin/env python

import datetime
import json
import os
import sys

import humanize
import termcolor

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from _aws import scan_table  # noqa
from _azure_backfill import get_bags, has_been_replicated_to_azure


if __name__ == "__main__":
    try:
        env = sys.argv[1]
    except IndexError:
        sys.exit(f"Usage: {__file__} (stage|prod)")

    for env in ("stage",):
        if env == "stage":
            vhs_table = "vhs-storage-staging-manifests-2020-07-24"
            backfill_table = "vhs-storage-staging-manifests-2020-08-19"
        else:
            assert False, f"Unsupported environment: {env}"

        result = {"✔": [], "✘": []}

        for space, externalIdentifier, version in get_bags(vhs_table=vhs_table):
            manifest_id = f"{space}/{externalIdentifier}/{version}"

            if has_been_replicated_to_azure(
                backfill_table=backfill_table,
                space=space,
                externalIdentifier=externalIdentifier,
                version=version,
            ):
                result["✔"].append(manifest_id)
            else:
                result["✘"].append(manifest_id)

        json_string = json.dumps(result)
        now = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        with open(f"azure_backfill_result_{env}_{now}.json", "w") as outfile:
            outfile.write(json_string)

        print(f"== {env} ==")
        for label, colour in [("✔", "green"), ("✘", "red")]:
            print(
                termcolor.colored(
                    "%s %s" % (label, humanize.intcomma(len(result[label])).rjust(9)),
                    colour
                )
            )

        print("")
