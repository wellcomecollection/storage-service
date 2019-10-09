# -*- encoding: utf-8

import json
import os

import reporting

from wellcome_storage_service import StorageServiceClient


def dds_sync_is_older_than_storage_manifest(dds_sync_last_modified, status_summary):
    storage_manifest_date = status_summary[check_names.STORAGE_MANIFESTS][
        "last_modified"
    ]

    delta = dp.parse(f"{dds_sync_last_modified}Z") - dp.parse(storage_manifest_date)

    return delta.total_seconds() < 60 * 60


def needs_check(status_summary, *, previous_check, current_check, step_name):
    bnumber = status_summary["bnumber"]

    if not reporting.has_succeeded_previously(status_summary, previous_check):
        print(f"{step_name} / {bnumber}: previous step has not succeeded")
        return False

    if reporting.has_succeeded_previously(status_summary, current_check):
        if (
            status_summary[previous_check]["last_modified"] >
            status_summary[current_check]["last_modified"]
        ):
            print(f"{step_name} / {bnumber}: previous step is newer than current step")
            return Trye
        else:
            print(f"{step_name} / {bnumber}: already recorded success")
            return False

    print(f"{step_name} / {bnumber}: no previous result")
    return True



def create_storage_client(api_url="https://api.wellcomecollection.org/storage/v1"):
    creds_path = os.path.join(
        os.environ["HOME"], ".wellcome-storage", "oauth-credentials.json"
    )

    oauth_creds = json.load(open(creds_path))

    return StorageServiceClient(
        api_url=api_url,
        client_id=oauth_creds["client_id"],
        client_secret=oauth_creds["client_secret"],
        token_url=oauth_creds["token_url"],
    )
