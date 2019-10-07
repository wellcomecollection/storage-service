#!/usr/bin/env python
# -*- encoding: utf-8

import concurrent.futures
import pathlib
import sys

import tqdm
from wellcome_storage_service import BagNotFound

sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent))

from aws_client import dev_client
import dynamo_status_manager
import helpers


def needs_check(row):
    bnumber = row["bnumber"]

    if row.get("status-storage_manifest_created", {}).get("success"):
        print(f"Already recorded storage manifest for {bnumber}")
        return False

    return True


def run_check(status_updater, storage_client, row):
    bnumber = row["bnumber"]

    try:
        response = storage_client.get_bag("digitised", bnumber)

    except BagNotFound:
        print(f"No bag in storage service for {bnumber}", file=sys.stderr)
        return

    manifest_date = response["createdDate"]

    status_updater.update_status(
        bnumber,
        status_name="storage_manifest_created",
        success=True,
        last_modified=manifest_date,
    )

    # print(f"Recorded storage manifest creation for {bnumber}")


def get_statuses_for_updating():
    for row in dynamo_status_manager.get_all_statuses(first_bnumber="b28476979"):
        if needs_check(row):
            yield row


if __name__ == "__main__":
    storage_client = helpers.create_storage_client()

    futures = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        with dynamo_status_manager.DynamoStatusUpdater() as status_updater:
            for row in get_statuses_for_updating():
                futures.append(
                    executor.submit(run_check, status_updater, storage_client, row)
                )

            for fut in tqdm.tqdm(
                concurrent.futures.as_completed(futures), total=len(futures)
            ):
                fut.result()
