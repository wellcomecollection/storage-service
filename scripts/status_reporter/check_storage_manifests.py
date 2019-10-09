# -*- encoding: utf-8

import collections
import sys

from wellcome_storage_service import BagNotFound

import check_names
import dynamo_status_manager
import helpers
import reporting


def needs_check(row):
    bnumber = row["bnumber"]

    if reporting.has_succeeded_previously(row, check_names.STORAGE_MANIFESTS):
        print(f"Already recorded storage manifest for {bnumber}")
        return False

    return True


def get_statuses_for_updating(first_bnumber):
    reader = dynamo_status_manager.DynamoStatusReader()

    for row in reader.all(first_bnumber=first_bnumber):
        if needs_check(row):
            yield row


def run_check(status_updater, storage_client, row):
    bnumber = row["bnumber"]

    try:
        response = storage_client.get_bag("digitised", bnumber)

    except BagNotFound:
        print(f"No bag in storage service for {bnumber}", file=sys.stderr)
        return

    manifest_date = response["createdDate"]

    status_updater.update(
        row,
        status_name=check_names.STORAGE_MANIFESTS,
        success=True,
        last_modified=manifest_date,
    )

    print(f"Recorded storage manifest creation for {bnumber}")


def run(first_bnumber=None):
    storage_client = helpers.create_storage_client()

    with dynamo_status_manager.DynamoStatusUpdater() as status_updater:
        for row in get_statuses_for_updating(first_bnumber=first_bnumber):
            try:
                run_check(status_updater, storage_client, row)
            except Exception as err:
                print(err)


def report():
    return reporting.build_report(name=check_names.STORAGE_MANIFESTS)
