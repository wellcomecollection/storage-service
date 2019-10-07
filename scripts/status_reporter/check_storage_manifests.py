# -*- encoding: utf-8

import collections
import sys

from wellcome_storage_service import BagNotFound

import dynamo_status_manager
import helpers
import reporting


STATUS_NAME = "storage_manifest_created"


def needs_check(row):
    bnumber = row["bnumber"]

    if reporting.has_succeeded_previously(row, STATUS_NAME):
        print(f"Already recorded storage manifest for {bnumber}")
        return False

    return True


def get_statuses_for_updating(first_bnumber):
    reader = dynamo_status_manager.DynamoStatusReader()

    for row in reader.get_all_statuses(first_bnumber=first_bnumber):
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

    status_updater.update_status(
        bnumber,
        status_name="storage_manifest_created",
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
    return reporting.build_report(name=STATUS_NAME)
