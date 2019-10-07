# -*- encoding: utf-8

import collections
import datetime as dt
import sys

from wellcome_storage_service import BagNotFound

import dds_client
from defaults import defaults
import dynamo_status_manager
import helpers
import reporting


STATUS_NAME = "dds_sync_complete"


def needs_check(row):
    bnumber = row["bnumber"]

    if not reporting.has_succeeded_previously(row, "storage_manifest_created"):
        print(f"No storage manifest for {bnumber}")
        return False

    if reporting.has_succeeded_previously(row, STATUS_NAME):
        print(f"Already recorded successful DDS sync for {bnumber}")
        return False

    return True


def get_statuses_for_updating(first_bnumber):
    reader = dynamo_status_manager.DynamoStatusReader()

    for row in reader.get_all_statuses(first_bnumber=first_bnumber):
        if needs_check(row):
            yield row


def run_check(status_updater, row):
    bnumber = row["bnumber"]

    dds_start_ingest_url = defaults["libray_goobi_url"]
    dds_item_query_url = defaults["goobi_call_url"]
    storage_api_url = defaults["storage_api_url"]

    client = dds_client.DDSClient(dds_start_ingest_url, dds_item_query_url)

    result = client.status(bnumber)["status"]

    if result == "finished":
        status_updater.update_status(
            bnumber,
            status_name=STATUS_NAME,
            success=True,
            last_modified=dt.datetime.now().isoformat(),
        )

        print(f"Recorded DDS sync complete for {bnumber}")
    else:
        print(f"DDS sync status for {bnumber} is {result}; not finished yet")


def run(first_bnumber=None):
    with dynamo_status_manager.DynamoStatusUpdater() as status_updater:
        for row in get_statuses_for_updating(first_bnumber=first_bnumber):
            try:
                run_check(status_updater, row)
            except Exception as err:
                print(err)


def report():
    return reporting.build_report(name=STATUS_NAME)
