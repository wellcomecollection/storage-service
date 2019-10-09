# -*- encoding: utf-8

import collections
import datetime as dt
import sys

from wellcome_storage_service import BagNotFound

import check_names
import dateutil.parser as dp
import dds_client
from defaults import defaults
import dynamo_status_manager
import helpers
import reporting

def dds_sync_is_older_than_storage_manifest(dds_sync_last_modified, status_summary):
    storage_manifest_date = status_summary[check_names.STORAGE_MANIFESTS][
        "last_modified"
    ]

    delta = dp.parse(f"{dds_sync_last_modified}Z") - dp.parse(storage_manifest_date)

    return delta.total_seconds() < 60 * 60

def needs_check(status_summary):
    bnumber = status_summary["bnumber"]

    if not reporting.has_succeeded_previously(
        status_summary, check_names.STORAGE_MANIFESTS
    ):
        print(f"No storage manifest for {bnumber}")
        return False

    if reporting.has_succeeded_previously(status_summary, check_names.DDS_SYNC):
        dds_sync_last_modified = status_summary[check_names.DDS_SYNC]["last_modified"]

        if dds_sync_is_older_than_storage_manifest(dds_sync_last_modified, status_summary):
            print(f"There is a newer storage manifest for {bnumber}")
            return True
        else:
            print(f"Already recorded successful DDS sync for {bnumber}")
            return False

    return True


def get_statuses_for_updating(first_bnumber):
    reader = dynamo_status_manager.DynamoStatusReader()

    for status_summary in reader.all(first_bnumber=first_bnumber):
        if needs_check(status_summary):
            yield status_summary


def run_check(status_updater, status_summary):
    bnumber = status_summary["bnumber"]

    dds_start_ingest_url = defaults["libray_goobi_url"]
    dds_item_query_url = defaults["goobi_call_url"]
    storage_api_url = defaults["storage_api_url"]

    _dds_client = dds_client.DDSClient(dds_start_ingest_url, dds_item_query_url)

    result = _dds_client.status(bnumber)

    if not "Finished" in result:
        raise Exception(f"No attribute 'Finished' in {result}")

    if result["Finished"]:
        dds_sync_last_modified = result["Created"]

        # If the dds last_modified time is older than the storage manifest
        # it needs re-ingesting

        success = dds_sync_is_older_than_storage_manifest(
            dds_sync_last_modified,
            status_summary
        )

        status_updater.update(
            bnumber,
            status_name=check_names.DDS_SYNC,
            success=success,
            last_modified=dds_sync_last_modified,
        )

        print(f"Recorded DDS sync success: {success} for {bnumber}")
    else:
        print(f"DDS status for {bnumber} is not finished")

def run_one(bnumber):
    with dynamo_status_manager.DynamoStatusUpdater() as status_updater:
        reader = dynamo_status_manager.DynamoStatusReader()
        status_summary = reader.get_one(bnumber)
        if needs_check(status_summary):
            run_check(status_updater, status_summary)

def run(first_bnumber=None):
    with dynamo_status_manager.DynamoStatusUpdater() as status_updater:
        for status_summary in get_statuses_for_updating(first_bnumber=first_bnumber):
            run_check(status_updater, status_summary)

def report():
    return reporting.build_report(name=check_names.DDS_SYNC)
