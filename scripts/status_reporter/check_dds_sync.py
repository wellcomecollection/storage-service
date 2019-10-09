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


def needs_check(status_summary):
    bnumber = status_summary["bnumber"]

    if not reporting.has_succeeded_previously(status_summary, check_names.STORAGE_MANIFESTS):
        print(f"No storage manifest for {bnumber}")
        return False

    if reporting.has_succeeded_previously(status_summary, check_names.DDS_SYNC):
        dds_sync_date = status_summary[check_names.DDS_SYNC]["last_modified"]
        storage_manifest_date = status_summary[check_names.STORAGE_MANIFESTS]["last_modified"]

        delta = dp.parse(f"{dds_sync_date}Z") - dp.parse(storage_manifest_date)

        if delta.total_seconds() < 60 * 60:
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

    if not "finished" in result:
        raise Exception(f"No attribute 'finished' in {result}")

    if result["finished"]:
        last_modified = result["created"]

        status_updater.update(
            bnumber,
            status_name=check_names.DDS_SYNC,
            success=True,
            last_modified=last_modified,
        )

        print(f"Recorded DDS sync complete for {bnumber}")
    else:
        print(f"DDS status for {bnumber} is not finished")


def run(first_bnumber=None):
    with dynamo_status_manager.DynamoStatusUpdater() as status_updater:
        for status_summary in get_statuses_for_updating(first_bnumber=first_bnumber):
            run_check(status_updater, status_summary)


def report():
    return reporting.build_report(name=check_names.DDS_SYNC)
