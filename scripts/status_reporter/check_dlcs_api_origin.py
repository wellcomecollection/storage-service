# -*- encoding: utf-8

import datetime as dt
import functools
import json

from aws_client import dev_client
import check_names
from defaults import defaults
import dynamo_status_manager
import helpers
from id_mapper import IDMapper
from iiif_diff import IIIFDiff
from library_iiif import LibraryIIIF
from matcher import Matcher

# import preservica
import reporting


# We're using functools.lru_cache() as a sort of lazy evaluation here.
# We don't want to fetch the key simply by importing this file, but we also
# don't want to fetch it repeatedly.


@functools.lru_cache()
def dlcs_api_key():
    return dev_client.secrets_manager_value("storage/bagger_dlcs_api_key")


@functools.lru_cache()
def dlcs_api_secret():
    return dev_client.secrets_manager_value("storage/bagger_dlcs_api_secret")


def needs_check(status_summary):
    bnumber = status_summary["bnumber"]

    if not reporting.has_succeeded_previously(status_summary, check_names.DDS_SYNC):
        print(f"No successful DDS sync for {bnumber}")
        return False

    if reporting.has_succeeded_previously(status_summary, check_names.DLCS_ORIGIN_MATCH):
        dds_sync_date = status_summary[check_names.DDS_SYNC]["last_modified"]
        dlcs_origin_date = status_summary[check_names.DLCS_ORIGIN_MATCH]["last_modified"]

        delta = dp.parse(dlcs_origin_date) - dp.parse(dds_sync_date)

        if delta.total_seconds() < 60 * 60:
            print(f"There is a newer DDS sync for {bnumber}")
            return True
        else:
            print(f"Already recorded successful DLCS origin match for {bnumber}")
            return False

    return True


def get_statuses_for_updating(first_bnumber):
    reader = dynamo_status_manager.DynamoStatusReader()

    for row in reader.all(first_bnumber=first_bnumber):
        if needs_check(status_summary):
            yield status_summary


def run_check(status_updater, row):
    bnumber = row["bnumber"]

    # Look up https://api.dlcs.io/customers/2/spaces/5/images/b3134902x_0001.jp2

    assert False


def run(first_bnumber=None):
    with dynamo_status_manager.DynamoStatusUpdater() as status_updater:
        for status_summary in get_statuses_for_updating(first_bnumber=first_bnumber):
            run_check(status_updater, status_summary)


def report():
    return reporting.build_report(name=check_names.DLCS_ORIGIN_MATCH)
