# -*- encoding: utf-8

import datetime as dt
import json

import aws_client
import check_names
from defaults import defaults
import dynamo_status_manager
import helpers
from id_mapper import IDMapper
from iiif_diff import IIIFDiff
from library_iiif import LibraryIIIF
from matcher import Matcher
import preservica
import reporting


def needs_check(row):
    bnumber = row["bnumber"]

    if not reporting.has_succeeded_previously(row, check_names.IIIF_MANIFESTS_CONTENTS):
        print(f"No successful IIIF manifest contents check for {bnumber}")
        return False

    if reporting.has_succeeded_previously(row, check_names.IIIF_MANIFESTS_FILE_SIZES):
        print(f"Already checked IIIF manifest file sizes for {bnumber}")
        return False

    return True


def get_statuses_for_updating(first_bnumber):
    reader = dynamo_status_manager.DynamoStatusReader()

    for row in reader.all(first_bnumber=first_bnumber):
        if needs_check(row):
            yield row


def run_check(status_updater, row):
    bnumber = row["bnumber"]

    s3_client = aws_client.dev_client.s3_client()

    s3_body = s3_client.get_object(
        Bucket="wellcomecollection-storage-infra",
        Key=f"tmp/manifest_diffs/{bnumber}.json",
    )["Body"]

    matcher_result = json.load(s3_body)

    assert not matcher_result["diff"]

    differences = []

    for f in matcher_result["files"]:
        preservica_size = preservica.get_preservica_asset_size(f["preservica_guid"])
        storage_manifest_size = f["storage_manifest_entry"]["size"]

        if preservica_size != storage_manifest_size:
            differences.append(f["preservica_guid"])

    if differences:
        print(f"Not all file sizes match for {bnumber}: {differences}")
        status_updater.update(
            row,
            status_name=check_names.IIIF_MANIFESTS_FILE_SIZES,
            success=False,
            last_modified=dt.datetime.now().isoformat(),
        )
    else:
        print(f"File sizes in IIIF and storage service manifests match for {bnumber}!")
        status_updater.update(
            row,
            status_name=check_names.IIIF_MANIFESTS_FILE_SIZES,
            success=True,
            last_modified=dt.datetime.now().isoformat(),
        )


def run(first_bnumber=None):
    with dynamo_status_manager.DynamoStatusUpdater() as status_updater:
        for row in get_statuses_for_updating(first_bnumber=first_bnumber):
            try:
                run_check(status_updater, row)
            except Exception as err:
                print(err)


def report():
    return reporting.build_report(name=check_names.IIIF_MANIFESTS_CONTENTS)
