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


def needs_check(status_summary):
    return helpers.needs_check(
        status_summary,
        previous_check=check_names.IIIF_MANIFESTS_CONTENTS,
        current_check=check_names.IIIF_MANIFESTS_FILE_SIZES,
        step_name="IIIF manifests sizes",
    )


def get_statuses_for_updating(first_bnumber):
    reader = dynamo_status_manager.DynamoStatusReader()

    for status_summary in reader.all(first_bnumber=first_bnumber):
        if needs_check(status_summary):
            yield status_summary


def run_check(status_updater, status_summary):
    bnumber = status_summary["bnumber"]

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
            differences.append(
                {
                    "guid": f["preservica_guid"],
                    "preservica": preservica_size,
                    "storage_service": storage_manifest_size,
                }
            )

    if differences:
        print(f"Not all file sizes match for {bnumber}: {differences}")
        status_updater.update_status(
            bnumber,
            status_name=check_names.IIIF_MANIFESTS_FILE_SIZES,
            success=False,
            last_modified=dt.datetime.now().isoformat(),
        )
    else:
        print(f"File sizes in IIIF and storage service manifests match for {bnumber}!")
        status_updater.update(
            bnumber,
            status_name=check_names.IIIF_MANIFESTS_FILE_SIZES,
            success=True,
            last_modified=dt.datetime.now().isoformat(),
        )


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
    return reporting.build_report(name=check_names.IIIF_MANIFESTS_CONTENTS)
