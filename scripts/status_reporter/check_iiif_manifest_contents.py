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
import reporting


def needs_check(status_summary):
    bnumber = status_summary["bnumber"]

    if not reporting.has_succeeded_previously(status_summary, check_names.DDS_SYNC):
        print(f"No successful DDS sync for {bnumber}")
        return False

    if reporting.has_succeeded_previously(
        status_summary, check_names.IIIF_MANIFESTS_CONTENTS
    ):
        print(f"Already checked IIIF manifest contents for {bnumber}")
        return False

    return True


def get_statuses_for_updating(first_bnumber):
    reader = dynamo_status_manager.DynamoStatusReader()

    for status_summary in reader.all(first_bnumber=first_bnumber):
        if needs_check(status_summary):
            yield status_summary


def run_check(status_updater, status_summary):
    bnumber = status_summary["bnumber"]

    storage_api_url = defaults["storage_api_url"]

    id_mapper = IDMapper()

    iiif_diff = IIIFDiff(library_iiif=LibraryIIIF(), id_mapper=id_mapper)

    storage_client = helpers.create_storage_client(storage_api_url)

    iiif_matcher = Matcher(iiif_diff, storage_client)
    match_result = iiif_matcher.match(bnumber)

    s3_client = aws_client.dev_client.s3_client()

    s3_client.put_object(
        Bucket="wellcomecollection-storage-infra",
        Key=f"tmp/manifest_diffs/{bnumber}.json",
        Body=json.dumps(match_result),
    )

    if match_result["diff"] == {}:
        print(f"IIIF manifests match for {bnumber}!")
        status_updater.update(
            bnumber,
            status_name=check_names.IIIF_MANIFESTS_CONTENTS,
            success=True,
            last_modified=dt.datetime.now().isoformat(),
        )
    else:
        print(f"IIIF manifests vary for {bnumber}!")
        from pprint import pprint

        pprint(match_result["diff"])
        status_updater.update(
            status_summary,
            status_name=check_names.IIIF_MANIFESTS_CONTENTS,
            success=False,
            last_modified=dt.datetime.now().isoformat(),
        )


def run(first_bnumber=None):
    with dynamo_status_manager.DynamoStatusUpdater() as status_updater:
        for status_summary in get_statuses_for_updating(first_bnumber=first_bnumber):
            run_check(status_updater, status_summary)


def report():
    return reporting.build_report(name=check_names.IIIF_MANIFESTS_CONTENTS)
