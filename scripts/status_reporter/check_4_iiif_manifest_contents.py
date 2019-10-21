# -*- encoding: utf-8

import datetime as dt
import json
import random

import requests

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
    return helpers.needs_check(
        status_summary,
        previous_check=check_names.DDS_SYNC,
        current_check=check_names.IIIF_MANIFESTS_CONTENTS,
        step_name="IIIF manifests contents",
    )


def get_statuses_for_updating(first_bnumber, segment, total_segments):
    reader = dynamo_status_manager.DynamoStatusReader()

    for status_summary in reader.all(first_bnumber, segment, total_segments):
        if needs_check(status_summary):
            yield status_summary


def _is_closed(bnumber):
    for url in [
        f"https://wellcomelibrary.org/iiif/{bnumber}/manifest",
        f"https://library-uat.wellcomelibrary.org/iiif/{bnumber}/manifest",
    ]:
        resp = requests.get(url, params={"cachebust": random.randint(0, 100)})

        if (
            resp.status_code != 500
            or "ERROR: I will not serve a b number that has a closed section"
            not in resp.text
        ):
            return False

    return True


def _is_shared_error(bnumber):
    resps = [
        requests.get(url, params={"cachebust": random.randint(0, 100)})
        for url in [
            f"https://wellcomelibrary.org/iiif/{bnumber}/manifest",
            f"https://library-uat.wellcomelibrary.org/iiif/{bnumber}/manifest",
        ]
    ]

    if not all(r.status_code == 500 for r in resps):
        return False

    for err_text in [
        "ERROR: No width or height data in METS file",
        "ERROR: MODS Section does not contain a DZ License Code",
        "Object reference not set to an instance of an object.",
        "Sequence contains more than one matching element",
    ]:
        if all(err_text in r.text for r in resps):
            print(f"{bnumber}: error is {err_text}")
            return True

    return False


def run_check(status_updater, status_summary):
    bnumber = status_summary["bnumber"]

    storage_api_url = defaults["storage_api_url"]

    known_failure_reason = None

    try:
        id_mapper = IDMapper()

        iiif_diff = IIIFDiff(library_iiif=LibraryIIIF(), id_mapper=id_mapper)

        storage_client = helpers.create_storage_client(storage_api_url)

        iiif_matcher = Matcher(iiif_diff, storage_client)
        match_result = iiif_matcher.match(bnumber)
    except requests.exceptions.HTTPError:

        # Some b numbers have a closed section, which don't return a storage manifest
        # but instead a 500 error.
        # e.g. https://wellcomelibrary.org/iiif/b18876985/manifest
        #
        # In this case, we should check that UAT and live both return a 500 error
        # with the same error.
        if _is_closed(bnumber):
            print(f"Both sites report {bnumber} as having a closed section")
            match_result = {
                "bnumber": bnumber,
                "space": "digitised",
                "files": [],
                "diff": {},
                "is_closed_manifest": True,
            }

            known_failure_reason = (
                "Both UAT and live site have closed section in the METS"
            )

        elif _is_shared_error(bnumber):
            print(f"Both sites report {bnumber} have the same error")
            match_result = {
                "bnumber": bnumber,
                "space": "digitised",
                "files": [],
                "diff": {},
                "is_common_error": True,
            }

            known_failure_reason = "Both UAT and live site have the same 500 errror"
        else:
            raise

    s3_client = aws_client.dev_client.s3_client()

    s3_client.put_object(
        Bucket="wellcomecollection-storage-infra",
        Key=f"tmp/manifest_diffs/{bnumber}.json",
        Body=json.dumps(match_result),
    )

    if match_result["diff"] == {} and known_failure_reason is None:
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

        # There are some cases where labels are mangled, probably by DDS config,
        # in ways that aren't interesting.
        if match_result["diff"].keys() == {"values_changed"} and match_result["diff"][
            "values_changed"
        ].keys() == {"root['label']"}:
            label_diff = match_result["diff"]["values_changed"]["root['label']"]
            old_label = label_diff["old_value"]
            new_label = label_diff["new_value"]

            # Trailing slash:
            #
            #       {
            #           'values_changed': {
            #               "root['label']": {
            #                   'new_value': '[Report 1927] /',
            #                   'old_value': '[Report 1927]'
            #               }
            #           }
            #       }
            #
            if new_label == old_label + " /":
                known_failure_reason = "trailing slash: (new label) = (old label) + /"

            # Square brackets:
            #
            #       "root['label']": {
            #           'new_value': '[Miniyar, Govind Lal]',
            #           'old_value': 'Miniyar, Govind Lal'
            #       }
            #
            #       {
            #           'new_value': '[James, Brennig ]',
            #           'old_value': 'James, Brennig'
            #       }
            #
            #       {
            #           'new_value': '[ \'"Memory" Series\']',
            #           'old_value': '\'"Memory" Series\''
            #       }
            #
            if new_label in {f"[{old_label}]", f"[{old_label} ]", f"[ {old_label}]"}:
                known_failure_reason = "square brackets: (new label) = [ (old label) ]"

            # Truncated labels:
            #
            #       "root['label']": {
            #           'new_value': 'Spec of Henry Beriah Brook :',
            #           'old_value': 'Spec of Henry Beriah Brook : medicinal compound.'
            #       }
            #
            # The length check isn't strictly required, but helps avoid this
            # accidentally catching trivial labels.
            if (
                len(new_label) > 10
                and new_label.endswith(" :")
                and old_label.startswith(new_label)
            ) or (
                len(new_label) > 10
                and not new_label.endswith(" :")
                and old_label.startswith(new_label + " :")
            ):
                known_failure_reason = (
                    "truncated label: (old label) = (new label) : (extra)"
                )

        # Now assemble the line to store in DynamoDB.
        kwargs = {
            "status_name": check_names.IIIF_MANIFESTS_CONTENTS,
            "success": False,
            "last_modified": dt.datetime.now().isoformat(),
        }

        if known_failure_reason is not None:
            print(f"Marking {bnumber} as a known failure: {known_failure_reason}")
            kwargs["known_failure"] = known_failure_reason

        status_updater.update(bnumber, **kwargs)


def run_one(bnumber):
    with dynamo_status_manager.DynamoStatusUpdater() as status_updater:
        reader = dynamo_status_manager.DynamoStatusReader()
        status_summary = reader.get_one(bnumber)
        if needs_check(status_summary):
            run_check(status_updater, status_summary)


def _run_all(first_bnumber, segment, total_segments):
    with dynamo_status_manager.DynamoStatusUpdater() as status_updater:
        for status_summary in get_statuses_for_updating(
            first_bnumber=first_bnumber, segment=segment, total_segments=total_segments
        ):
            try:
                run_check(status_updater, status_summary)
            except Exception as err:
                print(err)


def run(first_bnumber=None):
    import concurrent.futures
    import multiprocessing

    workers = multiprocessing.cpu_count() * 2 + 1
    total_segments = 5

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        for segment in range(total_segments):
            executor.submit(_run_all, first_bnumber, segment, total_segments)


def report(report=None):
    return reporting.build_report(
        name=check_names.IIIF_MANIFESTS_CONTENTS, report=report
    )
