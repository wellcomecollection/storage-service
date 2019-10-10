# -*- encoding: utf-8

import functools
import json
import os

import hyperlink
import requests

import aws_client
import check_names
import dynamo_status_manager
import helpers
import reporting


# We're using functools.lru_cache() as a sort of lazy evaluation here.
# We don't want to fetch the key simply by importing this file, but we also
# don't want to fetch it repeatedly.


@functools.lru_cache()
def dlcs_api_key():
    return aws_client.dev_client.secrets_manager_value("storage/bagger_dlcs_api_key")


@functools.lru_cache()
def dlcs_api_secret():
    return aws_client.dev_client.secrets_manager_value("storage/bagger_dlcs_api_secret")


def needs_check(status_summary):
    return helpers.needs_check(
        status_summary,
        previous_check=check_names.IIIF_MANIFESTS_FILE_SIZES,
        current_check=check_names.DLCS_ORIGIN_MATCH,
        step_name="DLCS origins match",
    )


def get_statuses_for_updating(first_bnumber):
    reader = dynamo_status_manager.DynamoStatusReader()

    for status_summary in reader.all(first_bnumber=first_bnumber):
        if needs_check(status_summary):
            yield status_summary


def run_check(status_updater, storage_client, row):
    bnumber = row["bnumber"]

    # Fetch the cached list of files associated with this b number from the
    # IIIF manifest contents cache
    s3_client = aws_client.read_only_client.s3_client()

    s3_body = s3_client.get_object(
        Bucket="wellcomecollection-storage-infra",
        Key=f"tmp/manifest_diffs/{bnumber}.json",
    )["Body"]

    matcher_result = json.load(s3_body)

    # Find a JP2 that's in this manifest
    try:
        first_file = next(
            f
            for f in matcher_result["files"]
            if f["storage_manifest_entry"]["name"].endswith((".jp2", ".JP2"))
        )
    except StopIteration:
        print("Not sure what to do: no JP2s? %r" % matcher_result["files"])
        return

    preservica_guid = first_file["preservica_guid"]
    storage_service_filename = os.path.basename(
        first_file["storage_manifest_entry"]["name"]
    )

    # Now look up the DLCS API entry for this file
    storage_service_resp = requests.get(
        f"https://api.dlcs.io/customers/2/spaces/5/images/{storage_service_filename}",
        auth=(dlcs_api_key(), dlcs_api_secret()),
    )

    preservica_resp = requests.get(
        f"https://api.dlcs.io/customers/2/spaces/1/images/{preservica_guid}",
        auth=(dlcs_api_key(), dlcs_api_secret()),
    )

    # Now compare the metadata in both images.  If it's wrong, we should record
    # an error and flag it for further processing.

    has_differences = False

    if storage_service_resp.json() == {"success": "false"}:
        print(f"{bnumber}: error response looking up storage service file in DLCS")
        return True

    if preservica_resp.json() == {"success": "false"}:
        print(f"{bnumber}: error response looking up Preservica file in DLCS")
        return True

    for key in (
        "duration",
        "family",
        "height",
        "maxUnauthorised",
        "mediaType",
        "number1",
        "number2",
        "number3",
        "string1",
        "string2",
        "string3",
        "width",
    ):
        try:
            storage_value = storage_service_resp.json()[key]
        except KeyError:
            print(f"{bnumber} [{key}]: no value in storage service response")
            has_differences = True
            continue

        try:
            preservica_value = preservica_resp.json()[key]
        except KeyError:
            print(f"{bnumber} [{key}]: no value in preservica response")
            has_differences = True
            continue

        if storage_value != preservica_value:
            print(f"{bnumber} [{key}]: {storage_value!r} != {preservica_value!r}")
            has_differences = True

    if not has_differences:
        print(f"{bnumber}: metadata matches")

    # Now check the origin for the storage service image is correct
    storage_service_origin = storage_service_resp.json()["origin"]
    url = hyperlink.URL.from_text(storage_service_origin)

    bucket, space, dlcs_bnumber, version, *_ = url.path

    bag = storage_client.get_bag("digitised", bnumber)

    if bag["location"]["bucket"] != bucket:
        print(f"{bnumber} [bucket]: {bag['location']['bucket']!r} != {bucket!r}")
        has_differences = True
    if space != "digitised":
        print(f"{bnumber} [space]: {space!r} != 'digitised'")
        has_differences = True
    if dlcs_bnumber != bnumber:
        print(f"{bnumber} [bnumber]: {dlcs_bnumber!r} != {bnumber!r}")
        has_differences = True
    if bag["version"] != version:
        print(f"{bnumber} [version]: {bag['version']!r} != {version!r}")
        has_differences = True

    if has_differences:
        print(f"{bnumber}: differences between DLCS API origin and storage manifest")
        status_updater.update(
            bnumber, status_name=check_names.DLCS_ORIGIN_MATCH, success=False
        )
    else:
        print(f"{bnumber}: DLCS API origin is correct")
        status_updater.update(
            bnumber, status_name=check_names.DLCS_ORIGIN_MATCH, success=True
        )


def run_one(bnumber):
    storage_client = helpers.create_storage_client()
    with dynamo_status_manager.DynamoStatusUpdater() as status_updater:
        reader = dynamo_status_manager.DynamoStatusReader()
        status_summary = reader.get_one(bnumber)

        if needs_check(status_summary):
            run_check(status_updater, storage_client, status_summary)


def run(first_bnumber=None):
    storage_client = helpers.create_storage_client()

    with dynamo_status_manager.DynamoStatusUpdater() as status_updater:
        for status_summary in get_statuses_for_updating(first_bnumber=first_bnumber):
            run_check(status_updater, storage_client, status_summary)


def report():
    return reporting.build_report(name=check_names.DLCS_ORIGIN_MATCH)
