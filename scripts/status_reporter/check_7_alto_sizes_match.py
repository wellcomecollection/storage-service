# -*- encoding: utf-8

import functools
import json
import os

import aws_client
import check_names
import dynamo_status_manager
import helpers
import reporting


def needs_check(status_summary):
    return helpers.needs_check(
        status_summary,
        previous_check=check_names.STORAGE_MANIFESTS,
        current_check=check_names.ALTO_SIZES_MATCH,
        step_name="ALTO sizes",
    )


def get_statuses_for_updating(first_bnumber):
    reader = dynamo_status_manager.DynamoStatusReader()

    for status_summary in reader.all(first_bnumber=first_bnumber):
        if needs_check(status_summary):
            yield status_summary


def get_matching_s3_objects(bucket, prefix=""):
    """
    Generate objects in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    s3 = aws_client.read_only_client.s3_client()
    paginator = s3.get_paginator("list_objects_v2")

    kwargs = {"Bucket": bucket, "Prefix": prefix}

    for page in paginator.paginate(**kwargs):
        try:
            yield from page["Contents"]
        except KeyError:
            return


def run_check(status_updater, storage_client, row):
    bnumber = row["bnumber"]

    bag = storage_client.get_bag("digitised", bnumber)

    from pprint import pprint

    alto_files_in_bag = [
        f for f in bag["manifest"]["files"] if f["name"].startswith("data/alto/")
    ]

    pprint([f["name"] for f in bag["manifest"]["files"]])

    shard_path = "/".join(list(bnumber[-4:][::-1]))
    prefix = f"mets/{shard_path}/{bnumber}"

    alto_files_in_s3 = [
        s3_obj
        for s3_obj in get_matching_s3_objects(
            bucket="wellcomecollection-assets-workingstorage", prefix=prefix
        )
        if "alto/" in s3_obj["Key"]
    ]

    has_differences = False

    def _sort(f):
        # ALTO files are numbered b1234_001.xml, b1234_002.xml.
        #
        # We need to sort them correctly even if the prefix is borked, e.g.
        # B1234_005.xml, b1234_001.xml.
        #
        return f.split("_")[-1]

    from pprint import pprint

    # pprint(alto_files_in_bag)
    # pprint(alto_files_in_s3)

    if False:
        # if False or len(alto_files_in_bag) != len(alto_files_in_s3) or False:
        print(
            f"Different counts: bag has {len(alto_files_in_bag)} ALTO files, S3 has {len(alto_files_in_s3)}"
        )
        has_differences = True
    else:
        for bag_alto, s3_alto in zip(
            sorted(alto_files_in_bag, key=lambda f: _sort(f["name"])),
            sorted(alto_files_in_s3, key=lambda f: _sort(f["Key"])),
        ):
            # We need to account for the fact that the ALTO files in S3 might have
            # different capitalisations, e.g.
            #
            #   B1234.xml
            #   b1234X.xml
            #
            # But we don't always want to lowercase!  e.g.
            #
            #   PP_CRI_E_1_16_8_0100.xml
            #
            bag_name = os.path.basename(bag_alto["name"])
            s3_name = os.path.basename(s3_alto["Key"])

            if bag_name != s3_name and bag_name != s3_name.lower():
                print(f"{bnumber}: ALTO filenames don't match! {bag_name} != {s3_name}")
                has_differences = True
                continue

            if bag_alto["size"] != s3_alto["Size"]:
                print(
                    f"{bnumber}: ALTO sizes don't match! {bag_name}: {bag_alto['size']} != {s3_alto['Size']}"
                )
                has_differences = True
                continue

    if has_differences:
        print(f"{bnumber}: differences between ALTO sizes in bag and S3")
        status_updater.update(
            bnumber, status_name=check_names.ALTO_SIZES_MATCH, success=False
        )
    else:
        print(f"{bnumber}: ALTO sizes in bag and S3 match")
        status_updater.update(
            bnumber, status_name=check_names.ALTO_SIZES_MATCH, success=True
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


def report(report=None):
    return reporting.build_report(name=check_names.ALTO_SIZES_MATCH, report=report)
