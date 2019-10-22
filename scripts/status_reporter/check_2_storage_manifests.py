# -*- encoding: utf-8

import collections
import concurrent.futures
import sys

from wellcome_storage_service import BagNotFound

import check_names
import dynamo_status_manager
import helpers
import reporting


def needs_check(status_summary):
    return helpers.needs_check(
        status_summary,
        previous_check=check_names.METS_EXISTS,
        current_check=check_names.STORAGE_MANIFESTS,
        step_name="Storage manifest",
    )


def get_statuses_for_updating(first_bnumber, db_shard, total_db_shards):
    reader = dynamo_status_manager.DynamoStatusReader()

    for row in reader.all(
        first_bnumber=first_bnumber, db_shard=db_shard, total_db_shards=total_db_shards
    ):
        if needs_check(row):
            yield row


def run_check(status_updater, storage_client, status_summary):
    bnumber = status_summary["bnumber"]

    try:
        response = storage_client.get_bag("digitised", bnumber)

    except BagNotFound:
        print(f"No bag in storage service for {bnumber}", file=sys.stderr)
        return

    manifest_date = response["createdDate"]

    status_updater.update(
        bnumber=bnumber,
        status_name=check_names.STORAGE_MANIFESTS,
        success=True,
        last_modified=manifest_date,
    )

    print(f"Recorded storage manifest creation for {bnumber}")


def run_one(bnumber):
    reader = dynamo_status_manager.DynamoStatusReader()
    status_summary = reader.get_one(bnumber)
    storage_client = helpers.create_storage_client()

    if needs_check(status_summary):
        with dynamo_status_manager.DynamoStatusUpdater() as status_updater:
            run_check(status_updater, storage_client, status_summary)


def run(first_bnumber=None):
    futures = []
    storage_client = helpers.create_storage_client()

    WORKERS = 5

    with dynamo_status_manager.DynamoStatusUpdater() as status_updater:
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
            for i in range(WORKERS):
                for status_summary in get_statuses_for_updating(
                    first_bnumber=first_bnumber, db_shard=i, total_db_shards=WORKERS
                ):
                    future = executor.submit(
                        run_check, status_updater, storage_client, status_summary
                    )

            for fut in concurrent.futures.as_completed(futures):
                fut.result()


def report(report=None):
    return reporting.build_report(name=check_names.STORAGE_MANIFESTS, report=report)
