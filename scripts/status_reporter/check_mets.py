# -*- encoding: utf-8

import collections
import datetime as dt
import sys

import aws_client
import check_names
import dynamo_status_manager
import bnumbers
import reporting


def needs_check(row):
    bnumber = row["bnumber"]

    if reporting.has_succeeded_previously(row, check_names.METS_EXISTS):
        print(f"Already recorded METS exists for {bnumber}")
        return False

    return True


def run_check(status_updater, bnumber_generator, row):
    bnumber = row["bnumber"]

    mets_record = bnumber_generator.get(bnumber)

    status_updater.update(
        row,
        status_name=check_names.METS_EXISTS,
        success=True,
        last_modified=mets_record['last_modified'],
    )

    print(f"Recorded storage manifest creation for {bnumber}")


def run(first_bnumber=None):
    s3_client = aws_client.read_only_client.s3_client()
    bnumber_generator = bnumbers.BibNumberGenerator(s3_client)

    reader = dynamo_status_manager.DynamoStatusReader()

    with dynamo_status_manager.DynamoStatusUpdater() as status_updater:
        for bnumber in bnumber_generator.bnumbers():
            rows = list(reader.get(bnumber))
            row = rows[0] if rows else None

            if row is not None:
                if True: #needs_check(row):
                    run_check(
                        status_updater,
                        bnumber_generator,
                        row
                    )
            else:
                print(f"{bnumber} not found in database, adding.")
                status_updater.insert(bnumber, [check_names.METS_EXISTS])

def report():
    return reporting.build_report(name=check_names.METS_EXISTS)
