# -*- encoding: utf-8

import collections
import datetime as dt
import sys

import aws_client
import check_names
import dynamo_status_manager
import bnumbers
import reporting


def needs_check(status_summary):
    bnumber = status_summary["bnumber"]

    if reporting.has_succeeded_previously(status_summary, check_names.METS_EXISTS):
        print(f"Already recorded METS exists for {bnumber}")
        return False

    return True


def record_check(status_updater, bnumber_generator, bnumber, last_modified):
    status_updater.update(
        bnumber,
        status_name=check_names.METS_EXISTS,
        success=True,
        last_modified=last_modified,
    )

    print(f"Recorded METS for {bnumber}")


def run_one(bnumber):
    reader = dynamo_status_manager.DynamoStatusReader()
    status_summary = reader.get_one(bnumber)

    s3_client = aws_client.read_only_client.s3_client()
    bnumber_generator = bnumbers.BibNumberGenerator(s3_client)

    with dynamo_status_manager.DynamoStatusUpdater() as status_updater:
        if needs_check(status_summary):
            mets_record = bnumber_generator.get(bnumber)
            record_check(
                status_updater, bnumber_generator, bnumber, mets_record["last_modified"]
            )


def run(first_bnumber):
    s3_client = aws_client.read_only_client.s3_client()
    bnumber_generator = bnumbers.BibNumberGenerator(s3_client)

    reader = dynamo_status_manager.DynamoStatusReader()

    with dynamo_status_manager.DynamoStatusUpdater() as status_updater:
        for bnumber_obj in bnumber_generator.bnumber_objects(first_bnumber):
            bnumber = bnumber_obj["bnumber"]

            try:
                status_summary = reader.get_one(bnumber)
            except dynamo_status_manager.NoSuchRecord:
                print(f"{bnumber} not found in database, adding.")
                mets_record = bnumber_generator.get(bnumber)
                status_updater.insert(
                    bnumber=bnumber,
                    status_summaries={
                        check_names.METS_EXISTS: {
                            "success": True,
                            "last_modified": mets_record["last_modified"],
                        }
                    },
                )
            else:
                if needs_check(status_summary):
                    record_check(
                        status_updater,
                        bnumber_generator,
                        bnumber,
                        last_modified=bnumber_obj["last_modified"],
                    )


def report(report=None):
    return reporting.build_report(name=check_names.METS_EXISTS, report=report)
