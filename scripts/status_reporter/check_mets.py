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


def run(first_bnumber=None):
    s3_client = aws_client.read_only_client.s3_client()
    bnumber_generator = bnumbers.BibNumberGenerator(s3_client)

    reader = dynamo_status_manager.DynamoStatusReader()

    with dynamo_status_manager.DynamoStatusUpdater() as status_updater:
        for bnumber in bnumber_generator.bnumbers():
            status_summary = list(reader.get(bnumber))

            if status_summary:
                if needs_check(status_summary[0]):
                    mets_record = bnumber_generator.get(bnumber)
                    record_check(
                        status_updater,
                        bnumber_generator,
                        bnumber,
                        mets_record["last_modified"],
                    )
            else:
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


def report():
    return reporting.build_report(name=check_names.METS_EXISTS)
