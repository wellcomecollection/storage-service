# -*- encoding: utf-8

import argparse
import decimal
import getpass
import json
import urllib3
import warnings

from aws_client import read_only_client
import check_names
import dds_client
import helpers
import iiif_diff
import id_mapper
import library_iiif
import matcher
import dynamo_status_manager

from defaults import defaults

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)


def _print_as_json(obj):
    print(json.dumps(obj, indent=2, sort_keys=True, cls=DecimalEncoder))


def _split_on_comma(string):
    return [i.strip() for i in string.split(",") if i]


def _add_check_storage_manifests(subparsers):
    check_storage_manifests = subparsers.add_parser(
        "check_storage_manifests", help="Check for a storage manifest for each b number"
    )
    check_storage_manifests.add_argument(
        "--first_bnumber", help="Start checking from this b number"
    )

    report_storage_manifests = subparsers.add_parser(
        "report_storage_manifests",
        help="Report how many b numbers have storage manifests",
    )


def _add_check_dds_sync(subparsers):
    check_dds_sync = subparsers.add_parser(
        "check_dds_sync", help="Check for a successful DDS sync for each b number"
    )
    check_dds_sync.add_argument(
        "--first_bnumber", help="Start checking from this b number"
    )

    report_dds_sync = subparsers.add_parser(
        "report_dds_sync", help="Report how many b numbers have a successful DDS sync"
    )


def _add_check_iiif_manifest_contents(subparsers):
    check_iiif_manifest_contents = subparsers.add_parser(
        "check_iiif_manifest_contents",
        help="Check for matching IIIF manifest contents for each b number",
    )
    check_iiif_manifest_contents.add_argument(
        "--first_bnumber", help="Start checking from this b number"
    )

    report_iiif_manifest_contents = subparsers.add_parser(
        "report_iiif_manifest_contents",
        help="Report how many b numbers have matching IIIF manifest contents",
    )


def _add_check_iiif_manifest_file_sizes(subparsers):
    check_iiif_manifest_file_sizes = subparsers.add_parser(
        "check_iiif_manifest_file_sizes",
        help="Check the size of entries in the IIIF manifests for each b number",
    )
    check_iiif_manifest_file_sizes.add_argument(
        "--first_bnumber", help="Start checking from this b number"
    )

    report_iiif_manifest_file_sizes = subparsers.add_parser(
        "report_iiif_manifest_file_sizes",
        help="Report how many b numbers have correct sizes for IIIF manifest entries",
    )


def _add_manual_skip(subparsers):
    manually_skip = subparsers.add_parser(
        "manually_skip_bnumber",
        help="Mark a b number as manually skipped for the migration"
    )

    manually_skip.add_argument(
        "--bnumber", help="b number to skip", required=True
    )
    manually_skip.add_argument(
        "--reason", help="Why is this b number being skipped?", required=True
    )


def main():
    parser = argparse.ArgumentParser(description="Check status of jobs")
    subparsers = parser.add_subparsers(dest="subcommand_name", help="subcommand help")

    _add_check_storage_manifests(subparsers)
    _add_check_dds_sync(subparsers)
    _add_check_iiif_manifest_contents(subparsers)
    _add_check_iiif_manifest_file_sizes(subparsers)
    _add_manual_skip(subparsers)

    # Not check or reporting

    parser.add_argument("--get_status", default=None, help="Get status for b numbers")
    parser.add_argument(
        "--reset_status", default=None, help="Reset status for b numbers"
    )
    parser.add_argument(
        "--dds_ingest_bnumber", default=None, help="Call DDS ingest for b numbers"
    )
    parser.add_argument(
        "--dds_job_status", default=None, help="Inspect status from DDS for b numbers"
    )
    parser.add_argument(
        "--compare_manifest",
        default=None,
        help="Compare library manifests for b numbers",
    )
    parser.add_argument(
        "--match_files", default=None, help="Compare manifest files for b numbers"
    )

    args = parser.parse_args()

    dds_start_ingest_url = defaults["libray_goobi_url"]
    dds_item_query_url = defaults["goobi_call_url"]
    storage_api_url = defaults["storage_api_url"]

    _dds_client = dds_client.DDSClient(dds_start_ingest_url, dds_item_query_url)
    _library_iiif = library_iiif.LibraryIIIF()
    _id_mapper = id_mapper.IDMapper()
    _iiif_diff = iiif_diff.IIIFDiff(_library_iiif, _id_mapper)
    _storage_client = helpers.create_storage_client(storage_api_url)
    _matcher = matcher.Matcher(_iiif_diff, _storage_client)

    if args.get_status:
        reader = dynamo_status_manager.DynamoStatusReader()

        bnumbers = args.get_status
        bnumbers = _split_on_comma(bnumbers)

        results = list(reader.get(bnumbers))

        _print_as_json(results)

    if args.reset_status:
        bnumbers = args.reset_status

        with dynamo_status_manager.DynamoStatusUpdater() as status_updater:
            bnumbers = _split_on_comma(bnumbers)

            for bnumber in bnumbers:
                status_updater.reset(bnumber=bnumber)
                print(f"Reset all status checks for {bnumber!r}")

    if args.dds_ingest_bnumber:
        bnumbers = args.dds_ingest_bnumber
        bnumbers = _split_on_comma(bnumbers)

        print(f"Calling DDS Client for ingest of {bnumber}")

        results = [_dds_client.ingest(bnumber) for bnumber in bnumbers]
        _print_as_json(results)

    elif args.dds_job_status:
        bnumbers = args.dds_job_status
        bnumbers = _split_on_comma(bnumbers)

        print(f"Calling DDS Client for status of {bnumber}")

        results = [_dds_client.status(bnumber) for bnumber in bnumbers]
        _print_as_json(results)

    elif args.compare_manifest:
        bnumbers = args.compare_manifest
        bnumbers = _split_on_comma(bnumbers)

        print(f"Comparing Production and UAT manifests for {bnumber}")

        results = [_iiif_diff.fetch_and_diff(bnumber) for bnumber in bnumbers]
        _print_as_json(results)

    elif args.match_files:
        bnumbers = args.match_files
        bnumbers = _split_on_comma(bnumbers)

        print(f"Comparing files for {bnumber}")

        results = [_matcher.match(bnumber) for bnumber in bnumbers]
        _print_as_json(results)

    # Subcommands for check/reporting start here

    # storage_manifests

    if args.subcommand_name == "check_storage_manifests":
        import check_storage_manifests

        check_storage_manifests.run(first_bnumber=args.first_bnumber)
        return

    if args.subcommand_name == "report_storage_manifests":
        import check_storage_manifests

        check_storage_manifests.report()
        return

    # dds_sync

    if args.subcommand_name == "check_dds_sync":
        import check_dds_sync

        check_dds_sync.run(first_bnumber=args.first_bnumber)
        return

    if args.subcommand_name == "report_dds_sync":
        import check_dds_sync

        check_dds_sync.report()
        return

    # iiif_manifest_contents

    if args.subcommand_name == "check_iiif_manifest_contents":
        import check_iiif_manifest_contents

        check_iiif_manifest_contents.run(first_bnumber=args.first_bnumber)
        return

    if args.subcommand_name == "report_iiif_manifest_contents":
        import check_iiif_manifest_contents

        check_iiif_manifest_contents.report()
        return

    # iiif_manifest_file_sizes

    if args.subcommand_name == "check_iiif_manifest_file_sizes":
        import check_iiif_manifest_file_sizes

        check_iiif_manifest_file_sizes.run(first_bnumber=args.first_bnumber)
        return

    if args.subcommand_name == "report_iiif_manifest_file_sizes":
        import check_iiif_manifest_file_sizes

        check_iiif_manifest_file_sizes.report()
        return

    if args.subcommand_name == "manually_skip_bnumber":
        dynamo_table = read_only_client.dynamo_table("storage-migration-status")

        bnumber = args.bnumber
        reason = args.reason

        resp = dynamo_table.get_item(Key={"bnumber": bnumber})

        # TODO: What if this item doesn't exist yet?
        item = resp["Item"]


        with dynamo_status_manager.DynamoStatusUpdater() as status_updater:
            status_updater.update(
                item,
                status_name=check_names.MANUAL_SKIP,
                success=True,
                reason=reason,
                user=getpass.getuser()
            )

        print(f"Marked {bnumber} as manually skipped")


if __name__ == "__main__":
    main()
