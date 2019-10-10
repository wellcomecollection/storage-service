# -*- encoding: utf-8

import argparse
import decimal
import getpass
import importlib
import json
import os
import pathlib
import re
import sys

import urllib3

from aws_client import read_only_client
import check_names
from defaults import defaults
import dds_client
import helpers
import iiif_diff
import id_mapper
import library_iiif
import matcher
import dynamo_status_manager

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Register check files
# [check_1_mets, check_2_storage_manifests, ...]

# Get a list of .py files that are checks
SRC_DIR = pathlib.Path(__file__).resolve().parent

def _register_module(f):
    name = os.path.splitext(f)[0]
    importlib.import_module(name)

    return name

CHECK_MODULES = sorted(
    [
        _register_module(f)
        for f in os.listdir(SRC_DIR)
        if re.match(r"^check_\d+_[a-z_]+\.py$", f)
    ]
)


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)


def _print_as_json(obj):
    print(json.dumps(obj, indent=2, sort_keys=True, cls=DecimalEncoder))


def _split_on_comma(string):
    return [i.strip() for i in string.split(",") if i]


def _add_manual_skip(subparsers):
    manually_skip = subparsers.add_parser(
        "manually_skip_bnumber",
        help="Mark a b number as manually skipped for the migration",
    )

    manually_skip.add_argument("--bnumber", help="b number to skip", required=True)
    manually_skip.add_argument(
        "--reason", help="Why is this b number being skipped?", required=True
    )


def _add_report_all(subparsers):
    subparsers.add_parser("report_all", help="Report stats for the entire migration")


def _add_check_commands(subparsers):
    for name in CHECK_MODULES:
        report_name = name.replace("check_", "report_")

        check_parser = subparsers.add_parser(name, help=f"Run {name}")

        check_parser.add_argument(
            "--first_bnumber", help="Start checking from this b number"
        )
        check_parser.add_argument("--check_one", help="Check only this b number")

        subparsers.add_parser(
            report_name, help=f"Report how many b numbers have passed {name}"
        )


def main():
    parser = argparse.ArgumentParser(description="Check status of jobs")
    subparsers = parser.add_subparsers(dest="subcommand_name", help="subcommand help")

    _add_check_commands(subparsers)
    _add_manual_skip(subparsers)
    _add_report_all(subparsers)

    # Not check or reporting

    run_all = subparsers.add_parser("run_all", help="Run all checks for b numbers")
    run_all.add_argument(
     "--first_bnumber", help="Start checking from this b number"
    )
    run_all.add_argument(
     "--skip_checks", default=[], help="Skip these checks"
    )
    run_all.add_argument(
     "--check_one", help="check only this bnumber"
    )

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

    if args.subcommand_name == 'run_all':
        modules = sorted([name for name in CHECK_MODULES])

        checks_to_skip = []
        first_bnumber = None

        if args.skip_checks:
            checks_to_skip = _split_on_comma(args.skip_checks)
            for check_skip_name in checks_to_skip:
                if check_skip_name not in CHECK_MODULES:
                    raise Exception(f"Cannot skip check {check_skip_name}! Must be one of {CHECK_MODULES}.")

        if args.first_bnumber:
            first_bnumber = args.first_bnumber
        elif args.check_one:
            bnumber = args.check_one

            for module_name in modules:
                if module_name not in checks_to_skip:
                    module = sys.modules[module_name]
                    module.run_one(bnumber)
                else:
                    print(f"Skipping check: {module_name}")
        else:
            reader = dynamo_status_manager.DynamoStatusReader()

            for status_summary in reader.all(first_bnumber):
                for module_name in modules:
                    module = sys.modules[module_name]
                    module.run_one(status_summary['bnumber'])

                print('')

        return

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

        results = [_dds_client.ingest(bnum) for bnum in bnumbers]
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

    if args.subcommand_name:
        report_match = re.match(r"^report_(?P<name>\d+_[a-z_]+)$", args.subcommand_name)
        check_match = re.match(r"^check_(?P<name>\d+_[a-z_]+)$", args.subcommand_name)
    else:
        parser.print_help()
        sys.exit(2)

    # check_mets

    if check_match is not None:
        name = check_match.group(0)
        module = sys.modules[name]

        if args.check_one:
            module.run_one(args.check_one)
        elif args.first_bnumber:
            module.run(first_bnumber=args.first_bnumber)
        else:
            module.run()

        sys.exit(0)

    if report_match is not None:
        name = report_match.group("name")
        module_name = f"check_{name}"

        module = sys.modules[module_name]
        module.report()
        sys.exit(0)

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
                user=getpass.getuser(),
            )

        print(f"Marked {bnumber} as manually skipped")

    if args.subcommand_name == "report_all":
        import report_all

        report_all.run()


if __name__ == "__main__":
    main()
