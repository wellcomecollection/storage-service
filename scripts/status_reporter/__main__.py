# -*- encoding: utf-8

import argparse
import json
import urllib3

import dds_client
import helpers
import iiif_diff
import id_mapper
import library_iiif
import matcher
import dynamo_status_manager

from defaults import defaults

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _print_as_json(obj):
    print(json.dumps(obj, indent=2, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description="Check status of jobs")

    subparsers = parser.add_subparsers(
        dest="subcommand_name",
        help="subcommand help")

    check_storage_manifests = subparsers.add_parser(
        "check_storage_manifests",
        help="Check for a storage manifest for each b number"
    )
    check_storage_manifests.add_argument(
        "--first_bnumber",
        help="Start checking from this b number"
    )

    report_storage_manifests = subparsers.add_parser(
        "report_storage_manifests",
        help="Report how many b numbers have storage manifests"
    )

    check_dds_sync = subparsers.add_parser(
        "check_dds_sync",
        help="Check for a successful DDS sync for each b number"
    )
    check_dds_sync.add_argument(
        "--first_bnumber",
        help="Start checking from this b number"
    )

    report_dds_sync = subparsers.add_parser(
        "report_dds_sync",
        help="Report how many b numbers have a successful DDS sync"
    )

    parser.add_argument("--get_status", default=None, help="Get status from dynamo")

    parser.add_argument(
        "--dds_ingest_bnumber", default=None, help="Call DDS Client Ingest for bnumber"
    )

    parser.add_argument(
        "--dds_job_status",
        default=None,
        help="Inspect status in DDS Client for bnumber",
    )

    parser.add_argument(
        "--compare_manifest", default=None, help="Compare library manifests for bnumber"
    )

    parser.add_argument(
        "--match_files", default=None, help="Compare manifest files for bnumber"
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
        bnumber = args.get_status

        reader = dynamo_status_manager.DynamoStatusReader()
        status = reader.get_status(bnumber=bnumber)

        _print_as_json(status)

    if args.dds_ingest_bnumber:
        bnumber = args.dds_ingest_bnumber

        print(f"Calling DDS Client for ingest of {bnumber}")

        dds_job_status = _dds_client.ingest(bnumber)
        _print_as_json(dds_job_status)

    elif args.dds_job_status:
        bnumber = args.dds_job_status

        print(f"Calling DDS Client for status of {bnumber}")

        dds_job_status = _dds_client.status(bnumber)
        _print_as_json(dds_job_status)

    elif args.compare_manifest:
        bnumber = args.compare_manifest

        print(f"Comparing Production and UAT manifests for {bnumber}")

        diff_summary = _iiif_diff.fetch_and_diff(bnumber)

        _print_as_json(diff_summary)

    elif args.match_files:
        bnumber = args.match_files

        print(f"Comparing files for {bnumber}")

        _match_summary = _matcher.match(bnumber)

        _print_as_json(_match_summary)

    if args.subcommand_name == "check_storage_manifests":
        import check_storage_manifests
        check_storage_manifests.run(first_bnumber=args.first_bnumber)
        return

    if args.subcommand_name == "report_storage_manifests":
        import check_storage_manifests
        check_storage_manifests.report()
        return

    if args.subcommand_name == "check_dds_sync":
        import check_dds_sync
        check_dds_sync.run(first_bnumber=args.first_bnumber)
        return

    if args.subcommand_name == "report_dds_sync":
        import check_dds_sync
        check_dds_sync.report()
        return



    print("Done.")


if __name__ == "__main__":
    main()
