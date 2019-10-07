import argparse
from multiprocessing.dummy import Pool as ThreadPool
from pprint import pprint
import time
import urllib3

import aws_client
import bnumbers
import dds_client
import helpers
import iiif_diff
import id_mapper
import library_iiif
import matcher
import dynamo_status_manager

from defaults import *

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def main():
    parser = argparse.ArgumentParser(description="Check status of jobs")

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
    role_arn = defaults["role_arn"]

    _thread_pool = ThreadPool(20)
    _aws_client = aws_client.AwsClient(role_arn)
    _s3_client = _aws_client.s3_client()
    _dds_client = dds_client.DDSClient(dds_start_ingest_url, dds_item_query_url)
    _library_iiif = library_iiif.LibraryIIIF()
    _id_mapper = id_mapper.IDMapper()
    _iiif_diff = iiif_diff.IIIFDiff(_library_iiif, _id_mapper)
    _storage_client = helpers.create_storage_client(storage_api_url)
    _matcher = matcher.Matcher(_iiif_diff, _storage_client)

    _dynamo_status_manager = dynamo_status_manager.DynamoStatusManager(
        aws_client.dev_client
    )

    if args.get_status:
        bnumber = args.get_status

        o = _dynamo_status_manager.get_row_status(bnumber)
        pprint(o)

    if args.dds_ingest_bnumber:
        bnumber = args.dds_ingest_bnumber

        print(f"Calling DDS Client for ingest of {bnumber}")

        dds_job_status = _dds_client.ingest(bnumber)
        pprint(dds_job_status)

    elif args.dds_job_status:
        bnumber = args.dds_job_status

        print(f"Calling DDS Client for status of {bnumber}")

        dds_job_status = _dds_client.status(bnumber)
        pprint(dds_job_status)

    elif args.compare_manifest:
        bnumber = args.compare_manifest

        print(f"Comparing Production and UAT manifests for {bnumber}")

        diff_summary = _iiif_diff.fetch_and_diff(bnumber)

        pprint(diff_summary)

    elif args.match_files:
        bnumber = args.match_files

        print(f"Comparing files for {bnumber}")

        _match_summary = _matcher.match(bnumber)

        pprint(_match_summary)

    print("Done.")


if __name__ == "__main__":
    main()
