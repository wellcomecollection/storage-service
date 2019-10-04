import argparse
from multiprocessing.dummy import Pool as ThreadPool
from pprint import pprint
import time
import urllib3

import aws_client
import bnumbers
import dds_client
import dds_call_sync
import helpers
import iiif_diff
import id_mapper
import library_iiif
import status_store
import matcher

from defaults import *

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def reset(s3_client, store):
    bib_number_generator = bnumbers.BibNumberGenerator(s3_client)
    numbers = list(bib_number_generator.bnumbers())

    print(f"Found {len(numbers)} bib numbers, storing.")

    store.reset(numbers)


def main():
    parser = argparse.ArgumentParser(description="Check status of jobs")

    parser.add_argument(
        "--database_location",
        default=defaults["database_location"],
        help="Location of sqllite database",
    )

    parser.add_argument(
        "--library_goobi_url",
        default=defaults["libray_goobi_url"],
        help="URL pattern for starting ingests",
    )

    parser.add_argument(
        "--goobi_call_url",
        default=defaults["goobi_call_url"],
        help="URL pattern for requesting ingest status",
    )

    parser.add_argument(
        "--ingest_bnumber", default=None, help="Location of sqllite database"
    )

    parser.add_argument(
        "--compare_manifest", default=None, help="Compare library manifests for bnumber"
    )

    parser.add_argument(
        "--match_manifest_files",
        default=None,
        help="Compare manifest files for bnumber",
    )

    parser.add_argument(
        "--reset",
        action="store_true",
        help="Location of file containing bnumbers to reset status",
    )

    parser.add_argument(
        "--should_request_ingests",
        action="store_true",
        help="When uningested bnumbers are identified, request them",
    )

    parser.add_argument(
        "--retry_finished",
        action="store_true",
        help="Recheck whether ingests marked finished have changed",
    )

    parser.add_argument(
        "--verify_ingests",
        action="store_true",
        help="Verify ingest requests update status",
    )

    parser.add_argument(
        "--dump_finished",
        action="store_true",
        help="Print all finished ingest bnumbers",
    )

    parser.add_argument(
        "--dump_waiting", action="store_true", help="Print all finished ingest bnumbers"
    )

    parser.add_argument(
        "--dds_call_sync", action="store_true", help="Sync call status with DDS"
    )

    args = parser.parse_args()

    status_store_location = args.database_location
    dds_start_ingest_url = args.library_goobi_url
    dds_item_query_url = args.goobi_call_url

    storage_api_url = defaults["storage_api_url"]
    role_arn = defaults["role_arn"]

    _thread_pool = ThreadPool(20)
    _aws_client = aws_client.AwsClient(role_arn)
    _s3_client = _aws_client.s3_client()
    _status_store = status_store.StatusStore(status_store_location)
    _dds_client = dds_client.DDSClient(dds_start_ingest_url, dds_item_query_url)
    _call_sync = dds_call_sync.DDSCallSync(_dds_client, _status_store, _thread_pool)
    _library_iiif = library_iiif.LibraryIIIF()
    _id_mapper = id_mapper.IDMapper()
    _iiif_diff = iiif_diff.IIIFDiff(_library_iiif, _id_mapper)
    _storage_client = helpers.create_storage_client(storage_api_url)
    _matcher = matcher.Matcher(_iiif_diff, _storage_client)

    if args.ingest_bnumber:
        bnumber = args.ingest_bnumber

        print(f"Calling DDS GoobiSync endpoint for {bnumber}")

        ingest_status = _dds_client.ingest(bnumber)
        print(ingest_status)

    elif args.reset:
        print("Resetting local data.")
        reset(_s3_client, _status_store)

    elif args.dump_finished:
        finished = _status_store.get_status("finished")

        for finished_bnumbers_batch in finished:
            for bnumber in finished_bnumbers_batch:
                print(bnumber["bnumber"])

    elif args.dump_waiting:
        finished = _status_store.get_status("waiting")

        for finished_bnumbers_batch in finished:
            for bnumber in finished_bnumbers_batch:
                print(bnumber["bnumber"])

    elif args.dds_call_sync:
        print("Attempting to sync status with DDS.")

        should_request_ingests = args.should_request_ingests
        retry_finished = args.retry_finished
        verify_ingests = args.verify_ingests

        _call_sync.update_store_from_dds(
            should_request_ingests=should_request_ingests,
            retry_finished=retry_finished,
            verify_ingests=verify_ingests,
        )

    elif args.compare_manifest:
        bnumber = args.compare_manifest

        print(f"Comparing Production and UAT manifests for {bnumber}")

        diff_summary = _iiif_diff.fetch_and_diff(bnumber)

        pprint(diff_summary)

    elif args.match_manifest_files:
        bnumber = args.match_manifest_files

        print(f"Comparing manifest files for {bnumber}")

        _match_summary = _matcher.match(bnumber)

        pprint(_match_summary)

    print("Done.")


if __name__ == "__main__":
    main()
