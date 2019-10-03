import argparse
from multiprocessing.dummy import Pool as ThreadPool
from pprint import pprint
import time
import urllib3

import aws_client
import bnumbers
import dds_client
import dds_call_sync
import status_store
import library_iiif
import iiif_diff
import manifest_sync
import storage_client

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
        "--compare_manifest", default=None, help="Location of sqllite database"
    )

    parser.add_argument(
        "--compare_manifests",
        action="store_true",
        help="Compare all finished manifests",
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
        "--dds_call_sync", action="store_true", help="Sync call status with DDS"
    )

    parser.add_argument(
        "--retry_mismatched_manifests",
        action="store_true",
        help="Reingest mismatched manifests",
    )

    args = parser.parse_args()

    status_store_location = args.database_location
    dds_start_ingest_url = args.library_goobi_url
    dds_item_query_url = args.goobi_call_url

    pool = ThreadPool(20)

    aws = aws_client.AwsClient(defaults["role_arn"])

    store = status_store.StatusStore(status_store_location)
    client = dds_client.DDSClient(dds_start_ingest_url, dds_item_query_url)
    call_sync = dds_call_sync.DDSCallSync(client, store, pool)
    iiif = library_iiif.LibraryIIIF()
    diff = iiif_diff.IIIFDiff(iiif)
    ss_client = storage_client.StorageClient().get()

    iiif_sync = manifest_sync.ManifestSync(
        store, diff, ss_client, client, aws.s3_client(), pool
    )

    if args.ingest_bnumber:
        bnumber = args.ingest_bnumber

        print(f"Calling DDS GoobiSync endpoint for {bnumber}")

        ingest_status = client.ingest(bnumber)
        print(ingest_status)

    elif args.reset:
        print("Resetting local data.")

        s3_client = aws.s3_client()
        s3_client = aws.s3_client()
        reset(s3_client, store)

    elif args.dump_finished:
        finished = store.get_status("finished")

        for finished_bnumbers_batch in finished:
            for bnumber in finished_bnumbers_batch:
                print(bnumber["bnumber"])

    elif args.dds_call_sync:
        print("Attempting to sync status with DDS.")

        should_request_ingests = args.should_request_ingests
        retry_finished = args.retry_finished
        verify_ingests = args.verify_ingests

        call_sync.update_store_from_dds(
            should_request_ingests=should_request_ingests,
            retry_finished=retry_finished,
            verify_ingests=verify_ingests,
        )

    elif args.compare_manifest:
        bnumber = args.compare_manifest

        print(f"Comparing Production and UAT manifests for {bnumber}")

        diff_summary = iiif_sync.diff_summary(bnumber)

        pprint(diff_summary)

    elif args.compare_manifests:
        print(f"Comparing Production and UAT manifests for all finished.")

        iiif_sync.diff_summary_all_finished()

    elif args.retry_mismatched_manifests:
        print("Getting finished DDS ingests.")

        iiif_sync.retry_mismatched_manifests()

    print("Done.")


if __name__ == "__main__":
    main()
