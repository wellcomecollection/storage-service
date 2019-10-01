import argparse
from multiprocessing.dummy import Pool as ThreadPool
import time

import aws_client
import bnumbers
import dds_client
import dds_call_sync
import status_store
import library_iiif
import iiif_diff
import manifest_sync

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def reset(s3_client, store):
    print("Resetting local data.")

    bib_number_generator = bnumbers.BibNumberGenerator(s3_client)
    numbers = list(bib_number_generator.bnumbers())

    print(f"Found {len(numbers)} bib numbers, storing.")

    store.reset(numbers)
    print("Done.")


def main():
    parser = argparse.ArgumentParser(description="Check status of jobs")

    parser.add_argument(
        "--database_location",
        default="status_reporter.db",
        help="Location of sqllite database",
    )

    parser.add_argument(
        "--library_goobi_url",
        default="https://library-uat.wellcomelibrary.org/goobipdf/{0}",
        help="URL pattern for starting ingests",
    )

    parser.add_argument(
        "--goobi_call_url",
        default="http://wt-havana:88/Dash/GoobiCall/{0}?json",
        help="URL pattern for requesting ingest status",
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
        action="store_false",
        help="Verify ingest requests update status",
    )

    parser.add_argument(
        '--dump_finished',
        action='store_true',
        help='Print all finished ingest bnumbers'
    )

    parser.add_argument(
        '--dds_call_sync',
        action='store_true',
        help='Sync call status with DDS'
    )

    parser.add_argument(
        '--compare_manifests',
        action='store_true',
        help='Compare UAT manifest with production'
    )

    args = parser.parse_args()

    status_store_location = args.database_location
    dds_start_ingest_url = args.library_goobi_url
    dds_item_query_url = args.goobi_call_url

    pool = ThreadPool(20)

    store = status_store.StatusStore(status_store_location)
    client = dds_client.DDSClient(dds_start_ingest_url, dds_item_query_url)
    call_sync = dds_call_sync.DDSCallSync(client, store, pool)
    iiif = library_iiif.LibraryIIIF()
    diff = iiif_diff.IIIFDiff(iiif)
    iiif_sync = manifest_sync.ManifestSync(store, diff)

    aws = aws_client.AwsClient()

    if args.reset:
        s3_client = aws.s3_client()
        s3_client = aws.s3_client()
        reset(s3_client, store)

        return

    if(args.dump_finished):
        finished = store.get_status('finished')

        for finished_bnumbers_batch in finished:
            for bnumber in finished_bnumbers_batch:
                print(bnumber['bnumber'])

        return

    if(args.dds_call_sync):
        should_request_ingests = args.should_request_ingests
        retry_finished = args.retry_finished
        verify_ingests = args.verify_ingests

        call_sync.update_store_from_dds(
            should_request_ingests=should_request_ingests,
            retry_finished=retry_finished,
            verify_ingests=verify_ingests,
        )

        return

    if(args.compare_manifests):
        print("Getting finished DDS ingests.")

        iiif_sync.get_manifests()

        return


if __name__ == "__main__":
    main()
