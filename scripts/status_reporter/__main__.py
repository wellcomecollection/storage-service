import argparse
from multiprocessing.dummy import Pool as ThreadPool
import time

import aws_client
import bnumbers
import dds_client
import status_store


def update_from_store(store, seen=0):
    return {
        "unknown": store.count_status("unknown"),
        "finished": store.count_status("finished"),
        "waiting": store.count_status("waiting"),
        "not_found": store.count_status("not_found"),
        "total": store.count(),
        "seen": seen,
    }


def ingest(dds, store, bnumber, assert_updated=True):
    status = None
    requested = None
    updated_status = None

    status = dds.status(bnumber)

    if status["status"] is "not_found":
        requested = dds.ingest(bnumber)

        if requested is "requested":
            updated_status = dds.status(bnumber)
            if assert_updated:
                assert updated_status["status"] is "waiting"

            return True
        else:
            return False
    else:
        return False


def update_store_from_dds(
    thread_pool,
    dds_client,
    status_store,
    should_request_ingests=False,
    retry_finished=False,
    verify_ingests=True,
):
    def _ingest(bnumber):
        return ingest(dds, store, bnumber, verify_ingests)

    total_time = 0
    time_to_run = 0
    average_time = 0
    batches_seen = 0

    total_ingested = 0
    last_batch_not_found = 0
    last_batch_waiting = 0

    seen = 0
    batch_size = 1000

    for next_batch in status_store.get_all(batch_size):
        latest = update_from_store(status_store, seen)

        print("---------------------")
        print(f"seen: {seen}")
        print(f"ingested: {total_ingested}")
        print(f"last_batch_not_found: {last_batch_not_found}")
        print(f"last_batch_waiting: {last_batch_waiting}")
        print("")
        print(f"unknown: {latest['unknown']}")
        print(f"not_found: {latest['not_found']}")
        print(f"waiting: {latest['waiting']}")
        print(f"finished: {latest['finished']}")
        print(f"total: {latest['total']}")
        print(f"last: {time_to_run}, total: {total_time}, mean: {average_time}")
        print("")

        start_time = time.time()

        bnumbers = [
            record["bnumber"]
            for record in next_batch
            if (record["status"] != "finished" or retry_finished)
        ]

        # These requests can proceed in parallel
        results = thread_pool.map(dds_client.status, bnumbers)

        not_found_bnumbers = [
            record["bnumber"] for record in results if record["status"] is "not_found"
        ]

        waiting_bnumbers = [
            record["bnumber"] for record in results if record["status"] is "waiting"
        ]

        last_batch_waiting = len(waiting_bnumbers)
        last_batch_not_found = len(not_found_bnumbers)

        if len(not_found_bnumbers) > 0 and should_request_ingests:
            ingest_results = thread_pool.map(_ingest, not_found_bnumbers)

            ingested_count = len([result for result in ingest_results if result])

            total_ingested = total_ingested + ingested_count

        # Database updates can be done in batches as parallel access is dangerous
        status_store.batch_update(results)

        seen = seen + len(next_batch)
        batches_seen = batches_seen + 1

        time_to_run = time.time() - start_time
        total_time = total_time + time_to_run
        average_time = total_time / batches_seen

    print("Finished.")


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

    args = parser.parse_args()

    status_store_location = args.database_location
    dds_start_ingest_url = args.library_goobi_url
    dds_item_query_url = args.goobi_call_url

    pool = ThreadPool(20)

    store = status_store.StatusStore(status_store_location)

    client = dds_client.DDSClient(dds_start_ingest_url, dds_item_query_url)

    aws = aws_client.AwsClient()

    if args.reset:
        s3_client = aws.s3_client()
        reset(s3_client, store)

        return

    if(args.dump_finished):
        finished = store.get_status('finished')

        for finished_bnumbers_batch in finished:
            for bnumber in finished_bnumbers_batch:
                print(bnumber['bnumber'])

        return


    should_request_ingests = args.should_request_ingests
    retry_finished = args.retry_finished
    verify_ingests = args.verify_ingests

    update_store_from_dds(
        thread_pool=pool,
        dds_client=client,
        status_store=store,
        should_request_ingests=should_request_ingests,
        retry_finished=retry_finished,
        verify_ingests=verify_ingests,
    )


if __name__ == "__main__":
    main()
