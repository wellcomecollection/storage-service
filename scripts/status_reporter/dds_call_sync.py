import time

class DDSCallSync:
    def __init__(self, dds_client, status_store, thread_pool):
        self.dds_client = dds_client
        self.status_store = status_store
        self.thread_pool = thread_pool

    def _update_from_store(self, seen=0):
        return {
            "unknown": self.status_store.count_status("unknown"),
            "finished": self.status_store.count_status("finished"),
            "waiting": self.status_store.count_status("waiting"),
            "not_found": self.status_store.count_status("not_found"),
            "total": self.status_store.count(),
            "seen": seen,
        }

    def _ingest(self, bnumber, assert_updated=True):
        status = None
        requested = None
        updated_status = None

        status = self.dds_client.status(bnumber)

        if status["status"] is "not_found":
            requested = self.dds_client.ingest(bnumber)

            if requested is "requested":
                updated_status = self.dds_client.status(bnumber)
                if assert_updated:
                    assert updated_status["status"] is "waiting"

                return True
            else:
                return False

        else:
            return False

    def update_store_from_dds(
        self,
        should_request_ingests=False,
        retry_finished=False,
        verify_ingests=True,
    ):
        def _ingest(bnumber):
            return self._ingest(bnumber, verify_ingests)

        total_time = 0
        time_to_run = 0
        average_time = 0
        batches_seen = 0

        total_ingested = 0
        last_batch_not_found = 0
        last_batch_waiting = 0

        seen = 0
        batch_size = 1000

        for next_batch in self.status_store.get_all(batch_size):
            latest = self._update_from_store(seen)

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
            results = self.thread_pool.map(self.dds_client.status, bnumbers)

            not_found_bnumbers = [
                record["bnumber"] for record in results if record["status"] is "not_found"
            ]

            waiting_bnumbers = [
                record["bnumber"] for record in results if record["status"] is "waiting"
            ]

            last_batch_waiting = len(waiting_bnumbers)
            last_batch_not_found = len(not_found_bnumbers)

            if len(not_found_bnumbers) > 0 and should_request_ingests:
                ingest_results = self.thread_pool.map(_ingest, not_found_bnumbers)

                ingested_count = len([result for result in ingest_results if result])

                total_ingested = total_ingested + ingested_count

            # Database updates can be done in batches as parallel access is dangerous
            self.status_store.batch_update(results)

            seen = seen + len(next_batch)
            batches_seen = batches_seen + 1

            time_to_run = time.time() - start_time
            total_time = total_time + time_to_run
            average_time = total_time / batches_seen

        print("Finished.")