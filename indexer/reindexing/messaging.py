import concurrent.futures
import getpass
import itertools
import json

from tqdm import tqdm


def publish_notifications(sns_client, *, topic_arn, payloads, dry_run=False):
    # This code parallelises publication, to make bags go faster.
    # https://alexwlchan.net/2019/10/adventures-with-concurrent-futures/
    max_parallel_notifications = 50

    def publish(payload):
        if not dry_run:
            return sns_client.publish(
                TopicArn=topic_arn,
                Subject=f"Sent by reindexer (user {getpass.getuser()})",
                Message=json.dumps(payload),
            )
        else:
            return True

    payloads_length = len(payloads)
    payloads = iter(payloads)

    print(f"\nPublishing {payloads_length} notifications to {topic_arn}")
    with tqdm(total=payloads_length) as progress_bar:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Schedule the first N futures.  We don't want to schedule them all
            # at once, to avoid consuming excessive amounts of memory.
            futures = {
                executor.submit(publish, payload)
                for payload in itertools.islice(payloads, max_parallel_notifications)
            }

            while futures:
                # Wait for the next future to complete.
                done, futures = concurrent.futures.wait(
                    futures, return_when=concurrent.futures.FIRST_COMPLETED
                )

                for fut in done:
                    fut.result()

                progress_bar.update(len(done))

                # Schedule the next set of futures.  We don't want more than N futures
                # in the pool at a time, to keep memory consumption down.
                for payload in itertools.islice(payloads, len(done)):
                    futures.add(executor.submit(publish, payload))
