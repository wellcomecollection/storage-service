# monitoring

This directory contains some stuff for monitoring the storage service.

*   `daily_reporter` prints a summary of storage service activity to Slack, and highlights ingests that might need extra investigation.

*   `end_to_end_bag_test` is a Lambda that sends a new bag for ingest into the storage service.
    It can be run on demand as a check that the storage service is behaving correctly.

*   `test_bags` are some tiny bags to use when testing the storage service.
