# monitoring

This directory contains some stuff for monitoring the storage service.

*   `end_to_end_bag_test` is a Lambda that sends a new bag for ingest into the storage service.
    It can be run on demand as a check that the storage service is behaving correctly.

*   `test_bags` are some tiny bags to use when testing the storage service.
