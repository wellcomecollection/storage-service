# indexer

We want to use the storage service to do bulk analysis of our files.
For example, we might ask questions like:

*   How many images did we digitise last month?
*   What file formats are we holding?
*   How big are the files we're storing?

The indexer apps record manifests, files and ingests in Elasticsearch.

## Chemist & Druggist

Chemist & Druggist is ~200MB of JSON.
Trying to retrieve or index it will cause our services to run out of memory.

To index it manually:

1.  Raise the CPU/memory on the following services:

    *   bags_api
    *   bag_indexer
    *   file_finder

    I use 2 vCPUs and 16GB of memory each.
    We wouldn't want that much normally, but for a one-off operation it's fine.

2.  Run the following commands:

    To reindex the bag:

    ```
    aws sns publish --topic-arn arn:aws:sns:eu-west-1:975596993436:storage_prod_bag_reindexer_output --message '{"space": "digitised", "externalIdentifier": "b19974760", "version": "v1", "type": "RegisteredBagNotification"}'
    ```

    To reindex the files:

    ```shell
    python3 reindex_files_chemist_and_druggist.py
    ```
