# Completely deleting bags from the storage service

It's rare, but sometimes we do need to delete all copies of a bag from the storage service.

Examples:

*   The Collections & Research team have asked us to delete some material
*   A bag was ingested under the wrong identifier; we've reingested it under the correct identifier and now we want to remove the incorrectly-labelled bag

## How to delete bags from the storage service

1.  Open a Service Desk ticket asking for contributor access to our Azure replicas with your c_ cloud account.

    By default, nobody has write permission to our Azure replica -- this is a deliberate choice, to prevent accidental deletions.

    Even if you're only deleting bags in prod, ask for access to our staging replica as well -- this will allow you to test the procedure on non-essential material first.

2.  Install the Azure CLI and log in to your c_ cloud account with `az login`.

3.  Identify the environment (prod/staging), space, external identifier, and version of the bag you want to delete.

4.  Run the `ss_delete_bag.py` script, for example:

    ```shell
    python3 scripts/ss_delete_bag.py \
      --environment staging \
      --space testing \
      --external-identifier archivematica-dev/TEST/1 \
      --version v30
    ```

    This script will:

    *   prompt you to confirm you do want to delete this bag
    *   ask for a reason, which is recorded in DynamoDB
    *   create a temporary copy of the bag in the `wellcomecollection-storage-infra` bucket (kept for 30 days)
    *   remove the bag from the reporting cluster, all the objects in S3, all the blobs in Azure, and the bags/ingests APIs

5.  Ask D&T to downgrade your permissions for the Azure replica back to read-only.

{% hint style="warning" %}
Only run one deletion at a time.
This is slower, but is necessary because of the way we handle the legal holds on the Azure replica – running two deletions at once may cause a conflict.
{% endhint %}