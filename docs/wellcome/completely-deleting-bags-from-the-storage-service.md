# Deleting files or bags from the storage service

It's rare, but sometimes we do need to delete all copies of a file and/or entire bag from the storage service.
This page contains instructions for doing so.

Examples:

*   The Collections & Research team have asked us to delete some material
*   A bag was ingested under the wrong identifier; we've reingested it under the correct identifier and now we want to remove the incorrectly-labelled bag

This page contains instructions for deleting an entire bag.

If you want to delete or modify a file in an existing bag, you need to delete the stored bag, then re-ingest a modified version.
In this case, it may be helpful to include a `CHANGELOG.md` file and/or add the bag to the [list of awkward files and bags](https://docs.wellcomecollection.org/storage-service/wellcome-specific-information/awkward-files-and-bags).

## 1. Get contributor access to our Azure replicas

Open a Service Desk ticket asking for contributor access to our Azure replicas with your c_ cloud account.

Our bags are stored across a mixture of S3 and Azure.
Your c_ cloud AWS access should give you access to the S3 buckets, but you shouldn't have write permissions in the Azure replica.
By default, nobody has these permissions -- this is a deliberate choice, to prevent accidental deletions.

Even if you're only deleting bags in prod, ask for access to our staging replica as well -- this will allow you to test the procedure on non-essential material first.

Here's a script for the Service Desk ticket (fill in the details):

<details>
  <blockquote>
  <p>Temporary write access to the wecostorage{prod,stage} Azure storage account</p>

  <p>We keep three copies of every file in Wellcome Collection's digital collections: two copies in Amazon S3, one copy in Azure Blob. The Azure copy lives in the wecostorageprod Azure storage account.</p>

  <p>By default, nobody has write/delete access to all three copies – this is by design, to prevent somebody inadvertently deleting part of the collections. Our storage service has write-only access, so it can store new material, but it can't delete existing material.</p>

  <p>We need to [explanation], and for this I need to be able to delete the copies we keep in Azure.</p>

  <p>Please give my c_ cloud account write access to the wecostorage{prod,stage} Azure storage account, so that I can remove these files. This is usually done by assigning the "Contributor" role to the c_cloud account.<br/> Once this is done, I'll file a second request to downgrade my permissions again.</p>

  <p>If you want approval, contact [name] – she'll confirm that we want to delete all copies of a particular set of images.</p>
</details>

## 2. Install the Azure CLI and log in to your c_ cloud account with `az login`

This checks that everything is working.
You can install the Azure CLI on macOS using Homebrew or pip.

## 3. Identify the bag you want to delete / contains the files you want to delete

This includes identifying:

*   the environment (prod/staging)
*   space
*   external identifier
*   version

(see [identifiers](../explanations/identifiers))

{% hint style="info" %}
You can only delete the latest version of a bag.
If you want to delete every version of a bag, you'll have to delete them one-by-one, working backwards from the latest version.
This is a by-product of the way versioning works.
{% endhint %}

### 3a. How to identify the bag

To confirm you have the right details for the bag, you can either:
 
* Look in s3 and examine the corresponding bag-info.txt to compare the description with the original deletion request.
* Run `ss_get_bag.py` with those details. Compare the value of info.externalDescription with your expectations.

## 4. Run the "delete bag" script

There's a `ss_delete_bag.py` script in the storage service repo.

You pass the details as command-line arguments, for example:

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
  * If you need to recover the bag using this copy, you can create a tgz file from it and [ingest](../howto/ingest-a-bag.md) it.
*   remove the bag from the reporting cluster, all the objects in S3, all the blobs in Azure, and the bags/ingests APIs

{% hint style="warning" %}
Only run one deletion at a time.
This is slower, but is necessary because of the way we handle the legal holds on the Azure replica – running two deletions at once may cause a conflict.
{% endhint %}

{% hint style="info" %}
It can take a long time for the Azure phase of deletion to complete for a large bag.
{% endhint %}

## 5. Ask D&T to downgrade your permissions to the Azure replica

This returns us to the default "safe" state, where there's nobody with write permissions on all three replicas.
