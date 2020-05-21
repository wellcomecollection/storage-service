# S3 object tagger

This Lambda receives the event stream from the `wellcomecollection-storage` bucket, and applies tags to some objects.



## Moving infrequently-accessed objects to cold storage tiers

At time of writing, the storage service keeps two replicas of every bag:

-   A warm access replica in `wellcomecollection-storage`
-   A cold replica in `wellcomecollection-storage-replica-ireland`
    This is a backup, which is lifecycled to Glacier Deep Archive.
-   (Soon) A cold replica in Azure Blob Storage.

We need to be able to access most files at zero notice, for example to display content on the website.
These files are kept in the warm replica in the Standard IA storage class.

There are some files we know we don't need immediate access to: for example, the high-resolution video masters from A/V digitisation.
The A/V workflow creates a smaller MP4 file that can be user for the website.
We keep the master file in case we need to re-encode it, but we don't need day-to-day access.

We give these objects a special tag, and the lifecycle rules on the bucket move them into cold storage tiers.
