# Awkward files and bags

This is a list of files and bags that are known to have caused issues, and are recorded here in case they cause problems in future.

## b19974760: Chemist and Druggist

This bag contains the digitised volumes of the journal [*Chemist + Druggist*][c_and_d], which amounts to nearly a million files.
This is substantially larger than any other bag in the storage service.

Because it's so much larger than any other bag, it's quite difficult to store.
When we originally stored this bag, we had to tweak queue timeouts and retries to get it working.
Additionally, there's a hard-coded exception in the bag register for returning the associated storage manifest, because it's such a large JSON object to deserialise.

Now it's stored it should be fine, but the sheer number of files may be a problem the next time it's processes.

[c_and_d]: https://en.wikipedia.org/wiki/Chemist_%2B_Druggist

## SATSY/1821/4: filename that ends with a dot

This bag contains a file whose name ends in a dot (`.`).

The current bag verifier will prevent you storing such a file, because a trailing dot isn't allowed in [Azure blob names][blobs].
When we originally stored this file, we were only storing objects in S3, so we didn't have this restriction.

The object has been replicated to both S3 and Azure, but under slightly different names.
The name in S3 ends with a dot; the name in Azure omits the dot.

Now it's stored it should be fine, but it's notable as the one known discrepancy between the S3 and Azure replicas.

[blobs]: https://docs.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata#blob-names

## b13135934: missed the initial Azure verification

This is a bag which missed its initial Azure verification, and then some of the blobs got cycled to the Archive tier.

It was originally ingested in March 2022, but Azure replication failed for an unknown reason.
At least some of the files were replicated, but the bag didn't store successfully.
(We didn't notice for months, so the application logs had all rotated before we could debug.)

When we attempted to complete the ingest in January 2023, the replication completed, but now Azure verification failed -- anything replicated in March 2022 had been cycled to Azure's archive tier, and so they were inaccessible to the verifier.

Although I was able to rehydrate the blobs, the verifier still got errors from Azure:

```
com.azure.storage.blob.models.BlobStorageException: Status code 403, (empty body)
```

I could download the blobs to an EC2 instance using the Azure CLI, and I verified the bag by hand – so I sent a message "Azure verification complete" to the replica aggregator to complete the ingest and register the bag.

I didn't have time to debug why the verifier can't read blobs which have been rehydrated from the archive tier, but since we can retrieve them through the Azure CLI I don't think it's a major issue.
