# Our three replicas: S3, Glacier, and Azure

We have three replicas for the storage service content:

1.  **A warm replica in S3.**
    This is an S3 bucket in the storage AWS account, in Amazon's eu-west-1 (Ireland) region.
    Objects are stored in a mixture of the Standard-IA and Glacier storage classes and versioning is enabled.

    This is the copy intended for day-to-day access.

    Developers get access to these buckets as part of their standard AWS account permissions, but note there are specific IAM exclusions to prevent us from modifying objects in the prod bucket.

    *   Prod: [wellcomecollection-storage](https://console.aws.amazon.com/s3/buckets/wellcomecollection-storage?region=eu-west-1&tab=objects)
    *   Staging: [wellcomecollection-storage-staging](https://console.aws.amazon.com/s3/buckets/wellcomecollection-storage-staging?region=eu-west-1&tab=objects)

2.  **A cold replica in S3.**
    This is an S3 bucket in the storage AWS account, in Amazon's eu-west-1 (Ireland) region.
    Objects are stored in the Glacier Deep Archive storage classes and versioning is enabled.

    This is the copy intended for disaster recovery.

    Developers get access to these buckets as part of their standard AWS account permissions, but note there are specific IAM exclusions to prevent us from modifying objects in the prod bucket.

    *   Prod: [wellcomecollection-storage-replica-ireland](https://console.aws.amazon.com/s3/buckets/wellcomecollection-storage-replica-ireland?tab=objects&region=eu-west-1)
    *   Staging: [wellcomecollection-storage-staging-replica-ireland](https://console.aws.amazon.com/s3/buckets/wellcomecollection-storage-staging-replica-ireland?region=eu-west-1&tab=objects)

3.  **A cold replica in Azure.**
    This is an Azure Blob container in the D&T account, in Azure's West Europe (Netherlands) region, where blobs are stored in the Archive storage tier.
    The containers have a [legal hold][az_legal_hold] policy applied.

    This is the copy intended for worst-case disaster recovery.
    It's stored in a different geographic location and service provider, to minimise the risk of a problem affecting all three copies at once.

    The storage service accesses these containers using a [shared access signature (SAS)][sas].
    These are signed URIs that we keep in Secrets Manager; note that they're tied to the external IP address of the NAT Gateway in the storage account, so you can't use them locally.

    You can only get access to these containers by asking D&T, and we don't grant access to it by default.
    Ideally there should be nobody who has write access to all three replica locations, to reduce the risk of somebody inadvertently deleting all three copies of an object.

    *   Prod: [wellcomecollection-storage-replica-netherlands](https://portal.azure.com/#blade/Microsoft_Azure_Storage/ContainerMenuBlade/overview/storageAccountId/%2Fsubscriptions%2F2d1115fa-60d9-4509-9c46-29c8e1288618%2FresourceGroups%2Frg-wcollarchive-prod%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Fwecostorageprod/path/wellcomecollection-storage-replica-netherlands/etag/%220x8D8E95F86B1DF59%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride//defaultId//publicAccessVal/None), in the wecostorageprod storage account, in the rg-wcollarchive-prod resource group
    *   Staging: [wellcomecollection-storage-replica-netherlands](https://portal.azure.com/#blade/Microsoft_Azure_Storage/ContainerMenuBlade/overview/storageAccountId/%2Fsubscriptions%2F2d1115fa-60d9-4509-9c46-29c8e1288618%2FresourceGroups%2Frg-wcollarchive-stage%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Fwecostoragestage/path/wellcomecollection-storage-staging-replica-netherlands/etag/%220x8D8B0948F244C2D%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride//defaultId//publicAccessVal/None), in the wecostoragestage storage account, in the rg-wcollarchive-stage resource group

[sas]: https://docs.microsoft.com/en-us/azure/storage/common/storage-sas-overview
[az_legal_hold]: https://docs.microsoft.com/en-gb/azure/storage/blobs/immutable-storage-overview
