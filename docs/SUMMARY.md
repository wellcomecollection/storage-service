# Table of contents

* [Introduction](README.md)

## How-to: basic operations

-   [Ingest a bag into the storage service](howto/ingest-a-bag.md)
-   [Look up an already-stored bag in the storage service](howto/look-up-a-bag.md)
-   [Look up the versions of a bag in the storage service](howto/look-up-versions-of-a-bag.md)

## How to: advanced usage

-   [Getting notifications of newly stored bags](howto/get-notifications-of-stored-bags.md)

## How to: debugging errors

-   [Where to find application logs](howto/where-to-find-application-logs.md)
-   [Manually marking ingests as failed](howto/manually-marking-ingests-as-failed.md)

## Reference/design decisison

-   [The semantics of bags, ingests and ingest types](explanations/ingest-type.md)
-   [How identifiers work in the storage service](explanations/identifiers.md)
-   [How files are laid out in the underlying storage](explanations/file-layout.md)
-   [Compressed vs uncompressed bags, and the choice of tar.gz](explanations/compression-formats.md)

## Developer information/workflow

-   [An API reference for the user-facing storage service APIs](developers/api-reference.md)
-   [Key technologies](developers/key-technologies.md)
-   [Inter-app messaging with SQS and SNS](developers/inter-app-messaging.md)
-   [How requests are routed from the API to app containers](explanations/how-requests-are-routed.md)
-   [Repository layout](developers/repository-layout.md)
-   [How Docker images are published to ECR](developers/ecr-publishing.md)

## Wellcome-specific information

-   Our storage configuration
    -   [Our three replicas: S3, Glacier, and Azure](wellcome/replica-configuration.md)
    -   [Using multiple storage tiers for cost-efficiency (A/V, TIFFs)](wellcome/using-multiple-storage-tiers-for-cost-efficiency-a-v-tiffs.md)
    -   [Small fluctuations in our storage bill](wellcome/small-fluctuations-in-our-storage-bill.md)
    -   [Delete protection on the production storage service](wellcome/delete-protection-on-the-production-storage-service.md)

-   Wellcome-specific debugging
    -   [Why did my callback to Goobi return a 401 Unauthorized?](wellcome/why-did-my-callback-to-goobi-fail.md)

-   [Recovering files from our Azure replica](wellcome/recovering-files-from-our-azure-replica.md)
-   [Awkward files and bags](wellcome/awkward-files-and-bags.md)
-   [Deleting files or bags bags from the storage service](wellcome/completely-deleting-bags-from-the-storage-service.md)