# Table of contents

* [Introduction](README.md)

## How-to: basic operations

-   [Ingest a bag into the storage service](howto/ingest-a-bag.md)
-   [Look up an already-stored bag in the storage service](howto/look-up-a-bag.md)
-   [Look up the versions of a bag in the storage service](howto/look-up-versions-of-a-bag.md)

## How to: advanced usage

-   [Getting notifications of newly stored bags](howto/get-notifications-of-stored-bags.md)

## How to: debugging errors

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

-   [Our three replicas: S3, Glacier, and Azure](wellcome/replica-configuration.md)
-   [Awkward files and bags](wellcome/awkward-files-and-bags.md)
