# Repository layout

The code for the storage service isn't contained in a single repo; it's spread across multiple repos.
This document lists the key repositories for the storage service, and how to find the code within them.

-   [wellcomecollection/storage-service](https://github.com/wellcomecollection/storage-service)

    This contains:

    -   Code for our Scala applications.
        For a guide to the projects within the repo, see the [project guide](project-guide.md).
    -   Documentation for the storage service, in [the `docs` directory](https://github.com/wellcomecollection/storage-service/tree/main/docs).
    -   Infrastructure definitions in Terraform, in [the `terraform` directory](https://github.com/wellcomecollection/storage-service/tree/main/terraform).
        This includes both the infrastructure for the Wellcome instance of the storage service and modules that can be used to run other instances of the storage service.

-   [wellcomecollection/scala-libs](https://github.com/wellcomecollection/scala-libs) – some Scala code shared with other Wellcome services.

    This repo has a lot of the code that interacts directly with AWS services (S3, DynamoDB, SQS, etc.), and the `storage-service` uses more abstract traits -- so implementation details of those services don't leak into the applications.

    Any Scala in the `weco` namespace but not in the `weco.storage_service` namespace is defined in scala-libs.

-   [wellcomecollection-terraform-*](https://github.com/search?type=repositories&q=org%3Awellcomecollection%20terraform-%2A) – shared [Terraform modules](https://www.terraform.io/docs/language/modules/index.html).
    These give us a consistent approach to deploying resources across all of our services (e.g. ECS tasks, SNS topics, SQS queues).
