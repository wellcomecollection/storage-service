# terraform

The storage service Terraform is organised as follows:

*   `app_clients` – credentials for different clients of the storage service.

*   `critical_{staging, prod}` – the permanent storage resources for the staging/prod service.
    This includes the S3 buckets and Blob containers that hold all the files.

    **Modifying this configuration can affect our permanent files, so changes in critical_prod should always be applied by two developers working together.**

*   `infra` – some generic infrastructure for the storage service that isn't tied to a particular stack, e.g. ECR repositories and VPC endpoints

*   `monitoring` – tools for monitoring the storage service, both staging and prod

*   `stack_{staging, prod}` – the transient processing services for a particular instance of the storage service, e.g. the ECS tasks and SQS queues
