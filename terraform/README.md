# terraform

The storage service Terraform is organised as follows:

*   `app_clients` – credentials for different clients of the storage service.

*   `critical_{staging, prod}` – the permanent storage resources for the staging/prod service.
    This includes the S3 buckets and Blob containers that hold all the files.

    **Modifying this configuration can affect our permanent files, so changes in critical_prod should always be applied by two developers working together.**

*   `infra` – some generic infrastructure for the storage service that isn't tied to a particular stack, e.g. ECR repositories and VPC endpoints

*   `monitoring` – tools for monitoring the storage service, both staging and prod

*   `stack_{staging, prod}` – the transient processing services for a particular instance of the storage service, e.g. the ECS tasks and SQS queues

## Differences from our other Terraform configurations

*   The `critical` stacks require permission to modify our Azure account.

    At time of writing (September 2022), Alex is the only person who has these permissions.

    We should be modifying this configuration extremely sparingly – this is a ["turn two keys"](https://en.wikipedia.org/wiki/Two-man_rule) stack – and it's hard to imagine a case where we'd need to modify something urgently.
    But at some point we should add a second person.

*   The logging/fluentbit container uses a specific tag, set in `stack_staging/main.tf` and `stack_prod/main.tf`.

    All our other applications inherit the default tag from our [terraform-aws-ecs-service module](https://github.com/wellcomecollection/terraform-aws-ecs-service), but we set a specific tag here because of the storage service demo.
    Our services pull the fluentbit image from a private ECR registry, but the demo stack uses an image in ECR Public.
    When I set that up, I specified the registry/image/tag all in one go.
