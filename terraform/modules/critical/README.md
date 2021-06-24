# critical

The "critical" module defines the permanent data stores for the Wellcome instance of the storage service, in particular:

*   The S3 buckets and Azure containers that hold the preservation files
*   The DynamoDB tables that hold metadata for the storage service (e.g. the list of ingests)
*   An [S3 Inventory](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory.html) that provides regular reports on the objects in our primary S3 bucket

In a live setup, you should manage these resources in a dedicated Terraform configuration -- i.e., not the same state as the applications/services.
This minimises the risk of them being inadvertently deleted or modified by a bad change to an unrelated resource.

## Sub modules

-   `metadata_stores` creates the DynamoDB tables that hold metadata for the storage service.
    This is required for every instance of the storage service.

## Reusing this module

-   Create your own instance of `metadata_stores` -- this is required to run the storage service.

-   Create your own S3 buckets and Azure containers.

    These will vary a lot from setup to setup, e.g. in:

    -   Naming scheme
    -   Geographic region
    -   Storage lifecycle rules
    -   Permissions management

    You might look at our definitions for inspiration, but you shouldn't use them directly.

    Further reading:

    -   [Terraform docs for the `aws_s3_bucket` resource](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket)
    -   [Terraform docs for the `azurerm_storage_container` resource](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/storage_container)
    -   TODO: Link to as-yet unpublished blog post about splitting Terraform configurations
