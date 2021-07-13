# Terraform demo

This is a Terraform configuration that spins up an instance of the storage service inside your AWS account.

**It is meant for testing the storage service, not to be a production-ready environment for real data.**
There are [limitations and issues][limitations] that need to be addressed before you can deploy this into a real environment.

**This demo will cost money.**
It spins up real resources in your AWS account and you will be billed for them.
(We'd like to provide a demo that runs locally in Docker Compose, but that isn't available yet.)

**Do not use this demo as the only copy of any of your content.**

[limitations]: readme/limitations.md



## Creating the demo

1.  Install [Terraform](https://www.terraform.io) and set up credentials for the [Terraform AWS provider](https://registry.terraform.io/providers/hashicorp/aws/latest/docs).

1.  Create a file `main.tf` with the following contents:

    ```hcl
    module "demo_stack" {
      source = "github.com/wellcomecollection/storage-service.git//demo/terraform/demo_stack?ref=302cb69"

      namespace       = "..."  # e.g. weco-dams-prototype
      short_namespace = "..."  # e.g. weco -- this should be 5 chars or less
    }

    output "elasticsearch_host" { value = module.demo_stack.elasticsearch_host }
    output "kibana_endpoint" { value = module.demo_stack.kibana_endpoint }
    output "token_url" { value = module.demo_stack.token_url }
    output "api_url" { value = module.demo_stack.api_url }
    output "replica_primary_bucket_name" { value = module.demo_stack.replica_primary_bucket_name }
    output "replica_glacier_bucket_name" { value = module.demo_stack.replica_glacier_bucket_name }
    output "uploads_bucket_name" { value = module.demo_stack.uploads_bucket_name }
    output "unpacked_bags_bucket_name" { value = module.demo_stack.unpacked_bags_bucket_name }

    provider "aws" { ... }

    terraform {
      backend "s3" { ... }
    }
    ```

    Fill in a namespace (e.g. `weco-storage-prototype`) and a short namespace (five chars or less).

    You will need to [configure the AWS provider](https://registry.terraform.io/providers/hashicorp/aws/latest/docs) and the [S3 remote backend](https://www.terraform.io/docs/language/settings/backends/s3.html) (or another backend of your choice).

2.  In the same folder as this file, run the following commands:

    ```
    terraform init
    terraform plan -out=terraform.plan
    terraform apply terraform.plan
    ```

    This should take 10–15 minutes to run to completion.
    When it's done, it will print a series of outputs -- save these, they'll be useful later.



## Using the demo

If your `terraform apply` command is successful, Terraform will print a series of values to the console:

```
Outputs:

api_url = "https://abcdef1234.execute-api.eu-west-1.amazonaws.com/v1"
elasticsearch_host = "search-weco-storage-prototype-abcdefghijklmnopqrstuvwxyz.eu-west-1.es.amazonaws.com"
kibana_endpoint = "search-weco-storage-prototype-abcdefghijklmnopqrstuvwxyz.eu-west-1.es.amazonaws.com/_plugin/kibana/"
replica_glacier_bucket_name = "weco-storage-prototype-replica-glacier"
replica_primary_bucket_name = "weco-storage-prototype-replica-primary"
token_url = "https://weco-storage-prototype.auth.eu-west-1.amazoncognito.com/token"
unpacked_bags_bucket_name = "weco-storage-prototype-weco-unpacked-bags"
uploads_bucket_name = "weco-storage-prototype-uploads"
```

You can use these values in [the how-to guides](../../docs/README.md#how-to).

To retrieve the client ID and client secret, use the AWS CLI:

```
aws secretsmanager get-secret-value --secret-id client_id
aws secretsmanager get-secret-value --secret-id client_secret
```

-   For testing purposes, there is a known-working bag in the "uploads" bucket.
    This has the external identifier `test_bag` and is stored in the key `example_bag.tar.gz`.

-   Application logs are written to an Amazon Elasticsearch cluster.
    To see these logs, see the [logging instructions](readme/demo_logging.md).



## Tearing down the demo

1.  Empty all the S3 buckets that were created.

    **This will delete everything you have uploaded.**

    ```
    for bucket in replica_primary_bucket_name replica_glacier_bucket_name unpacked_bags_bucket_name uploads_bucket_name
    do
      aws s3 rm s3://$(terraform output -raw "$bucket")
    done
    ```

2.  Destroy all the resources created by Terraform:

    ```
    terraform destroy
    ```
