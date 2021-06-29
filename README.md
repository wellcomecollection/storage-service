# dams-prototype-project

This is an initial attempt to run [our storage service](https://github.com/wellcomecollection/storage-service) in a fresh AWS account, without relying on any of our existing infrastructure (e.g. logging cluster, S3 buckets, VPCs).

The long-term goal is to provide a set of instructions for another institution to be able to run their own instance of the storage service.

## Running the terraform

```console
$ terraform plan -out=terraform.plan -target=module.stack.aws_service_discovery_private_dns_namespace.namespace
$ terraform plan -out=terraform.plan
```