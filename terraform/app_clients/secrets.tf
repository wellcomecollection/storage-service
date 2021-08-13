locals {
  description = "Storage service credentials; managed in https://github.com/wellcomecollection/storage-service/tree/main/terraform/app_clients"
}

module "catalogue_secrets" {
  source = "github.com/wellcomecollection/terraform-aws-secrets.git?ref=v1.3.0"

  providers = {
    aws = aws.platform
  }

  key_value_map = {
    "mets_adapter/mets_adapter/client_id" = module.catalogue_client.id
    "mets_adapter/mets_adapter/secret"    = module.catalogue_client.secret
  }
}

module "end_to_end_tester_secrets" {
  source = "github.com/wellcomecollection/terraform-aws-secrets.git?ref=v1.3.0"

  providers = {
    aws = aws.storage
  }

  key_value_map = {
    "end_to_end_bag_tester/client_id"     = module.end_to_end_client.id
    "end_to_end_bag_tester/client_secret" = module.end_to_end_client.secret
  }
}

module "dev_secrets" {
  source = "github.com/wellcomecollection/terraform-aws-secrets.git?ref=v1.3.0"

  providers = {
    aws = aws.storage
  }

  key_value_map = {
    "dev_testing/client_id"     = module.dev_client.id
    "dev_testing/client_secret" = module.dev_client.secret
  }
}
