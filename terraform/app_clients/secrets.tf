locals {
  description = "Storage service credentials; managed in https://github.com/wellcomecollection/storage-service/tree/master/terraform/app_clients"
}

module "catalogue_secrets" {
  source = "../modules/secrets"

  providers = {
    aws = aws.platform
  }

  key_value_map = {
    "mets_adapter/mets_adapter/client_id" = module.catalogue_client.id
    "mets_adapter/mets_adapter/secret"    = module.catalogue_client.secret
  }
}

module "end_to_end_tester_secrets" {
  source = "../modules/secrets"

  providers = {
    aws = aws.storage
  }

  key_value_map = {
    "end_to_end_bag_tester/client_id"     = module.end_to_end_client.id
    "end_to_end_bag_tester/client_secret" = module.end_to_end_client.secret
  }
}
