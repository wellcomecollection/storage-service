locals {
  description = "Storage service credentials; managed in https://github.com/wellcomecollection/storage-service/tree/master/terraform/app_clients"
}

# Store the Catalogue client credentials in KMS for the METS adapter

resource "aws_secretsmanager_secret" "catalogue_client_id" {
  name        = "mets_adapter/mets_adapter/client_id"
  description = local.description
}

resource "aws_secretsmanager_secret_version" "catalogue_client_id" {
  secret_id     = aws_secretsmanager_secret.catalogue_client_id.id
  secret_string = module.catalogue_client.id
}

resource "aws_secretsmanager_secret" "catalogue_client_secret" {
  name        = "mets_adapter/mets_adapter/secret"
  description = local.description
}

resource "aws_secretsmanager_secret_version" "catalogue_client_secret" {
  secret_id     = aws_secretsmanager_secret.catalogue_client_secret.id
  secret_string = module.catalogue_client.secret
}
