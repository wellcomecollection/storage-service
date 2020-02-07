locals {
  namespace = "storage"

  staging_api_url     = "https://api-stage.wellcomecollection.org"
  staging_domain_name = "storage.api-stage.wellcomecollection.org"

  vpc_id          = data.terraform_remote_state.infra_shared.outputs.storage_vpc_id
  private_subnets = data.terraform_remote_state.infra_shared.outputs.storage_vpc_private_subnets

  cert_domain_name = "storage.api.wellcomecollection.org"

  dlq_alarm_arn = data.terraform_remote_state.infra_shared.outputs.dlq_alarm_arn

  cognito_user_pool_arn          = data.terraform_remote_state.infra_critical.outputs.cognito_user_pool_arn
  cognito_storage_api_identifier = data.terraform_remote_state.infra_critical.outputs.cognito_storage_api_identifier

  nginx_image = "975596993436.dkr.ecr.eu-west-1.amazonaws.com/uk.ac.wellcome/nginx:95821b39e1683a0a7098cd67d82bc690415c1d5a"

  gateway_server_error_alarm_arn = data.terraform_remote_state.infra_shared.outputs.gateway_server_error_alarm_arn

  workflow_staging_bucket_name = "wellcomecollection-workflow-export-bagit-stage"

  archivematica_ingests_bucket = data.terraform_remote_state.archivematica_infra.outputs.ingests_bucket_arn
}

