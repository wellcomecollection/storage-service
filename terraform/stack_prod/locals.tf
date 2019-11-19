locals {
  namespace = "storage"

  api_url     = "https://api.wellcomecollection.org"
  domain_name = "storage.api.wellcomecollection.org"

  vpc_id          = data.terraform_remote_state.infra_shared.outputs.storage_vpc_id
  private_subnets = data.terraform_remote_state.infra_shared.outputs.storage_vpc_private_subnets

  cert_domain_name = "storage.api.wellcomecollection.org"

  dlq_alarm_arn = data.terraform_remote_state.infra_shared.outputs.dlq_alarm_arn

  cognito_user_pool_arn          = data.terraform_remote_state.infra_critical.outputs.cognito_user_pool_arn
  cognito_storage_api_identifier = data.terraform_remote_state.infra_critical.outputs.cognito_storage_api_identifier

  nginx_image = "760097843905.dkr.ecr.eu-west-1.amazonaws.com/uk.ac.wellcome/nginx_api-gw:bad0dbfa548874938d16496e313b05adb71268b7"

  gateway_server_error_alarm_arn = data.terraform_remote_state.infra_shared.outputs.gateway_server_error_alarm_arn

  workflow_bucket_name = "wellcomecollection-workflow-export-bagit"

  subnets_ids = [
    data.terraform_remote_state.infra_shared.outputs.storage_vpc_private_subnets[0],
    data.terraform_remote_state.infra_shared.outputs.storage_vpc_private_subnets[2],
  ]

  service-wt-winnipeg = data.terraform_remote_state.infra_shared.outputs.service-wt-winnipeg
}

