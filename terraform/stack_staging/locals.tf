locals {
  namespace = "storage"

  staging_api_url     = "https://api-stage.wellcomecollection.org"
  staging_domain_name = "storage.api-stage.wellcomecollection.org"

  vpc_id          = local.storage_vpcs["storage_vpc_id"]
  private_subnets = local.storage_vpcs["storage_vpc_private_subnets"]

  cert_domain_name = "storage.api.wellcomecollection.org"

  monitoring_outputs = data.terraform_remote_state.monitoring.outputs

  dlq_alarm_arn               = local.monitoring_outputs["storage_dlq_alarm_topic_arn"]
  api_gateway_alarm_topic_arn = local.monitoring_outputs["storage_api_gateway_alerts_topic_arn"]

  cognito_user_pool_arn          = data.terraform_remote_state.app_clients.outputs.cognito_user_pool_arn
  cognito_storage_api_identifier = data.terraform_remote_state.app_clients.outputs.cognito_storage_api_identifier

  # TODO: This value should be exported from the workflow-infra state, not hard-coded
  workflow_bucket_arn              = "arn:aws:s3:::wellcomecollection-workflow-export-bagit-stage"
  archivematica_ingests_bucket_arn = data.terraform_remote_state.archivematica_infra.outputs.ingests_bucket_arn
}
