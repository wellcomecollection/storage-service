resource "aws_api_gateway_base_path_mapping" "mapping_staging" {
  api_id      = module.stack_staging.api_gateway_id
  domain_name = local.staging_domain_name
  base_path   = "storage"
}

module "stack_staging" {
  source = "../modules/stack"

  namespace = "${local.namespace}-staging"

  api_url          = local.staging_api_url
  domain_name      = local.staging_domain_name
  cert_domain_name = local.cert_domain_name

  min_capacity = 0
  max_capacity = 3

  azure_ssm_parameter_base = "azure/wecostoragestage/wellcomecollection-storage-staging-replica-netherlands"

  vpc_id = local.vpc_id

  private_subnets = local.private_subnets

  dlq_alarm_arn = local.dlq_alarm_arn

  cognito_user_pool_arn          = local.cognito_user_pool_arn
  cognito_storage_api_identifier = local.cognito_storage_api_identifier

  release_label = "stage"

  replica_primary_bucket_name = data.terraform_remote_state.critical_staging.outputs.replica_primary_bucket_name
  replica_glacier_bucket_name = data.terraform_remote_state.critical_staging.outputs.replica_glacier_bucket_name
  azure_container_name        = "wellcomecollection-storage-staging-replica-netherlands"
  static_content_bucket_name  = data.terraform_remote_state.critical_staging.outputs.static_content_bucket_name

  vhs_manifests_bucket_name = data.terraform_remote_state.critical_staging.outputs.vhs_manifests_bucket_name
  vhs_manifests_table_name  = data.terraform_remote_state.critical_staging.outputs.vhs_manifests_table_name

  vhs_manifests_readonly_policy  = data.terraform_remote_state.critical_staging.outputs.vhs_manifests_readonly_policy
  vhs_manifests_readwrite_policy = data.terraform_remote_state.critical_staging.outputs.vhs_manifests_readwrite_policy

  # backfill VHS
  vhs_manifests_bucket_name_backfill = data.terraform_remote_state.critical_staging.outputs.vhs_manifests_bucket_name_backfill
  vhs_manifests_table_name_backfill  = data.terraform_remote_state.critical_staging.outputs.vhs_manifests_table_name_backfill

  vhs_manifests_readonly_policy_backfill  = data.terraform_remote_state.critical_staging.outputs.vhs_manifests_readonly_policy_backfill
  vhs_manifests_readwrite_policy_backfill = data.terraform_remote_state.critical_staging.outputs.vhs_manifests_readwrite_policy_backfill

  versioner_versions_table_arn   = data.terraform_remote_state.critical_staging.outputs.versions_table_arn
  versioner_versions_table_name  = data.terraform_remote_state.critical_staging.outputs.versions_table_name
  versioner_versions_table_index = data.terraform_remote_state.critical_staging.outputs.versions_table_index

  alarm_topic_arn = local.gateway_server_error_alarm_arn

  ingests_table_name = data.terraform_remote_state.critical_staging.outputs.ingests_table_name
  ingests_table_arn  = data.terraform_remote_state.critical_staging.outputs.ingests_table_arn

  replicas_table_arn  = data.terraform_remote_state.critical_staging.outputs.replicas_table_arn
  replicas_table_name = data.terraform_remote_state.critical_staging.outputs.replicas_table_name

  es_ingests_index_name = "storage_stage_ingests"

  ingests_indexer_secrets = {
    es_host     = "stage/ingests_indexer/es_host"
    es_port     = "stage/ingests_indexer/es_port"
    es_protocol = "stage/ingests_indexer/es_protocol"
    es_username = "stage/ingests_indexer/es_username"
    es_password = "stage/ingests_indexer/es_password"
  }

  es_bags_index_name = "storage_stage_bags"

  bag_indexer_secrets = {
    es_host     = "stage/bag_indexer/es_host"
    es_port     = "stage/bag_indexer/es_port"
    es_protocol = "stage/bag_indexer/es_protocol"
    es_username = "stage/bag_indexer/es_username"
    es_password = "stage/bag_indexer/es_password"
  }

  workflow_bucket_name = local.workflow_staging_bucket_name

  archivematica_ingests_bucket             = local.archivematica_ingests_bucket
  bag_register_output_subscribe_principals = []

  # This means the staging service might be interrupted (unlikely), but the
  # staging service doesn't make the same guarantees of uptime and this
  # saves us money.
  use_fargate_spot_for_api = true

  tags = local.default_tags
}

