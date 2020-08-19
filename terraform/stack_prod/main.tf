resource "aws_api_gateway_base_path_mapping" "mapping_prod" {
  api_id      = module.stack_prod.api_gateway_id
  domain_name = local.domain_name
  base_path   = "storage"
}

module "stack_prod" {
  source = "../modules/stack"

  namespace = "${local.namespace}-prod"

  api_url          = local.api_url
  domain_name      = local.domain_name
  cert_domain_name = local.cert_domain_name

  min_capacity = 0
  max_capacity = 10

  azure_ssm_parameter_base = "azure/wecostorageprod/wellcomecollection-storage-replica-netherlands"

  vpc_id = local.vpc_id

  private_subnets = local.private_subnets

  dlq_alarm_arn = local.dlq_alarm_arn

  cognito_user_pool_arn          = local.cognito_user_pool_arn
  cognito_storage_api_identifier = local.cognito_storage_api_identifier

  release_label = "prod"

  replica_primary_bucket_name = data.terraform_remote_state.critical_prod.outputs.replica_primary_bucket_name
  replica_glacier_bucket_name = data.terraform_remote_state.critical_prod.outputs.replica_glacier_bucket_name
  azure_container_name        = "wellcomecollection-storage-replica-netherlands"

  static_content_bucket_name = data.terraform_remote_state.critical_prod.outputs.static_content_bucket_name

  vhs_manifests_bucket_name = data.terraform_remote_state.critical_prod.outputs.vhs_manifests_bucket_name
  vhs_manifests_table_name  = data.terraform_remote_state.critical_prod.outputs.vhs_manifests_table_name

  vhs_manifests_readonly_policy  = data.terraform_remote_state.critical_prod.outputs.vhs_manifests_readonly_policy
  vhs_manifests_readwrite_policy = data.terraform_remote_state.critical_prod.outputs.vhs_manifests_readwrite_policy

  versioner_versions_table_arn   = data.terraform_remote_state.critical_prod.outputs.versions_table_arn
  versioner_versions_table_name  = data.terraform_remote_state.critical_prod.outputs.versions_table_name
  versioner_versions_table_index = data.terraform_remote_state.critical_prod.outputs.versions_table_index

  alarm_topic_arn = local.gateway_server_error_alarm_arn

  ingests_table_name = data.terraform_remote_state.critical_prod.outputs.ingests_table_name
  ingests_table_arn  = data.terraform_remote_state.critical_prod.outputs.ingests_table_arn

  replicas_table_arn  = data.terraform_remote_state.critical_prod.outputs.replicas_table_arn
  replicas_table_name = data.terraform_remote_state.critical_prod.outputs.replicas_table_name

  es_ingests_index_name = "storage_ingests"

  ingests_indexer_secrets = {
    es_host     = "prod/ingests_indexer/es_host"
    es_port     = "prod/ingests_indexer/es_port"
    es_protocol = "prod/ingests_indexer/es_protocol"
    es_username = "prod/ingests_indexer/es_username"
    es_password = "prod/ingests_indexer/es_password"
  }

  es_bags_index_name = "storage_bags"

  bag_indexer_secrets = {
    es_host     = "prod/bag_indexer/es_host"
    es_port     = "prod/bag_indexer/es_port"
    es_protocol = "prod/bag_indexer/es_protocol"
    es_username = "prod/bag_indexer/es_username"
    es_password = "prod/bag_indexer/es_password"
  }

  workflow_bucket_name = local.workflow_bucket_name

  archivematica_ingests_bucket             = data.terraform_remote_state.archivematica_infra.outputs.ingests_bucket_arn
  bag_register_output_subscribe_principals = [local.catalogue_pipeline_account_principal]

  tags                                    = local.default_tags
  vhs_manifests_bucket_name_backfill      = data.terraform_remote_state.critical_prod.outputs.vhs_manifests_bucket_name_backfill
  vhs_manifests_table_name_backfill       = data.terraform_remote_state.critical_prod.outputs.vhs_manifests_table_name_backfill
  vhs_manifests_readonly_policy_backfill  = data.terraform_remote_state.critical_prod.outputs.vhs_manifests_readonly_policy_backfill
  vhs_manifests_readwrite_policy_backfill = data.terraform_remote_state.critical_prod.outputs.vhs_manifests_readwrite_policy_backfill
}

