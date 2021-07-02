resource "aws_api_gateway_base_path_mapping" "mapping_staging" {
  api_id      = module.stack_staging.api_gateway_id
  domain_name = local.staging_domain_name
  base_path   = "storage"
}

module "domain" {
  source = "../modules/stack/api/domain"

  domain_name      = local.staging_domain_name
  cert_domain_name = local.cert_domain_name
}

locals {
  release_label = "stage"
}

module "stack_staging" {
  source = "../modules/stack"

  namespace = "${local.namespace}-staging"

  working_storage_bucket_prefix = "wellcomecollection-"

  api_url = local.staging_api_url

  min_capacity = 0
  max_capacity = 3

  azure_ssm_parameter_base = "azure/wecostoragestage/wellcomecollection-storage-staging-replica-netherlands"

  vpc_id = local.vpc_id

  private_subnets = local.private_subnets

  dlq_alarm_arn = local.dlq_alarm_arn

  cognito_user_pool_arn          = local.cognito_user_pool_arn
  cognito_storage_api_identifier = local.cognito_storage_api_identifier

  release_label = local.release_label

  replica_primary_bucket_name = data.terraform_remote_state.critical_staging.outputs.replica_primary_bucket_name
  replica_glacier_bucket_name = data.terraform_remote_state.critical_staging.outputs.replica_glacier_bucket_name
  azure_container_name        = "wellcomecollection-storage-staging-replica-netherlands"

  vhs_manifests_bucket_name = data.terraform_remote_state.critical_staging.outputs.vhs_manifests_bucket_name
  vhs_manifests_table_name  = data.terraform_remote_state.critical_staging.outputs.vhs_manifests_table_name

  vhs_manifests_readonly_policy  = data.terraform_remote_state.critical_staging.outputs.vhs_manifests_readonly_policy
  vhs_manifests_readwrite_policy = data.terraform_remote_state.critical_staging.outputs.vhs_manifests_readwrite_policy

  versioner_versions_table_arn   = data.terraform_remote_state.critical_staging.outputs.versions_table_arn
  versioner_versions_table_name  = data.terraform_remote_state.critical_staging.outputs.versions_table_name
  versioner_versions_table_index = data.terraform_remote_state.critical_staging.outputs.versions_table_index

  alarm_topic_arn = local.gateway_server_error_alarm_arn

  ingests_table_name = data.terraform_remote_state.critical_staging.outputs.ingests_table_name
  ingests_table_arn  = data.terraform_remote_state.critical_staging.outputs.ingests_table_arn

  replicas_table_arn  = data.terraform_remote_state.critical_staging.outputs.replicas_table_arn
  replicas_table_name = data.terraform_remote_state.critical_staging.outputs.replicas_table_name

  indexer_host_secrets = {
    es_host     = "staging/indexer/es_host"
    es_port     = "staging/indexer/es_port"
    es_protocol = "staging/indexer/es_protocol"
  }

  es_ingests_index_name = "storage_stage_ingests"

  ingests_indexer_secrets = {
    es_username = "staging/indexer/ingests/es_username"
    es_password = "staging/indexer/ingests/es_password"
  }

  es_bags_index_name = "storage_stage_bags"

  bag_indexer_secrets = {
    es_username = "staging/indexer/bags/es_username"
    es_password = "staging/indexer/bags/es_password"
  }

  es_files_index_name = "storage_stage_files"

  file_indexer_secrets = {
    es_username = "staging/indexer/files/es_username"
    es_password = "staging/indexer/files/es_password"
  }

  upload_bucket_arns = [
    local.workflow_bucket_arn,
    local.archivematica_ingests_bucket_arn,
  ]

  bag_register_output_subscribe_principals = []

  # This means the staging service might be interrupted (unlikely), but the
  # staging service doesn't make the same guarantees of uptime and this
  # saves us money.
  use_fargate_spot_for_api = true

  logging_container = {
    container_registry = "760097843905.dkr.ecr.eu-west-1.amazonaws.com/uk.ac.wellcome"
    container_name     = "fluentbit"
    container_tag      = "2ccd2c68f38aa77a8ac1a32fe3ea54bbbd397a38"
  }

  nginx_container = {
    container_registry = "760097843905.dkr.ecr.eu-west-1.amazonaws.com/uk.ac.wellcome"
    container_name     = "nginx_apigw"
    container_tag      = "f1188c2a7df01663dd96c99b26666085a4192167"
  }

  app_containers = {
    container_registry = "975596993436.dkr.ecr.eu-west-1.amazonaws.com/uk.ac.wellcome"
    container_tag      = "env.${local.release_label}"
  }
}

