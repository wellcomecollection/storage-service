locals {
  release_label = "prod"
}

module "stack_prod" {
  source = "../modules/stack"

  namespace = "${local.namespace}-prod"

  working_storage_bucket_prefix = "wellcomecollection-"

  api_url = local.api_url

  min_capacity = 0
  max_capacity = 10

  azure_ssm_parameter_base = "azure/wecostorageprod/wellcomecollection-storage-replica-netherlands"

  vpc_id = local.vpc_id

  private_subnets = local.private_subnets

  dlq_alarm_arn = local.dlq_alarm_arn

  cognito_user_pool_arn          = local.cognito_user_pool_arn
  cognito_storage_api_identifier = local.cognito_storage_api_identifier

  release_label = local.release_label

  replica_primary_bucket_name = data.terraform_remote_state.critical_prod.outputs.replica_primary_bucket_name
  replica_glacier_bucket_name = data.terraform_remote_state.critical_prod.outputs.replica_glacier_bucket_name
  azure_container_name        = "wellcomecollection-storage-replica-netherlands"

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

  indexer_host_secrets = {
    es_host     = "prod/indexer/es_host"
    es_port     = "prod/indexer/es_port"
    es_protocol = "prod/indexer/es_protocol"
  }

  es_ingests_index_name = "storage_ingests"

  ingests_indexer_secrets = {
    es_username = "prod/indexer/ingests/es_username"
    es_password = "prod/indexer/ingests/es_password"
  }

  es_bags_index_name = "storage_bags"

  bag_indexer_secrets = {
    es_username = "prod/indexer/bags/es_username"
    es_password = "prod/indexer/bags/es_password"
  }

  es_files_index_name = "storage_files"

  file_indexer_secrets = {
    es_username = "prod/indexer/files/es_username"
    es_password = "prod/indexer/files/es_password"
  }

  upload_bucket_arns = [
    local.workflow_bucket_arn,
    local.archivematica_ingests_bucket_arn,
  ]

  bag_register_output_subscribe_principals = [local.catalogue_pipeline_account_principal]

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
