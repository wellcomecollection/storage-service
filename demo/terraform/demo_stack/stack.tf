module "stack" {
  source = "../../../terraform/modules/stack"

  namespace = var.short_namespace

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  vpc_id = module.vpc.vpc_id

  private_subnets = module.vpc.private_subnets

  dlq_alarm_arn   = aws_sns_topic.dlq_alarm.arn
  alarm_topic_arn = aws_sns_topic.gateway_server_error_alarm.arn

  cognito_user_pool_arn          = aws_cognito_user_pool.pool.arn
  cognito_storage_api_identifier = aws_cognito_resource_server.storage_api.identifier

  release_label = "prod"

  replica_primary_bucket_name = aws_s3_bucket.replica_primary.id
  replica_glacier_bucket_name = aws_s3_bucket.replica_glacier.id

  working_storage_bucket_prefix = "${var.namespace}-"

  # Setting these both to empty strings means the storage service won't
  # try to replicate bags to Azure.
  azure_container_name     = null
  azure_ssm_parameter_base = ""

  vhs_manifests_bucket_name = module.metadata_stores.vhs_manifests_bucket_name
  vhs_manifests_table_name  = module.metadata_stores.vhs_manifests_table_name

  vhs_manifests_readonly_policy  = module.metadata_stores.vhs_manifests_readonly_policy
  vhs_manifests_readwrite_policy = module.metadata_stores.vhs_manifests_readwrite_policy

  versioner_versions_table_arn   = module.metadata_stores.versions_table_arn
  versioner_versions_table_name  = module.metadata_stores.versions_table_name
  versioner_versions_table_index = module.metadata_stores.versions_table_index

  ingests_table_name = module.metadata_stores.ingests_table_name
  ingests_table_arn  = module.metadata_stores.ingests_table_arn

  replicas_table_arn  = module.metadata_stores.replicas_table_arn
  replicas_table_name = module.metadata_stores.replicas_table_name

  indexer_host_secrets = {
    es_host     = "elasticsearch/host"
    es_port     = "elasticsearch/port"
    es_protocol = "elasticsearch/protocol"
  }

  es_ingests_index_name = "storage_ingests"

  ingests_indexer_secrets = {
    es_username = "elasticsearch/user"
    es_password = "elasticsearch/password"
  }

  es_bags_index_name = "storage_bags"

  bag_indexer_secrets = {
    es_username = "elasticsearch/user"
    es_password = "elasticsearch/password"
  }

  es_files_index_name = "storage_files"

  file_indexer_secrets = {
    es_username = "elasticsearch/user"
    es_password = "elasticsearch/password"
  }

  upload_bucket_arns = [
    aws_s3_bucket.uploads.arn,
  ]

  bag_register_output_subscribe_principals = []

  depends_on = [
    aws_iam_service_linked_role.autoscaling_linked_role,
    module.elasticsearch_secrets,
  ]

  logging_container = {
    container_registry = "public.ecr.aws/l7a1d1z4"
    container_name     = "fluentbit"
    container_tag      = "8c68706fba97189fc72e3d0844458c7c5dee0bbc"
  }

  nginx_container = {
    container_registry = "public.ecr.aws/l7a1d1z4"
    container_name     = "nginx_apigw"
    container_tag      = "8c68706fba97189fc72e3d0844458c7c5dee0bbc"
  }

  app_containers = {
    container_registry = "public.ecr.aws/y1p3h6z3"
    container_tag      = "ref.c3b7e5a1557ba0115a31ad06e6a7edd5ad143a64"
  }
}
