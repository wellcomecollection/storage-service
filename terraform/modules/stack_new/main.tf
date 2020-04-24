module "replicator_verifier_primary" {
  source = "./replicator_verifier_pair"

  namespace = var.namespace

  replica_id           = "primary"
  replica_display_name = "primary location"
  storage_provider     = "aws-s3-ia"
  replica_type         = "primary"

  topic_arns = [
    module.bag_versioner_output_topic.arn,
  ]

  bucket_name         = var.replica_primary_bucket_name
  primary_bucket_name = var.replica_primary_bucket_name

  ingests_read_policy_json          = data.aws_iam_policy_document.unpacked_bags_bucket_readonly.json
  cloudwatch_metrics_policy_json    = data.aws_iam_policy_document.cloudwatch_putmetrics.json
  replicator_lock_table_policy_json = module.replicator_lock_table.iam_policy

  security_group_ids = [
    aws_security_group.interservice.id,
    aws_security_group.service_egress.id,
  ]

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn

  subnets      = var.private_subnets

  ingests_topic_arn = module.ingests_topic.arn

  replicator_lock_table_name  = module.replicator_lock_table.table_name
  replicator_lock_table_index = module.replicator_lock_table.index_name

  bag_replicator_image = local.bag_replicator_image
  bag_verifier_image   = local.bag_verifier_image

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  dlq_alarm_arn = var.dlq_alarm_arn

  aws_region = var.aws_region

  service_discovery_namespace_id = local.service_discovery_namespace_id
}

module "replicator_verifier_glacier" {
  source = "./replicator_verifier_pair"

  namespace = var.namespace

  replica_id           = "glacier"
  replica_display_name = "Amazon Glacier"
  storage_provider     = "aws-s3-glacier"
  replica_type         = "secondary"

  topic_arns = [
    module.bag_versioner_output_topic.arn,
  ]

  bucket_name         = var.replica_glacier_bucket_name
  primary_bucket_name = var.replica_primary_bucket_name

  ingests_read_policy_json          = data.aws_iam_policy_document.unpacked_bags_bucket_readonly.json
  cloudwatch_metrics_policy_json    = data.aws_iam_policy_document.cloudwatch_putmetrics.json
  replicator_lock_table_policy_json = module.replicator_lock_table.iam_policy

  security_group_ids = [
    aws_security_group.interservice.id,
    aws_security_group.service_egress.id,
  ]

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn

  subnets      = var.private_subnets

  ingests_topic_arn = module.ingests_topic.arn

  replicator_lock_table_name  = module.replicator_lock_table.table_name
  replicator_lock_table_index = module.replicator_lock_table.index_name

  bag_replicator_image = local.bag_replicator_image
  bag_verifier_image   = local.bag_verifier_image

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  dlq_alarm_arn = var.dlq_alarm_arn

  aws_region = var.aws_region

  service_discovery_namespace_id = local.service_discovery_namespace_id
}

module "api" {
  source = "./api"

  vpc_id      = var.vpc_id
  cluster_arn = aws_ecs_cluster.cluster.arn
  subnets     = var.private_subnets

  domain_name      = var.domain_name
  cert_domain_name = var.cert_domain_name

  namespace    = var.namespace
  namespace_id = aws_service_discovery_private_dns_namespace.namespace.id

  # Auth

  auth_scopes = [
    "${var.cognito_storage_api_identifier}/ingests",
    "${var.cognito_storage_api_identifier}/bags",
  ]
  cognito_user_pool_arn = var.cognito_user_pool_arn

  # Bags endpoint

  bags_container_image = local.bags_api_image
  bags_container_port  = 9001
  bags_env_vars = {
    context_url           = "${var.api_url}/context.json"
    app_base_url          = "${var.api_url}/storage/v1/bags"
    vhs_bucket_name       = var.vhs_manifests_bucket_name
    vhs_table_name        = var.vhs_manifests_table_name
    metrics_namespace     = local.bags_api_service_name
    responses_bucket_name = aws_s3_bucket.large_response_cache.id

    //TODO: remove application reference, then this
    logstash_host = "localhost"
  }
  bags_env_vars_length       = 8
  bags_nginx_container_image = var.nginx_image
  bags_nginx_container_port  = 9000

  # Ingests endpoint

  ingests_container_image = local.ingests_api_image
  ingests_container_port  = 9001
  ingests_env_vars = {
    context_url               = "${var.api_url}/context.json"
    app_base_url              = "${var.api_url}/storage/v1/ingests"
    unpacker_topic_arn        = module.bag_unpacker_input_topic.arn
    archive_ingest_table_name = var.ingests_table_name
    metrics_namespace         = local.ingests_api_service_name

    //TODO: remove application reference, then this
    logstash_host = "localhost"
  }
  ingests_env_vars_length        = 7
  ingests_nginx_container_image  = var.nginx_image
  ingests_nginx_container_port   = 9000
  static_content_bucket_name     = var.static_content_bucket_name
  interservice_security_group_id = aws_security_group.interservice.id
  alarm_topic_arn                = var.alarm_topic_arn
  bag_unpacker_topic_arn         = module.bag_unpacker_input_topic.arn

  # The number of API tasks MUST be one per AZ.  This is due to the behaviour of
  # NLBs that seem to increase latency significantly if number of tasks < number of AZs.
  desired_bags_api_count    = max(3, var.desired_bags_api_count)
  desired_ingests_api_count = max(3, var.desired_ingests_api_count)

  use_fargate_spot_for_api = var.use_fargate_spot_for_api
}

