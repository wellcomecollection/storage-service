locals {
  java_opts_heap_size = "-Xss6M -Xms2G -Xmx3G"
}

# logstash_transit

module "logstash_transit" {
  source = "../service/worker"

  security_group_ids = [
    aws_security_group.interservice.id,
    aws_security_group.service_egress.id,
  ]

  cluster_arn  = aws_ecs_cluster.cluster.arn
  cluster_name = aws_ecs_cluster.cluster.name
  namespace_id = aws_service_discovery_private_dns_namespace.namespace.id
  subnets      = var.private_subnets
  service_name = local.logstash_transit_service_name

  env_vars = {
    XPACK_MONITORING_ENABLED = "false"
    NAMESPACE                = var.namespace
  }

  secret_env_vars = {
    ES_HOST = "storage/logstash/es_host"
    ES_USER = "storage/logstash/es_user"
    ES_PASS = "storage/logstash/es_pass"
  }

  cpu    = 1024
  memory = 2048

  container_image = local.logstash_transit_image
}

# bag_unpacker

module "bag_unpacker" {
  source = "../service/scaling_worker"

  security_group_ids = [
    aws_security_group.interservice.id,
    aws_security_group.service_egress.id,
  ]

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn
  namespace_id = aws_service_discovery_private_dns_namespace.namespace.id
  subnets      = var.private_subnets
  service_name = local.bag_unpacker_service_name

  env_vars = {
    queue_url               = module.bag_unpacker_queue.url
    destination_bucket_name = aws_s3_bucket.unpacked_bags.bucket
    ingest_topic_arn        = module.ingests_topic.arn
    outgoing_topic_arn      = module.bag_unpacker_output_topic.arn
    metrics_namespace       = local.bag_unpacker_service_name
    operation_name          = "unpacking"
    logstash_host           = local.logstash_host
    JAVA_OPTS               = local.java_opts_heap_size
    # If you run the unpacker with too much parallelism, it gets overwhelmed
    # and tries to open too many HTTP connections.  You get this error:
    #
    #     com.amazonaws.SdkClientException: Unable to execute HTTP request:
    #     Timeout waiting for connection from pool
    #
    # If you start seeing this error, consider turning this down.
    #
    queue_parallelism = 10
  }

  cpu    = 2048
  memory = 4096

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  container_image = local.bag_unpacker_image
}

# bag root finder

module "bag_root_finder" {
  source = "../service/scaling_worker"

  security_group_ids = [
    aws_security_group.interservice.id,
    aws_security_group.service_egress.id,
  ]

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn
  namespace_id = aws_service_discovery_private_dns_namespace.namespace.id
  subnets      = var.private_subnets
  service_name = local.bag_root_finder_service_name

  env_vars = {
    queue_url          = module.bag_root_finder_queue.url
    ingest_topic_arn   = module.ingests_topic.arn
    outgoing_topic_arn = module.bag_root_finder_output_topic.arn
    metrics_namespace  = local.bag_root_finder_service_name
    operation_name     = "detecting bag root"
    logstash_host      = local.logstash_host
  }

  cpu    = 512
  memory = 1024

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  container_image = local.bag_root_finder_image

  use_fargate_spot = true
}

# bag_verifier

module "bag_verifier_pre_replication" {
  source = "../service/scaling_worker"

  security_group_ids = [
    aws_security_group.interservice.id,
    aws_security_group.service_egress.id,
  ]

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn
  namespace_id = aws_service_discovery_private_dns_namespace.namespace.id
  subnets      = var.private_subnets
  service_name = local.bag_verifier_pre_repl_service_name

  env_vars = {
    queue_url          = module.bag_verifier_pre_replicate_queue.url
    ingest_topic_arn   = module.ingests_topic.arn
    outgoing_topic_arn = module.bag_verifier_pre_replicate_output_topic.arn
    metrics_namespace  = local.bag_verifier_pre_repl_service_name
    operation_name     = "verification (pre-replicating to archive storage)"
    logstash_host      = local.logstash_host
    JAVA_OPTS          = local.java_opts_heap_size
  }

  cpu    = 2048
  memory = 4096

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  container_image = local.bag_verifier_image
}

# bag versioner

module "bag_versioner" {
  source = "../service/scaling_worker"

  security_group_ids = [
    aws_security_group.interservice.id,
    aws_security_group.service_egress.id,
  ]

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn
  namespace_id = aws_service_discovery_private_dns_namespace.namespace.id
  subnets      = var.private_subnets
  service_name = local.bag_versioner_service_name

  env_vars = {
    queue_url            = module.bag_versioner_queue.url
    ingest_topic_arn     = module.ingests_topic.arn
    outgoing_topic_arn   = module.bag_versioner_output_topic.arn
    metrics_namespace    = local.bag_versioner_service_name
    operation_name       = "assigning bag version"
    logstash_host        = local.logstash_host
    locking_table_name   = module.versioner_lock_table.table_name
    locking_table_index  = module.versioner_lock_table.index_name
    versions_table_name  = var.versioner_versions_table_name
    versions_table_index = var.versioner_versions_table_index
  }

  cpu    = 512
  memory = 1024

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  container_image = local.bag_versioner_image

  use_fargate_spot = true
}

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
  namespace_id = aws_service_discovery_private_dns_namespace.namespace.id
  subnets      = var.private_subnets

  ingests_topic_arn = module.ingests_topic.arn
  logstash_host     = local.logstash_host

  replicator_lock_table_name  = module.replicator_lock_table.table_name
  replicator_lock_table_index = module.replicator_lock_table.index_name

  bag_replicator_image = local.bag_replicator_image
  bag_verifier_image   = local.bag_verifier_image

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  dlq_alarm_arn = var.dlq_alarm_arn

  aws_region = var.aws_region
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
  namespace_id = aws_service_discovery_private_dns_namespace.namespace.id
  subnets      = var.private_subnets

  ingests_topic_arn = module.ingests_topic.arn
  logstash_host     = local.logstash_host

  replicator_lock_table_name  = module.replicator_lock_table.table_name
  replicator_lock_table_index = module.replicator_lock_table.index_name

  bag_replicator_image = local.bag_replicator_image
  bag_verifier_image   = local.bag_verifier_image

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  dlq_alarm_arn = var.dlq_alarm_arn

  aws_region = var.aws_region
}

# replica_aggregator

module "replica_aggregator" {
  source = "../service/scaling_worker"

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn
  namespace_id = aws_service_discovery_private_dns_namespace.namespace.id
  subnets      = var.private_subnets
  service_name = "${var.namespace}-replica_aggregator"

  env_vars = {
    replicas_table_name    = var.replicas_table_name
    queue_url              = module.replica_aggregator_input_queue.url
    outgoing_topic_arn     = module.replica_aggregator_output_topic.arn
    ingest_topic_arn       = module.ingests_topic.arn
    metrics_namespace      = local.bag_register_service_name
    operation_name         = "Aggregating replicas"
    logstash_host          = local.logstash_host
    expected_replica_count = 2
    JAVA_OPTS              = local.java_opts_heap_size
  }

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  container_image = local.replica_aggregator_image

  use_fargate_spot = true
}

# bag_register

module "bag_register" {
  source = "../service/scaling_worker"

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn
  namespace_id = aws_service_discovery_private_dns_namespace.namespace.id
  subnets      = var.private_subnets
  service_name = "${var.namespace}-bag_register"

  env_vars = {
    queue_url         = module.bag_register_input_queue.url
    archive_bucket    = var.replica_primary_bucket_name
    ongoing_topic_arn = module.bag_register_output_topic.arn
    ingest_topic_arn  = module.ingests_topic.arn
    vhs_bucket_name   = var.vhs_manifests_bucket_name
    vhs_table_name    = var.vhs_manifests_table_name
    metrics_namespace = local.bag_register_service_name
    operation_name    = "register"
    logstash_host     = local.logstash_host
    JAVA_OPTS         = local.java_opts_heap_size
  }

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  container_image = local.bag_register_image

  use_fargate_spot = true
}

# notifier

module "notifier" {
  source = "../service/scaling_worker"

  security_group_ids = [
    aws_security_group.interservice.id,
    aws_security_group.service_egress.id,
  ]

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn
  namespace_id = aws_service_discovery_private_dns_namespace.namespace.id
  subnets      = var.private_subnets
  service_name = "${var.namespace}-notifier"

  env_vars = {
    context_url        = "https://api.wellcomecollection.org/storage/v1/context.json"
    notifier_queue_url = module.notifier_input_queue.url
    ingest_topic_arn   = module.ingests_topic.arn
    metrics_namespace  = local.notifier_service_name
    logstash_host      = local.logstash_host
  }

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  container_image = local.notifier_image

  use_fargate_spot = true
}

# ingests


//TODO: Fix issue with underlying module task role nonesense
//module "ingests" {
//  source = "../service_new/worker"
//
//  container_image = local.ingests_image
//
//  cluster_name = aws_ecs_cluster.cluster.name
//  cluster_arn  = aws_ecs_cluster.cluster.arn
//
//  subnets      = var.private_subnets
//  service_name = "${var.namespace}-ingests"
//
//  environment = {
//    queue_url                        = module.ingests_input_queue.url
//    callback_notifications_topic_arn = module.ingests_monitor_callback_notifications_topic.arn
//    updated_ingests_topic_arn        = module.updated_ingests_topic.arn
//    ingests_table_name               = var.ingests_table_name
//    metrics_namespace                = local.ingests_service_name
//
//    //TODO: remove application reference, then this
//    logstash_host = "localhost"
//  }
//
//  # We always run at least one ingests monitor so messages from other apps are
//  # displayed in the API immediately.
//  min_capacity = max(1, var.min_capacity)
//  max_capacity = var.max_capacity
//
//  use_fargate_spot = true
//
//  service_discovery_namespace_id = aws_service_discovery_private_dns_namespace.namespace.id
//}

module "ingests" {
  source = "../service/scaling_worker"

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn

  namespace_id = aws_service_discovery_private_dns_namespace.namespace.id
  subnets      = var.private_subnets
  service_name = "${var.namespace}-ingests"

  env_vars = {
    queue_url                        = module.ingests_input_queue.url
    callback_notifications_topic_arn = module.ingests_monitor_callback_notifications_topic.arn
    updated_ingests_topic_arn        = module.updated_ingests_topic.arn
    ingests_table_name               = var.ingests_table_name
    metrics_namespace                = local.ingests_service_name
    logstash_host                    = local.logstash_host
  }

  # We always run at least one ingests monitor so messages from other apps are
  # displayed in the API immediately.
  min_capacity = max(1, var.min_capacity)
  max_capacity = var.max_capacity

  container_image = local.ingests_image

  use_fargate_spot = true
}

# Storage API

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
    logstash_host         = local.logstash_host
    responses_bucket_name = aws_s3_bucket.large_response_cache.id
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
    logstash_host             = local.logstash_host
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

