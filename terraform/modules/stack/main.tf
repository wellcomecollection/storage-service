locals {
  java_opts_metrics_base = "-Dcom.amazonaws.sdk.enableDefaultMetrics=cloudwatchRegion=${var.aws_region}"
  java_opts_heap_size = "-Xss6M -Xms2G -Xmx3G"
}

# logstash_transit

module "logstash_transit" {
  source = "git::https://github.com/wellcometrust/terraform.git//ecs/prebuilt/default?ref=v19.15.0"

  security_group_ids = [
    "${aws_security_group.interservice.id}",
    "${aws_security_group.service_egress.id}",
  ]

  cluster_id   = "${aws_ecs_cluster.cluster.id}"
  namespace_id = "${aws_service_discovery_private_dns_namespace.namespace.id}"
  subnets      = "${var.private_subnets}"
  service_name = "${local.logstash_transit_service_name}"

  env_vars = {
    XPACK_MONITORING_ENABLED = "false"
    NAMESPACE                = "${var.namespace}"
  }

  env_vars_length = 2

  secret_env_vars = {
    ES_HOST = "storage/logstash/es_host"
    ES_USER = "storage/logstash/es_user"
    ES_PASS = "storage/logstash/es_pass"
  }

  secret_env_vars_length = 3

  cpu    = 1024
  memory = 2048

  container_image = "${local.logstash_transit_image}"
}

# bag_unpacker

module "bag_unpacker" {
  source = "../service/worker"

  security_group_ids = [
    "${aws_security_group.interservice.id}",
    "${aws_security_group.service_egress.id}",
  ]

  cluster_name = "${aws_ecs_cluster.cluster.name}"
  cluster_id   = "${aws_ecs_cluster.cluster.id}"
  namespace_id = "${aws_service_discovery_private_dns_namespace.namespace.id}"
  subnets      = "${var.private_subnets}"
  service_name = "${local.bag_unpacker_service_name}"

  env_vars = {
    queue_url               = "${module.bag_unpacker_queue.url}"
    destination_bucket_name = "${var.ingest_bucket_name}"
    ingest_topic_arn        = "${module.ingests_topic.arn}"
    outgoing_topic_arn      = "${module.bag_unpacker_output_topic.arn}"
    metrics_namespace       = "${local.bag_unpacker_service_name}"
    operation_name          = "unpacking"
    logstash_host           = "${local.logstash_host}"
    JAVA_OPTS               = "${local.java_opts_heap_size} ${local.java_opts_metrics_base},metricNameSpace=${local.bag_unpacker_service_name}"

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

  env_vars_length = 9

  cpu    = 2048
  memory = 4096

  min_capacity = "${var.min_capacity}"
  max_capacity = "${var.max_capacity}"

  container_image = "${local.bag_unpacker_image}"

  secret_env_vars_length = 0
}

# bag root finder

module "bag_root_finder" {
  source = "../service/worker"

  security_group_ids = [
    "${aws_security_group.interservice.id}",
    "${aws_security_group.service_egress.id}",
  ]

  cluster_name = "${aws_ecs_cluster.cluster.name}"
  cluster_id   = "${aws_ecs_cluster.cluster.id}"
  namespace_id = "${aws_service_discovery_private_dns_namespace.namespace.id}"
  subnets      = "${var.private_subnets}"
  service_name = "${local.bag_root_finder_service_name}"

  env_vars = {
    queue_url          = "${module.bag_root_finder_queue.url}"
    ingest_topic_arn   = "${module.ingests_topic.arn}"
    outgoing_topic_arn = "${module.bag_root_finder_output_topic.arn}"
    metrics_namespace  = "${local.bag_root_finder_service_name}"
    operation_name     = "detecting bag root"
    logstash_host      = "${local.logstash_host}"

    JAVA_OPTS = "-Dcom.amazonaws.sdk.enableDefaultMetrics=cloudwatchRegion=${var.aws_region},metricNameSpace=${local.bag_root_finder_service_name}"
  }

  env_vars_length = 7

  cpu    = 512
  memory = 1024

  min_capacity = "${var.min_capacity}"
  max_capacity = "${var.max_capacity}"

  container_image = "${local.bag_root_finder_image}"

  secret_env_vars_length = 0
}

# bag_verifier

module "bag_verifier_pre_replication" {
  source = "../service/worker"

  security_group_ids = [
    "${aws_security_group.interservice.id}",
    "${aws_security_group.service_egress.id}",
  ]

  cluster_name = "${aws_ecs_cluster.cluster.name}"
  cluster_id   = "${aws_ecs_cluster.cluster.id}"
  namespace_id = "${aws_service_discovery_private_dns_namespace.namespace.id}"
  subnets      = "${var.private_subnets}"
  service_name = "${local.bag_verifier_pre_repl_service_name}"

  env_vars = {
    queue_url          = "${module.bag_verifier_pre_replicate_queue.url}"
    ingest_topic_arn   = "${module.ingests_topic.arn}"
    outgoing_topic_arn = "${module.bag_verifier_pre_replicate_output_topic.arn}"
    metrics_namespace  = "${local.bag_verifier_pre_repl_service_name}"
    operation_name     = "verification (pre-replicating to archive storage)"
    logstash_host      = "${local.logstash_host}"

    JAVA_OPTS = "${local.java_opts_heap_size} ${local.java_opts_metrics_base},metricNameSpace=${local.bag_verifier_pre_repl_service_name}"
  }

  env_vars_length = 7

  cpu    = 2048
  memory = 4096

  min_capacity = "${var.min_capacity}"
  max_capacity = "${var.max_capacity}"

  container_image = "${local.bag_verifier_image}"

  secret_env_vars_length = 0
}

# bag versioner

module "bag_versioner" {
  source = "../service/worker"

  security_group_ids = [
    "${aws_security_group.interservice.id}",
    "${aws_security_group.service_egress.id}",
  ]

  cluster_name = "${aws_ecs_cluster.cluster.name}"
  cluster_id   = "${aws_ecs_cluster.cluster.id}"
  namespace_id = "${aws_service_discovery_private_dns_namespace.namespace.id}"
  subnets      = "${var.private_subnets}"
  service_name = "${local.bag_versioner_service_name}"

  env_vars = {
    queue_url          = "${module.bag_versioner_queue.url}"
    ingest_topic_arn   = "${module.ingests_topic.arn}"
    outgoing_topic_arn = "${module.bag_versioner_output_topic.arn}"
    metrics_namespace  = "${local.bag_versioner_service_name}"
    operation_name     = "assigning bag version"
    logstash_host      = "${local.logstash_host}"

    locking_table_name  = "${module.versioner_lock_table.table_name}"
    locking_table_index = "${module.versioner_lock_table.index_name}"

    versions_table_name  = "${var.versioner_versions_table_name}"
    versions_table_index = "${var.versioner_versions_table_index}"

    JAVA_OPTS = "-Dcom.amazonaws.sdk.enableDefaultMetrics=cloudwatchRegion=${var.aws_region},metricNameSpace=${local.bag_versioner_service_name}"
  }

  env_vars_length = 11

  cpu    = 512
  memory = 1024

  min_capacity = "${var.min_capacity}"
  max_capacity = "${var.max_capacity}"

  container_image = "${local.bag_versioner_image}"

  secret_env_vars_length = 0
}

module "replicator_verifier_primary" {
  source = "./replicator_verifier_pair"

  namespace = "${var.namespace}"

  replica_id           = "primary"
  replica_display_name = "primary location"
  storage_provider     = "aws-s3-ia"
  replica_type         = "primary"

  topic_names = [
    "${module.bag_versioner_output_topic.name}",
  ]

  bucket_name         = "${var.replica_primary_bucket_name}"
  primary_bucket_name = "${var.replica_primary_bucket_name}"

  ingests_read_policy_json          = "${data.aws_iam_policy_document.ingests_read.json}"
  cloudwatch_metrics_policy_json    = "${data.aws_iam_policy_document.cloudwatch_put.json}"
  replicator_lock_table_policy_json = "${module.replicator_lock_table.iam_policy}"

  security_group_ids = [
    "${aws_security_group.interservice.id}",
    "${aws_security_group.service_egress.id}",
  ]

  cluster_name = "${aws_ecs_cluster.cluster.name}"
  cluster_id   = "${aws_ecs_cluster.cluster.id}"
  namespace_id = "${aws_service_discovery_private_dns_namespace.namespace.id}"
  subnets      = "${var.private_subnets}"

  ingests_topic_arn = "${module.ingests_topic.arn}"
  logstash_host     = "${local.logstash_host}"

  replicator_lock_table_name  = "${module.replicator_lock_table.table_name}"
  replicator_lock_table_index = "${module.replicator_lock_table.index_name}"

  bag_replicator_image = "${local.bag_replicator_image}"
  bag_verifier_image   = "${local.bag_verifier_image}"

  min_capacity = "${var.min_capacity}"
  max_capacity = "${var.max_capacity}"

  dlq_alarm_arn = "${var.dlq_alarm_arn}"

  aws_region = "${var.aws_region}"
}

module "replicator_verifier_glacier" {
  source = "./replicator_verifier_pair"

  namespace = "${var.namespace}"

  replica_id           = "glacier"
  replica_display_name = "Amazon Glacier"
  storage_provider     = "aws-s3-glacier"
  replica_type         = "secondary"

  topic_names = [
    "${module.bag_versioner_output_topic.name}",
  ]

  bucket_name         = "${var.archive_bucket_name}"
  primary_bucket_name = "${var.replica_primary_bucket_name}"

  ingests_read_policy_json          = "${data.aws_iam_policy_document.ingests_read.json}"
  cloudwatch_metrics_policy_json    = "${data.aws_iam_policy_document.cloudwatch_put.json}"
  replicator_lock_table_policy_json = "${module.replicator_lock_table.iam_policy}"

  security_group_ids = [
    "${aws_security_group.interservice.id}",
    "${aws_security_group.service_egress.id}",
  ]

  cluster_name = "${aws_ecs_cluster.cluster.name}"
  cluster_id   = "${aws_ecs_cluster.cluster.id}"
  namespace_id = "${aws_service_discovery_private_dns_namespace.namespace.id}"
  subnets      = "${var.private_subnets}"

  ingests_topic_arn = "${module.ingests_topic.arn}"
  logstash_host     = "${local.logstash_host}"

  replicator_lock_table_name  = "${module.replicator_lock_table.table_name}"
  replicator_lock_table_index = "${module.replicator_lock_table.index_name}"

  bag_replicator_image = "${local.bag_replicator_image}"
  bag_verifier_image   = "${local.bag_verifier_image}"

  min_capacity = "${var.min_capacity}"
  max_capacity = "${var.max_capacity}"

  dlq_alarm_arn = "${var.dlq_alarm_arn}"

  aws_region = "${var.aws_region}"
}

# replica_aggregator

module "replica_aggregator" {
  source = "../service/worker"

  cluster_name = "${aws_ecs_cluster.cluster.name}"
  cluster_id   = "${aws_ecs_cluster.cluster.id}"
  namespace_id = "${aws_service_discovery_private_dns_namespace.namespace.id}"
  subnets      = "${var.private_subnets}"
  service_name = "${var.namespace}-replica_aggregator"

  env_vars = {
    replicas_table_name = "${var.replicas_table_name}"
    queue_url           = "${module.replica_aggregator_input_queue.url}"
    outgoing_topic_arn  = "${module.replica_aggregator_output_topic.arn}"
    ingest_topic_arn    = "${module.ingests_topic.arn}"
    metrics_namespace   = "${local.bag_register_service_name}"
    operation_name      = "Aggregating replicas"
    logstash_host       = "${local.logstash_host}"

    expected_replica_count = 2

    JAVA_OPTS = "${local.java_opts_heap_size} ${local.java_opts_metrics_base},metricNameSpace=${local.replica_aggregator_service_name}"
  }

  env_vars_length = 9

  min_capacity = "${var.min_capacity}"
  max_capacity = "${var.max_capacity}"

  container_image = "${local.replica_aggregator_image}"

  secret_env_vars_length = 0
}

# bag_register

module "bag_register" {
  source = "../service/worker"

  cluster_name = "${aws_ecs_cluster.cluster.name}"
  cluster_id   = "${aws_ecs_cluster.cluster.id}"
  namespace_id = "${aws_service_discovery_private_dns_namespace.namespace.id}"
  subnets      = "${var.private_subnets}"
  service_name = "${var.namespace}-bag_register"

  env_vars = {
    queue_url         = "${module.bag_register_input_queue.url}"
    archive_bucket    = "${var.archive_bucket_name}"
    ongoing_topic_arn = "${module.bag_register_output_topic.arn}"
    ingest_topic_arn  = "${module.ingests_topic.arn}"
    vhs_bucket_name   = "${var.vhs_archive_manifest_bucket_name}"
    vhs_table_name    = "${var.vhs_archive_manifest_table_name}"
    metrics_namespace = "${local.bag_register_service_name}"
    operation_name    = "register"
    logstash_host     = "${local.logstash_host}"

    JAVA_OPTS = "${local.java_opts_heap_size} ${local.java_opts_metrics_base},metricNameSpace=${local.bag_register_service_name}"
  }

  env_vars_length = 10

  min_capacity = "${var.min_capacity}"
  max_capacity = "${var.max_capacity}"

  container_image = "${local.bag_register_image}"

  secret_env_vars_length = 0
}

# notifier

module "notifier" {
  source = "../service/worker"

  security_group_ids = [
    "${aws_security_group.interservice.id}",
    "${aws_security_group.service_egress.id}",
  ]

  cluster_name = "${aws_ecs_cluster.cluster.name}"
  cluster_id   = "${aws_ecs_cluster.cluster.id}"
  namespace_id = "${aws_service_discovery_private_dns_namespace.namespace.id}"
  subnets      = "${var.private_subnets}"
  service_name = "${var.namespace}-notifier"

  env_vars = {
    context_url        = "https://api.wellcomecollection.org/storage/v1/context.json"
    notifier_queue_url = "${module.notifier_input_queue.url}"
    ingest_topic_arn   = "${module.ingests_topic.arn}"
    metrics_namespace  = "${local.notifier_service_name}"
    logstash_host      = "${local.logstash_host}"

    JAVA_OPTS = "-Dcom.amazonaws.sdk.enableDefaultMetrics=cloudwatchRegion=${var.aws_region},metricNameSpace=${local.notifier_service_name}"
  }

  env_vars_length = 6

  min_capacity = "${var.min_capacity}"
  max_capacity = "${var.max_capacity}"

  container_image = "${local.notifier_image}"

  secret_env_vars_length = 0
}

# ingests

module "ingests" {
  source = "../service/worker"

  cluster_name = "${aws_ecs_cluster.cluster.name}"
  cluster_id   = "${aws_ecs_cluster.cluster.id}"

  namespace_id = "${aws_service_discovery_private_dns_namespace.namespace.id}"
  subnets      = "${var.private_subnets}"
  service_name = "${var.namespace}-ingests"

  env_vars = {
    queue_url                 = "${module.ingests_input_queue.url}"
    topic_arn                 = "${module.ingests_output_topic.arn}"
    archive_ingest_table_name = "${var.ingests_table_name}"
    metrics_namespace         = "${local.ingests_service_name}"
    logstash_host             = "${local.logstash_host}"

    JAVA_OPTS = "-Dcom.amazonaws.sdk.enableDefaultMetrics=cloudwatchRegion=${var.aws_region},metricNameSpace=${local.ingests_service_name}"
  }

  env_vars_length = 6

  min_capacity = "${var.min_capacity}"
  max_capacity = "${var.max_capacity}"

  container_image = "${local.ingests_image}"

  secret_env_vars_length = 0
}

# Storage API

module "api" {
  source = "api"

  vpc_id     = "${var.vpc_id}"
  cluster_id = "${aws_ecs_cluster.cluster.id}"
  subnets    = "${var.private_subnets}"

  domain_name      = "${var.domain_name}"
  cert_domain_name = "${var.cert_domain_name}"

  namespace    = "${var.namespace}"
  namespace_id = "${aws_service_discovery_private_dns_namespace.namespace.id}"

  # Auth

  auth_scopes = [
    "${var.cognito_storage_api_identifier}/ingests",
    "${var.cognito_storage_api_identifier}/bags",
  ]
  cognito_user_pool_arn = "${var.cognito_user_pool_arn}"

  # Bags endpoint

  bags_container_image = "${local.bags_api_image}"
  bags_container_port  = "9001"
  bags_env_vars = {
    context_url       = "${var.api_url}/context.json"
    app_base_url      = "${var.api_url}/storage/v1/bags"
    vhs_bucket_name   = "${var.vhs_archive_manifest_bucket_name}"
    vhs_table_name    = "${var.vhs_archive_manifest_table_name}"
    metrics_namespace = "${local.bags_api_service_name}"
    logstash_host     = "${local.logstash_host}"

    responses_bucket_name = "${aws_s3_bucket.large_response_cache.id}"

    JAVA_OPTS = "-Dcom.amazonaws.sdk.enableDefaultMetrics=cloudwatchRegion=${var.aws_region},metricNameSpace=${local.bags_api_service_name}"
  }
  bags_env_vars_length       = 8
  bags_nginx_container_image = "${var.nginx_image}"
  bags_nginx_container_port  = "9000"

  # Ingests endpoint

  ingests_container_image = "${local.ingests_api_image}"
  ingests_container_port  = "9001"
  ingests_env_vars = {
    context_url               = "${var.api_url}/context.json"
    app_base_url              = "${var.api_url}/storage/v1/ingests"
    unpacker_topic_arn        = "${module.bag_unpacker_input_topic.arn}"
    archive_ingest_table_name = "${var.ingests_table_name}"
    metrics_namespace         = "${local.ingests_api_service_name}"
    logstash_host             = "${local.logstash_host}"

    JAVA_OPTS = "-Dcom.amazonaws.sdk.enableDefaultMetrics=cloudwatchRegion=${var.aws_region},metricNameSpace=${local.ingests_api_service_name}"
  }
  ingests_env_vars_length        = 7
  ingests_nginx_container_image  = "${var.nginx_image}"
  ingests_nginx_container_port   = "9000"
  static_content_bucket_name     = "${var.static_content_bucket_name}"
  interservice_security_group_id = "${aws_security_group.interservice.id}"
  alarm_topic_arn                = "${var.alarm_topic_arn}"
  bag_unpacker_topic_arn         = "${module.bag_unpacker_input_topic.arn}"
  desired_bags_api_count         = "${var.desired_bags_api_count}"
  desired_ingests_api_count      = "${var.desired_ingests_api_count}"
}

# Migration services

module "bagger" {
  source = "../service/worker+nvm"

  service_egress_security_group_id = "${aws_security_group.service_egress.id}"
  cluster_name                     = "${aws_ecs_cluster.cluster.name}"
  cluster_id                       = "${aws_ecs_cluster.cluster.id}"
  namespace_id                     = "${aws_service_discovery_private_dns_namespace.namespace.id}"
  subnets                          = "${var.private_subnets}"
  service_name                     = "${local.bagger_service_name}"

  env_vars = {
    METS_BUCKET_NAME         = "${var.bagger_mets_bucket_name}"
    READ_METS_FROM_FILESHARE = "${var.bagger_read_mets_from_fileshare}"
    WORKING_DIRECTORY        = "${var.bagger_working_directory}"

    DROP_BUCKET_NAME           = "${var.s3_bagger_drop_name}"
    DROP_BUCKET_NAME_METS_ONLY = "${var.s3_bagger_drop_mets_only_name}"
    DROP_BUCKET_NAME_ERRORS    = "${var.s3_bagger_errors_name}"

    CURRENT_PRESERVATION_BUCKET = "${var.bagger_current_preservation_bucket}"
    DLCS_SOURCE_BUCKET          = "${var.bagger_dlcs_source_bucket}"
    BAGGING_QUEUE               = "${module.bagger_queue.name}"
    BAGGING_COMPLETE_TOPIC_ARN  = "${module.bagging_complete_topic.arn}"

    // This value is hard coded as it is not provisioned via terraform
    DYNAMO_TABLE = "${var.bagger_ingest_table}"

    AWS_DEFAULT_REGION = "${var.aws_region}"

    # DLCS config
    DLCS_ENTRY       = "${var.bagger_dlcs_entry}"
    DLCS_CUSTOMER_ID = "${var.bagger_dlcs_customer_id}"
    DLCS_SPACE       = "${var.bagger_dlcs_space}"

    # DDS credentials
    DDS_ASSET_PREFIX = "${var.bagger_dds_asset_prefix}"

    ARCHIVE_FORMAT = "gztar"

    BAGGER_CACHE_BUCKET = "${var.s3_bagger_cache_name}"
  }

  env_vars_length = 18

  secret_env_vars = {
    DLCS_API_KEY    = "storage/bagger_dlcs_api_key"
    DLCS_API_SECRET = "storage/bagger_dlcs_api_secret"

    DDS_API_KEY    = "storage/bagger_dds_api_key"
    DDS_API_SECRET = "storage/bagger_dds_api_secret"
  }

  secret_env_vars_length = 4

  cpu    = 2600
  memory = 14000

  min_capacity       = "${min(var.desired_bagger_count, 1)}"
  desired_task_count = "${var.desired_bagger_count}"
  max_capacity       = "${var.desired_bagger_count}"

  container_image = "${local.bagger_image}"
}

module "trigger_bag_ingest" {
  source = "trigger_bag_ingest"

  name                   = "${var.namespace}-trigger-bag-ingest"
  lambda_s3_key          = "lambdas/archive/lambdas/trigger_bag_ingest.zip"
  lambda_error_alarm_arn = "${var.lambda_error_alarm_arn}"
  infra_bucket           = "${var.infra_bucket}"
  oauth_details_enc      = "${var.archive_oauth_details_enc}"
  bag_paths              = "${var.bag_paths}"
  ingest_bucket_name     = "${var.ingest_bucket_name}"

  use_encryption_key_policy = "${var.use_encryption_key_policy}"
}
