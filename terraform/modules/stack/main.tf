# Ingest service

module "ingest_service" {
  source = "../service/ingest"

  service_name = "${var.namespace}-ingests-service"

  security_group_ids = [
    aws_security_group.interservice.id,
    aws_security_group.service_egress.id
  ]

  cluster_arn = aws_ecs_cluster.cluster.arn

  external_api_container_image = local.image_ids["ingests_api"]
  internal_api_container_image = local.image_ids["ingests_tracker"]
  worker_container_image       = local.image_ids["ingests_worker"]

  external_api_environment = {
    context_url          = "${var.api_url}/context.json"
    app_base_url         = "${var.api_url}/storage/v1/ingests"
    unpacker_topic_arn   = module.bag_unpacker_input_topic.arn
    metrics_namespace    = local.ingests_api_service_name
    ingests_tracker_host = "http://localhost:8080"
  }

  worker_environment = {
    queue_url            = module.ingests_input_queue.url
    metrics_namespace    = local.ingests_service_name
    ingests_tracker_host = "http://localhost:8080"
  }

  internal_api_environment = {
    ingests_table_name               = var.ingests_table_name
    callback_notifications_topic_arn = module.ingests_monitor_callback_notifications_topic.arn
    updated_ingests_topic_arn        = module.updated_ingests_topic.arn
  }

  load_balancer_arn           = module.api.loadbalancer_arn
  load_balancer_listener_port = local.ingests_listener_port

  service_discovery_namespace_id = local.service_discovery_namespace_id

  subnets = var.private_subnets
  vpc_id  = var.vpc_id
}

module "ingests_indexer" {
  source = "../service/worker"

  container_image = local.image_ids["ingests_indexer"]

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn

  subnets      = var.private_subnets
  service_name = "${var.namespace}-ingests-indexer"

  environment = {
    queue_url         = module.updated_ingests_queue.url
    metrics_namespace = local.ingests_indexer_service_name

    es_ingests_index_name = var.es_ingests_index_name
  }

  secrets = var.ingests_indexer_secrets

  security_group_ids = [
    aws_security_group.service_egress.id,
    aws_security_group.interservice.id
  ]

  # We run the indexer all the time to updates appear in the reporting cluster
  # almost as soon as they're available in the API, rather than waiting for the
  # indexer to spin up/down every time.
  min_capacity = 1
  max_capacity = var.max_capacity

  use_fargate_spot = true

  service_discovery_namespace_id = local.service_discovery_namespace_id
}

# bag_indexer

module "bag_indexer" {
  source = "../service/worker"

  container_image = local.image_ids["bag_indexer"]

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn

  subnets      = var.private_subnets
  service_name = "${var.namespace}-bag-indexer"

  environment = {
    queue_url          = module.bag_indexer_input_queue.url
    metrics_namespace  = local.bag_indexer_service_name
    bags_tracker_host  = "http://${module.bags_api.name}.${var.namespace}:8080"
    es_bags_index_name = var.es_bags_index_name
  }

  secrets = var.bag_indexer_secrets

  security_group_ids = [
    aws_security_group.service_egress.id,
    aws_security_group.interservice.id
  ]

  min_capacity = 0
  max_capacity = var.max_capacity

  use_fargate_spot = true

  service_discovery_namespace_id = local.service_discovery_namespace_id
}

# bags_api

module "bags_api" {
  source = "../service/bags"

  service_name = "${var.namespace}-bags-api"

  api_container_image     = local.image_ids["bags_api"]
  tracker_container_image = local.image_ids["bag_tracker"]

  api_environment = {
    context_url           = "${var.api_url}/context.json"
    app_base_url          = "${var.api_url}/storage/v1/bags"
    vhs_bucket_name       = var.vhs_manifests_bucket_name
    vhs_table_name        = var.vhs_manifests_table_name
    metrics_namespace     = local.bags_api_service_name
    responses_bucket_name = aws_s3_bucket.large_response_cache.id
    bags_tracker_host     = "http://localhost:8080"
  }

  tracker_environment = {
    vhs_bucket_name = var.vhs_manifests_bucket_name
    vhs_table_name  = var.vhs_manifests_table_name
  }

  cpu    = 1024
  memory = 2048

  load_balancer_arn           = module.api.loadbalancer_arn
  load_balancer_listener_port = local.bags_listener_port

  security_group_ids = [
    aws_security_group.service_egress.id,
    aws_security_group.interservice.id
  ]

  service_discovery_namespace_id = local.service_discovery_namespace_id

  cluster_arn = aws_ecs_cluster.cluster.arn

  use_fargate_spot = var.use_fargate_spot_for_api

  subnets = var.private_subnets
  vpc_id  = var.vpc_id
}


# bag_unpacker

module "bag_unpacker" {
  source = "../service/worker"

  security_group_ids = [
    aws_security_group.interservice.id,
    aws_security_group.service_egress.id,
  ]

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn

  subnets      = var.private_subnets
  service_name = local.bag_unpacker_service_name

  environment = {
    queue_url               = module.bag_unpacker_queue.url
    destination_bucket_name = aws_s3_bucket.unpacked_bags.bucket
    ingest_topic_arn        = module.ingests_topic.arn
    outgoing_topic_arn      = module.bag_unpacker_output_topic.arn
    metrics_namespace       = local.bag_unpacker_service_name
    operation_name          = "unpacking"
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

  container_image = local.image_ids["bag_unpacker"]

  service_discovery_namespace_id = local.service_discovery_namespace_id
}

# bag root finder

module "bag_root_finder" {
  source = "../service/worker"

  security_group_ids = [
    aws_security_group.interservice.id,
    aws_security_group.service_egress.id,
  ]

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn

  subnets      = var.private_subnets
  service_name = local.bag_root_finder_service_name

  environment = {
    queue_url          = module.bag_root_finder_queue.url
    ingest_topic_arn   = module.ingests_topic.arn
    outgoing_topic_arn = module.bag_root_finder_output_topic.arn
    metrics_namespace  = local.bag_root_finder_service_name
    operation_name     = "detecting bag root"
  }

  cpu    = 512
  memory = 1024

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  container_image = local.image_ids["bag_root_finder"]

  use_fargate_spot = true

  service_discovery_namespace_id = local.service_discovery_namespace_id
}

# bag tagger

module "bag_tagger" {
  source = "../service/worker"

  service_name    = local.bag_tagger_service_name
  container_image = local.image_ids["bag_tagger"]

  environment = {
    queue_url         = module.bag_tagger_input_queue.url
    metrics_namespace = local.bag_tagger_service_name
    bags_tracker_host = "http://${module.bags_api.name}.${var.namespace}:8080"
  }

  cpu    = 512
  memory = 1024

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn

  security_group_ids = [
    aws_security_group.interservice.id,
    aws_security_group.service_egress.id,
  ]

  subnets = var.private_subnets

  use_fargate_spot = true

  service_discovery_namespace_id = local.service_discovery_namespace_id
}

# bag retagger - for adding checksum tags

module "bag_retagger" {
  source = "../service/worker"

  service_name    = "${var.namespace}-bag-retagger-temp2"
  container_image = "975596993436.dkr.ecr.eu-west-1.amazonaws.com/uk.ac.wellcome/bag_tagger:retagger"

  environment = {
    queue_url         = module.bag_retagger_input_queue.url
    metrics_namespace = "${var.namespace}-bag-retagger"
    bags_tracker_host = "http://${module.bags_api.name}.${var.namespace}:8080"
  }

  cpu    = 512
  memory = 1024

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn

  security_group_ids = [
    aws_security_group.interservice.id,
    aws_security_group.service_egress.id,
  ]

  subnets = var.private_subnets

  use_fargate_spot = true

  service_discovery_namespace_id = local.service_discovery_namespace_id
}

# bag_verifier

module "bag_verifier_pre_replication" {
  source = "../service/worker"

  security_group_ids = [
    aws_security_group.interservice.id,
    aws_security_group.service_egress.id,
  ]

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn

  subnets      = var.private_subnets
  service_name = local.bag_verifier_pre_repl_service_name

  environment = {
    queue_url          = module.bag_verifier_pre_replicate_queue.url
    ingest_topic_arn   = module.ingests_topic.arn
    outgoing_topic_arn = module.bag_verifier_pre_replicate_output_topic.arn
    metrics_namespace  = local.bag_verifier_pre_repl_service_name
    operation_name     = "verification (pre-replicating to archive storage)"
    JAVA_OPTS          = local.java_opts_heap_size

    primary_storage_bucket_name = var.replica_primary_bucket_name
  }

  cpu    = 2048
  memory = 4096

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  container_image = local.image_ids["bag_verifier"]

  service_discovery_namespace_id = local.service_discovery_namespace_id
}

# bag versioner

module "bag_versioner" {
  source = "../service/worker"

  security_group_ids = [
    aws_security_group.interservice.id,
    aws_security_group.service_egress.id,
  ]

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn

  subnets      = var.private_subnets
  service_name = local.bag_versioner_service_name

  environment = {
    queue_url            = module.bag_versioner_queue.url
    ingest_topic_arn     = module.ingests_topic.arn
    outgoing_topic_arn   = module.bag_versioner_output_topic.arn
    metrics_namespace    = local.bag_versioner_service_name
    operation_name       = "assigning bag version"
    locking_table_name   = module.versioner_lock_table.table_name
    locking_table_index  = module.versioner_lock_table.index_name
    versions_table_name  = var.versioner_versions_table_name
    versions_table_index = var.versioner_versions_table_index
  }

  cpu    = 512
  memory = 1024

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  container_image = local.image_ids["bag_versioner"]

  use_fargate_spot = true

  service_discovery_namespace_id = local.service_discovery_namespace_id
}

module "replicator_verifier_primary" {
  source = "./replifier"

  namespace = var.namespace

  replica_id           = "primary"
  replica_display_name = "primary location"
  storage_provider     = "amazon-s3"
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

  bag_replicator_image = local.image_ids["bag_replicator"]
  bag_verifier_image   = local.image_ids["bag_verifier"]

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  dlq_alarm_arn = var.dlq_alarm_arn

  aws_region = var.aws_region

  service_discovery_namespace_id = local.service_discovery_namespace_id
}

module "replicator_verifier_glacier" {
  source = "./replifier"

  namespace = var.namespace

  replica_id           = "glacier"
  replica_display_name = "Amazon Glacier"
  storage_provider     = "amazon-s3"
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

  bag_replicator_image = local.image_ids["bag_replicator"]
  bag_verifier_image   = local.image_ids["bag_verifier"]

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  dlq_alarm_arn = var.dlq_alarm_arn

  aws_region = var.aws_region

  service_discovery_namespace_id = local.service_discovery_namespace_id
}

# replica_aggregator

module "replica_aggregator" {
  source = "../service/worker"

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn

  subnets      = var.private_subnets
  service_name = "${var.namespace}-replica_aggregator"

  environment = {
    replicas_table_name    = var.replicas_table_name
    queue_url              = module.replica_aggregator_input_queue.url
    outgoing_topic_arn     = module.replica_aggregator_output_topic.arn
    ingest_topic_arn       = module.ingests_topic.arn
    metrics_namespace      = local.bag_register_service_name
    operation_name         = "Aggregating replicas"
    expected_replica_count = 2
    JAVA_OPTS              = local.java_opts_heap_size
  }

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  container_image = local.image_ids["replica_aggregator"]

  use_fargate_spot = true

  service_discovery_namespace_id = local.service_discovery_namespace_id
}

# bag_register

module "bag_register" {
  source = "../service/worker"

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn

  subnets      = var.private_subnets
  service_name = "${var.namespace}-bag_register"

  environment = {
    queue_url               = module.bag_register_input_queue.url
    archive_bucket          = var.replica_primary_bucket_name
    ongoing_topic_arn       = module.bag_register_output_topic.arn
    ingest_topic_arn        = module.ingests_topic.arn
    registrations_topic_arn = module.registered_bag_notifications_topic.arn
    metrics_namespace       = local.bag_register_service_name
    operation_name          = "register"
    bags_tracker_host       = "http://${module.bags_api.name}.${var.namespace}:8080"
    JAVA_OPTS               = local.java_opts_heap_size
  }

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  container_image = local.image_ids["bag_register"]

  security_group_ids = [
    aws_security_group.interservice.id,
    aws_security_group.service_egress.id,
  ]

  use_fargate_spot = true

  service_discovery_namespace_id = local.service_discovery_namespace_id
}

# notifier

module "notifier" {
  source = "../service/worker"

  security_group_ids = [
    aws_security_group.interservice.id,
    aws_security_group.service_egress.id,
  ]

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn

  subnets      = var.private_subnets
  service_name = "${var.namespace}-notifier"

  environment = {
    context_url        = "https://api.wellcomecollection.org/storage/v1/context.json"
    notifier_queue_url = module.notifier_input_queue.url
    ingest_topic_arn   = module.ingests_topic.arn
    metrics_namespace  = local.notifier_service_name
  }

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  container_image = local.image_ids["notifier"]

  use_fargate_spot = true

  service_discovery_namespace_id = local.service_discovery_namespace_id
}

# storage API

module "api" {
  source = "./api"

  subnets = var.private_subnets

  domain_name      = var.domain_name
  cert_domain_name = var.cert_domain_name

  namespace = var.namespace

  cognito_user_pool_arn = var.cognito_user_pool_arn

  alarm_topic_arn = var.alarm_topic_arn

  auth_scopes = [
    "${var.cognito_storage_api_identifier}/ingests",
    "${var.cognito_storage_api_identifier}/bags",
  ]

  static_content_bucket_name = var.static_content_bucket_name
}
