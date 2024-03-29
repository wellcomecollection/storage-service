locals {
  base_url = var.api_url != null ? "${var.api_url}/storage/v1" : module.api.invoke_url
}

module "working_storage" {
  source = "./working_storage"

  namespace          = var.namespace
  bucket_name_prefix = var.working_storage_bucket_prefix

  azure_replicator_enabled = local.azure_replicator_count > 0 ? true : false
}

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
    app_base_url         = "${local.base_url}/ingests"
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

  logging_container = var.logging_container
  nginx_container   = var.nginx_container
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

  secrets = merge(var.indexer_host_secrets, var.ingests_indexer_secrets)

  security_group_ids = [
    aws_security_group.service_egress.id,
    aws_security_group.interservice.id
  ]

  min_capacity = 0
  max_capacity = var.max_capacity

  use_fargate_spot = true

  service_discovery_namespace_id = local.service_discovery_namespace_id

  logging_container = var.logging_container
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

  secrets = merge(var.indexer_host_secrets, var.bag_indexer_secrets)

  security_group_ids = [
    aws_security_group.service_egress.id,
    aws_security_group.interservice.id
  ]

  min_capacity = 0
  max_capacity = var.max_capacity

  use_fargate_spot = true

  service_discovery_namespace_id = local.service_discovery_namespace_id

  logging_container = var.logging_container
}

# file finder

module "file_finder" {
  source = "../service/worker"

  container_image = local.image_ids["file_finder"]

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn

  subnets      = var.private_subnets
  service_name = "${var.namespace}-file-finder"

  environment = {
    queue_url                    = module.file_finder_input_queue.url
    metrics_namespace            = local.file_finder_service_name
    bags_tracker_host            = "http://${module.bags_api.name}.${var.namespace}:8080"
    file_finder_output_topic_arn = module.file_finder_output_topic.arn
  }

  security_group_ids = [
    aws_security_group.service_egress.id,
    aws_security_group.interservice.id
  ]

  min_capacity = 0
  max_capacity = var.max_capacity

  use_fargate_spot = true

  service_discovery_namespace_id = local.service_discovery_namespace_id

  logging_container = var.logging_container
}

# file indexer

module "file_indexer" {
  source = "../service/worker"

  container_image = local.image_ids["file_indexer"]

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn

  subnets      = var.private_subnets
  service_name = "${var.namespace}-file-indexer"

  environment = {
    queue_url           = module.file_indexer_input_queue.url
    metrics_namespace   = local.file_indexer_service_name
    es_files_index_name = var.es_files_index_name
  }

  secrets = merge(var.indexer_host_secrets, var.file_indexer_secrets)

  security_group_ids = [
    aws_security_group.service_egress.id,
    aws_security_group.interservice.id
  ]

  min_capacity = 0
  max_capacity = var.max_capacity

  use_fargate_spot = true

  service_discovery_namespace_id = local.service_discovery_namespace_id

  logging_container = var.logging_container
}

# bags_api

module "bags_api" {
  source = "../service/bags"

  service_name = "${var.namespace}-bags-api"

  api_container_image     = local.image_ids["bags_api"]
  tracker_container_image = local.image_ids["bag_tracker"]

  api_environment = {
    app_base_url          = "${local.base_url}/bags"
    vhs_bucket_name       = var.vhs_manifests_bucket_name
    vhs_table_name        = var.vhs_manifests_table_name
    metrics_namespace     = local.bags_api_service_name
    responses_bucket_name = module.working_storage.large_response_cache_bucket_name
    bags_tracker_host     = "http://localhost:8080"
  }

  tracker_environment = {
    vhs_bucket_name = var.vhs_manifests_bucket_name
    vhs_table_name  = var.vhs_manifests_table_name
  }

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

  logging_container = var.logging_container
  nginx_container   = var.nginx_container
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
    destination_bucket_name = module.working_storage.unpacked_bags_bucket_name
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

  logging_container = var.logging_container
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

  logging_container = var.logging_container
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

  logging_container = var.logging_container
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
    bag_verifier_mode  = "standalone"

    primary_storage_bucket_name = var.replica_primary_bucket_name
  }

  cpu    = 2048
  memory = 4096

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  container_image = local.image_ids["bag_verifier"]

  service_discovery_namespace_id = local.service_discovery_namespace_id

  logging_container = var.logging_container
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
    locking_table_name   = module.working_storage.versioner_lock_table_name
    locking_table_index  = module.working_storage.versioner_lock_index_name
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

  logging_container = var.logging_container
}

module "replicator_verifier_primary" {
  source = "./replifier"

  namespace = var.namespace

  replica_id           = "primary"
  replica_display_name = "primary location"
  storage_provider     = "amazon-s3"
  replica_type         = "primary"
  bag_verifier_mode    = "replica-s3"

  topic_arns = [
    module.bag_versioner_output_topic.arn,
  ]

  destination_namespace = var.replica_primary_bucket_name
  primary_bucket_name   = var.replica_primary_bucket_name
  unpacker_bucket_name  = module.working_storage.unpacked_bags_bucket_name

  ingests_read_policy_json          = data.aws_iam_policy_document.unpacked_bags_bucket_readonly.json
  replicator_lock_table_policy_json = module.working_storage.replicator_lock_iam_policy

  security_group_ids = [
    aws_security_group.interservice.id,
    aws_security_group.service_egress.id,
  ]

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn
  subnets      = var.private_subnets

  ingests_topic_arn = module.ingests_topic.arn

  replicator_lock_table_name  = module.working_storage.replicator_lock_table_name
  replicator_lock_table_index = module.working_storage.replicator_lock_index_name

  bag_replicator_image = local.image_ids["bag_replicator"]
  bag_verifier_image   = local.image_ids["bag_verifier"]

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  dlq_alarm_arn = var.dlq_alarm_arn

  service_discovery_namespace_id = local.service_discovery_namespace_id

  logging_container = var.logging_container
}

module "replicator_verifier_glacier" {
  source = "./replifier"

  namespace = var.namespace

  replica_id           = "glacier"
  replica_display_name = "Amazon Glacier"
  storage_provider     = "amazon-s3"
  replica_type         = "secondary"
  bag_verifier_mode    = "replica-s3"

  topic_arns = [
    module.bag_versioner_output_topic.arn,
  ]

  destination_namespace = var.replica_glacier_bucket_name
  primary_bucket_name   = var.replica_primary_bucket_name
  unpacker_bucket_name  = module.working_storage.unpacked_bags_bucket_name

  ingests_read_policy_json          = data.aws_iam_policy_document.unpacked_bags_bucket_readonly.json
  replicator_lock_table_policy_json = module.working_storage.replicator_lock_iam_policy

  security_group_ids = [
    aws_security_group.interservice.id,
    aws_security_group.service_egress.id,
  ]

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn
  subnets      = var.private_subnets

  ingests_topic_arn = module.ingests_topic.arn

  replicator_lock_table_name  = module.working_storage.replicator_lock_table_name
  replicator_lock_table_index = module.working_storage.replicator_lock_index_name

  bag_replicator_image = local.image_ids["bag_replicator"]
  bag_verifier_image   = local.image_ids["bag_verifier"]

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  dlq_alarm_arn = var.dlq_alarm_arn

  service_discovery_namespace_id = local.service_discovery_namespace_id

  logging_container = var.logging_container
}

locals {
  replicate_to_azure = var.azure_container_name != null

  azure_replicator_count = local.replicate_to_azure ? 1 : 0

  # 1 replica in S3 (primary)
  # 1 replica in S3 (Glacier)
  # + 1 replica in Azure (maybe)
  expected_replica_count = local.replicate_to_azure ? 3 : 2
}

module "replicator_verifier_azure" {
  source = "./replifier"

  count = local.azure_replicator_count

  namespace = var.namespace

  replica_id           = "azure"
  replica_display_name = "Azure"
  storage_provider     = "azure-blob-storage"
  replica_type         = "secondary"
  bag_verifier_mode    = "replica-azure"

  topic_arns = [
    module.bag_versioner_output_topic.arn
  ]

  verifier_environment = {
    azure_verifier_cache_table_name = module.working_storage.azure_verifier_cache_table_name[0]
  }

  verifier_secrets = {
    azure_endpoint = "${var.azure_ssm_parameter_base}/read_only_sas_url"
  }

  replicator_secrets = {
    azure_endpoint = "${var.azure_ssm_parameter_base}/read_write_sas_url"
  }

  destination_namespace = var.azure_container_name
  primary_bucket_name   = var.replica_primary_bucket_name
  unpacker_bucket_name  = module.working_storage.unpacked_bags_bucket_name

  ingests_read_policy_json          = data.aws_iam_policy_document.unpacked_bags_bucket_readonly.json
  replicator_lock_table_policy_json = module.working_storage.replicator_lock_iam_policy

  # Reducing the max capacity is somewhat speculative: we've seen very
  # intermittent issues that we suspect are caused by saturating the S3–Azure
  # connection, and so we want to avoid hitting those limits.
  #
  # We have plans for fixing this properly, but until we can do that we
  # just cap the capacity in the hope we don't hit the same issues.
  #
  # See https://github.com/wellcomecollection/storage-service/issues/993
  min_capacity = var.min_capacity
  max_capacity = min(var.max_capacity, 8)

  security_group_ids = [
    aws_security_group.interservice.id,
    aws_security_group.service_egress.id,
  ]

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn
  subnets      = var.private_subnets

  ingests_topic_arn = module.ingests_topic.arn

  replicator_lock_table_name  = module.working_storage.replicator_lock_table_name
  replicator_lock_table_index = module.working_storage.replicator_lock_index_name

  bag_replicator_image = local.image_ids["bag_replicator"]
  bag_verifier_image   = local.image_ids["bag_verifier"]

  dlq_alarm_arn = var.dlq_alarm_arn

  service_discovery_namespace_id = local.service_discovery_namespace_id

  logging_container = var.logging_container
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
    metrics_namespace      = local.replica_aggregator_service_name
    operation_name         = "Aggregating replicas"
    expected_replica_count = local.expected_replica_count
    JAVA_OPTS              = local.java_opts_heap_size
  }

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  container_image = local.image_ids["replica_aggregator"]

  use_fargate_spot = true

  service_discovery_namespace_id = local.service_discovery_namespace_id

  logging_container = var.logging_container
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
    ingest_topic_arn        = module.ingests_topic.arn
    registrations_topic_arn = module.registered_bag_notifications_topic.arn
    metrics_namespace       = local.bag_register_service_name
    operation_name          = "register"
    bags_tracker_host       = "http://${module.bags_api.name}.${var.namespace}:8080"
    JAVA_OPTS               = local.java_opts_heap_size
  }

  cpu    = 1024
  memory = 2048

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  container_image = local.image_ids["bag_register"]

  security_group_ids = [
    aws_security_group.interservice.id,
    aws_security_group.service_egress.id,
  ]

  use_fargate_spot = true

  service_discovery_namespace_id = local.service_discovery_namespace_id

  logging_container = var.logging_container
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
    notifier_queue_url = module.notifier_input_queue.url
    ingest_topic_arn   = module.ingests_topic.arn
    metrics_namespace  = local.notifier_service_name
  }

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  container_image = local.image_ids["notifier"]

  use_fargate_spot = true

  service_discovery_namespace_id = local.service_discovery_namespace_id

  logging_container = var.logging_container
}

# storage API

module "api" {
  source = "./api"

  subnets = var.private_subnets

  namespace = var.namespace

  cognito_user_pool_arn = var.cognito_user_pool_arn

  alarm_topic_arn = var.api_gateway_alarm_topic_arn

  auth_scopes = [
    "${var.cognito_storage_api_identifier}/ingests",
    "${var.cognito_storage_api_identifier}/bags",
  ]
}
