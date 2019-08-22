# bag_replicator

module "bag_replicator" {
  source = "../../modules/service/worker"

  security_group_ids = [
    "${var.security_group_ids}"
  ]

  cluster_name = "${var.cluster_name}"
  cluster_id   = "${var.cluster_id}"
  namespace_id = "${var.namespace_id}"
  subnets      = "${var.subnets}"
  service_name = "${local.bag_replicator_service_name}"

  env_vars = {
    queue_url               = "${module.bag_replicator_input_queue.url}"
    destination_bucket_name = "${var.bucket_name}"
    ingest_topic_arn        = "${var.ingests_topic_arn}"
    outgoing_topic_arn      = "${module.bag_replicator_output_topic.arn}"
    metrics_namespace       = "${local.bag_replicator_service_name}"
    operation_name          = "replicating to ${var.replica_display_name}"
    logstash_host           = "${var.logstash_host}"

    locking_table_name  = "${var.replicator_lock_table_name}"
    locking_table_index = "${var.replicator_lock_table_index}"

    storage_provider = "${var.storage_provider}"
    replica_type     = "${var.replica_type}"

    JAVA_OPTS = "-Dcom.amazonaws.sdk.enableDefaultMetrics=cloudwatchRegion=${var.aws_region},metricNameSpace=${local.bag_replicator_service_name}"
  }

  env_vars_length = 12

  cpu    = 1024
  memory = 2048

  min_capacity = 1
  max_capacity = 10

  container_image = "${var.bag_replicator_image}"

  secret_env_vars_length = 0
}

# bag_verifier

module "bag_verifier" {
  source = "../../modules/service/worker"

  security_group_ids = [
    "${var.security_group_ids}"
  ]

  cluster_name = "${var.cluster_name}"
  cluster_id   = "${var.cluster_id}"
  namespace_id = "${var.namespace_id}"
  subnets      = "${var.subnets}"
  service_name = "${local.bag_verifier_service_name}"

  env_vars = {
    queue_url          = "${module.bag_verifier_queue.url}"
    ingest_topic_arn   = "${var.ingests_topic_arn}"
    outgoing_topic_arn = "${module.bag_verifier_output_topic.arn}"
    metrics_namespace  = "${local.bag_verifier_service_name}"
    operation_name     = "verification (${var.replica_display_name})"
    logstash_host      = "${var.logstash_host}"

    JAVA_OPTS = "-Dcom.amazonaws.sdk.enableDefaultMetrics=cloudwatchRegion=${var.aws_region},metricNameSpace=${local.bag_verifier_service_name}"
  }

  env_vars_length = 7

  cpu    = 1024
  memory = 2048

  min_capacity = 1
  max_capacity = 10

  container_image = "${var.bag_verifier_image}"

  secret_env_vars_length = 0
}
