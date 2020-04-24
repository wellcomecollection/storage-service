module "replica_aggregator" {
  source = "../service_new/worker"

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

    //TODO: remove application reference, then this
    logstash_host = "localhost"
  }

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  container_image = local.replica_aggregator_image

  use_fargate_spot = true

  service_discovery_namespace_id = local.service_discovery_namespace_id
}