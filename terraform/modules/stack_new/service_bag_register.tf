module "bag_register" {
  source = "../service_new/worker"

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn

  subnets      = var.private_subnets
  service_name = "${var.namespace}-bag_register"

  environment = {
    queue_url         = module.bag_register_input_queue.url
    archive_bucket    = var.replica_primary_bucket_name
    ongoing_topic_arn = module.bag_register_output_topic.arn
    ingest_topic_arn  = module.ingests_topic.arn
    vhs_bucket_name   = var.vhs_manifests_bucket_name
    vhs_table_name    = var.vhs_manifests_table_name
    metrics_namespace = local.bag_register_service_name
    operation_name    = "register"
    JAVA_OPTS         = local.java_opts_heap_size

    //TODO: remove application reference, then this
    logstash_host = "localhost"
  }

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  container_image = local.bag_register_image

  use_fargate_spot = true

  service_discovery_namespace_id = local.service_discovery_namespace_id
}