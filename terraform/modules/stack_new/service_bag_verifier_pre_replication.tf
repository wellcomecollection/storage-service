module "bag_verifier_pre_replication" {
  source = "../service_new/worker"

  container_image = local.bag_verifier_image

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

    //TODO: remove application reference, then this
    logstash_host = "localhost"
  }

  cpu    = 2048
  memory = 4096

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  service_discovery_namespace_id = local.service_discovery_namespace_id
}
