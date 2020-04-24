module "bag_versioner" {
  source = "../service_new/worker"

  container_image = local.bag_versioner_image

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

    //TODO: remove application reference, then this
    logstash_host = "localhost"
  }

  cpu    = 512
  memory = 1024

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  use_fargate_spot = true

  service_discovery_namespace_id = local.service_discovery_namespace_id
}
