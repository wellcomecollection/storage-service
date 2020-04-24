module "bag_root_finder" {
  source = "../service_new/worker"

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

    //TODO: remove application reference, then this
    logstash_host = "localhost"
  }

  cpu    = 512
  memory = 1024

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  container_image = local.bag_root_finder_image

  use_fargate_spot = true

  service_discovery_namespace_id = local.service_discovery_namespace_id
}