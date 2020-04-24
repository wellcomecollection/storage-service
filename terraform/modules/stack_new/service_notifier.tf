module "notifier" {
  source = "../service_new/worker"

  container_image = local.notifier_image

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

    //TODO: remove application reference, then this
    logstash_host = "localhost"
  }

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  use_fargate_spot = true

  service_discovery_namespace_id = local.service_discovery_namespace_id
}