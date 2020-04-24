module "ingests" {
  source = "../service_new/worker"

  container_image = local.ingests_image

  cluster_name = aws_ecs_cluster.cluster.name
  cluster_arn  = aws_ecs_cluster.cluster.arn

  subnets      = var.private_subnets
  service_name = "${var.namespace}-ingests"

  environment = {
    queue_url                        = module.ingests_input_queue.url
    callback_notifications_topic_arn = module.ingests_monitor_callback_notifications_topic.arn
    updated_ingests_topic_arn        = module.updated_ingests_topic.arn
    ingests_table_name               = var.ingests_table_name
    metrics_namespace                = local.ingests_service_name

    //TODO: remove application reference, then this
    logstash_host = "localhost"
  }

  # We always run at least one ingests monitor so messages from other apps are
  # displayed in the API immediately.
  min_capacity = max(1, var.min_capacity)
  max_capacity = var.max_capacity

  use_fargate_spot = true

  service_discovery_namespace_id = local.service_discovery_namespace_id
}