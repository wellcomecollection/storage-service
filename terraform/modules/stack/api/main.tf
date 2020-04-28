module "services" {
  source = "./services"

  namespace = var.namespace

  cluster_arn = var.cluster_arn
  subnets     = var.subnets

  vpc_id = var.vpc_id

  interservice_security_group_id = var.interservice_security_group_id

  load_balancer_arn = aws_lb.nlb.arn

  bags_container_image = var.bags_container_image
  bags_environment     = var.bags_environment
  bags_listener_port   = local.bags_listener_port

  ingests_container_image = var.ingests_container_image
  ingests_environment     = var.ingests_environment
  ingests_listener_port   = local.ingests_listener_port

  service_discovery_namespace_id = var.service_discovery_namespace_id

  use_fargate_spot_for_api = var.use_fargate_spot_for_api

  allow_ingests_publish_to_unpacker_topic_json = data.aws_iam_policy_document.allow_ingests_publish_to_unpacker_topic.json
}

