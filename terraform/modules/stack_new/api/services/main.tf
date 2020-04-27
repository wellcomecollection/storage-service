module "bags" {
  source = "../../../service_new/api"

  service_name = "${var.namespace}-bags-api"

  container_image = var.bags_container_image

  environment = var.bags_environment

  load_balancer_arn           = var.load_balancer_arn
  load_balancer_listener_port = var.bags_listener_port

  security_group_ids = [
    aws_security_group.service_egress_security_group.id,
    aws_security_group.service_lb_ingress_security_group.id,
    var.interservice_security_group_id,
  ]

  service_discovery_namespace_id = var.service_discovery_namespace_id

  cluster_arn = var.cluster_arn
  subnets     = var.subnets
  vpc_id      = var.vpc_id

  use_fargate_spot = var.use_fargate_spot_for_api
}

module "ingests" {
  source = "../../../service_new/api"

  service_name = "${var.namespace}-ingests-api"

  container_image = var.ingests_container_image

  environment = var.ingests_environment

  load_balancer_arn           = var.load_balancer_arn
  load_balancer_listener_port = var.ingests_listener_port

  security_group_ids = [
    aws_security_group.service_egress_security_group.id,
    aws_security_group.service_lb_ingress_security_group.id,
    var.interservice_security_group_id,
  ]

  service_discovery_namespace_id = var.service_discovery_namespace_id

  cluster_arn = var.cluster_arn
  subnets     = var.subnets
  vpc_id      = var.vpc_id

  use_fargate_spot = var.use_fargate_spot_for_api
}

resource "aws_iam_role_policy" "allow_ingests_publish_to_unpacker_topic" {
  policy = var.allow_ingests_publish_to_unpacker_topic_json
  role   = module.ingests.task_role_name
}

