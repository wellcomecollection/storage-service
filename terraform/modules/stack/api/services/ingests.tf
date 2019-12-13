module "ingests" {
  source = "../../../service/api"

  namespace = "${var.namespace}-ingests-api"

  container_image = var.ingests_container_image
  container_port  = var.ingests_container_port

  namespace_id = var.namespace_id

  cluster_arn = var.cluster_arn

  vpc_id = var.vpc_id

  security_group_ids = [
    aws_security_group.service_egress_security_group.id,
    aws_security_group.service_lb_ingress_security_group.id,
    var.interservice_security_group_id,
  ]

  subnets = var.subnets

  nginx_container_port  = var.ingests_nginx_container_port
  nginx_container_image = var.ingests_nginx_container_image

  env_vars = var.ingests_env_vars

  lb_arn        = var.nlb_arn
  listener_port = var.ingests_listener_port

  sidecar_cpu    = 128
  sidecar_memory = 256

  app_cpu    = 896
  app_memory = 1792

  desired_task_count = var.desired_ingests_api_count
}

resource "aws_iam_role_policy" "allow_ingests_publish_to_unpacker_topic" {
  policy = var.allow_ingests_publish_to_unpacker_topic_json
  role   = module.ingests.task_role_name
}

