module "base" {
  source = "../base"

  service_name = var.service_name
  cluster_arn  = var.cluster_arn

  cpu    = var.cpu
  memory = var.memory

  container_definitions = [
    module.nginx_container.container_definition,
    module.app_container.container_definition,
    module.tracker_container.container_definition,
  ]

  # We need to run at least three API tasks (three = number of Availability Zones).
  # If you run less than three, the load balancer has to route requests between
  # AZs, which adds significant latency.
  #
  #     Your load balancer is most effective when you ensure that each
  #     enabled Availability Zone has at least one registered target
  #
  # See: https://docs.aws.amazon.com/elasticloadbalancing/latest/userguide/how-elastic-load-balancing-works.html
  desired_task_count = max(3, var.desired_task_count)

  security_group_ids = var.security_group_ids

  service_discovery_namespace_id = var.service_discovery_namespace_id

  target_group_arn = aws_lb_target_group.tcp.arn

  container_name = module.nginx_container.container_name
  container_port = module.nginx_container.container_port

  subnets = var.subnets

  deployment_service_name = var.deployment_service_name
  deployment_service_env  = var.deployment_service_env

  use_fargate_spot = var.use_fargate_spot
}

module "nginx_container" {
  source = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/nginx/apigw?ref=v3.2.2"

  forward_port      = var.container_port
  log_configuration = module.base.log_configuration
}

module "app_container" {
  source = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/container_definition?ref=v3.2.2"
  name   = "app"

  image = var.api_container_image

  environment = var.api_environment

  log_configuration = module.base.log_configuration
}

module "tracker_container" {
  source = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/container_definition?ref=v3.2.2"
  name   = "tracker"

  image = var.tracker_container_image

  port_mappings = [
    {
      containerPort = 8080
      hostPort      = 8080
      protocol      = "tcp"
    }
  ]

  environment = var.tracker_environment

  log_configuration = module.base.log_configuration
}
