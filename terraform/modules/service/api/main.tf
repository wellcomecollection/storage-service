module "base" {
  source = "../base"

  service_name = var.service_name
  cluster_arn  = var.cluster_arn

  container_definitions = [
    module.nginx_container.container_definition,
    module.app_container.container_definition
  ]

  desired_task_count = var.desired_task_count
  security_group_ids = var.security_group_ids

  service_discovery_namespace_id = var.service_discovery_namespace_id

  target_group_arn = aws_lb_target_group.tcp.arn

  container_name = module.nginx_container.container_name
  container_port = module.nginx_container.container_port

  subnets = var.subnets
}

module "nginx_container" {
  source = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/nginx/apigw?ref=v2.4.1"

  forward_port      = var.container_port
  log_configuration = module.base.log_configuration
}

module "app_container" {
  source = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/container_definition?ref=v2.4.1"
  name   = "app"

  image = var.container_image

  environment = var.environment
  secrets     = var.secrets

  log_configuration = module.base.log_configuration
}

module "app_container_secrets_permissions" {
  source    = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/secrets?ref=v2.4.1"
  secrets   = var.secrets
  role_name = module.base.task_execution_role_name
}