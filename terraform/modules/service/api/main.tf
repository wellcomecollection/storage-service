module "log_router_container" {
  source    = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/firelens?ref=v2.3.0"
  namespace = var.service_name
}

module "log_router_container_secrets_permissions" {
  source    = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/secrets?ref=v2.3.0"
  secrets   = module.log_router_container.shared_secrets_logging
  role_name = module.task_definition.task_execution_role_name
}

module "app_container" {
  source = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/container_definition?ref=v2.3.0"
  name   = "app"

  image = var.container_image

  environment = var.environment
  secrets     = var.secrets

  log_configuration = module.log_router_container.container_log_configuration
}

module "nginx_container" {
  source = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/nginx/apigw?ref=v2.3.0"

  forward_port      = var.container_port
  log_configuration = module.log_router_container.container_log_configuration
}

module "app_container_secrets_permissions" {
  source    = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/secrets?ref=v2.3.0"
  secrets   = var.secrets
  role_name = module.task_definition.task_execution_role_name
}

module "task_definition" {
  source = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/task_definition?ref=v2.3.0"

  cpu    = var.cpu
  memory = var.memory

  container_definitions = [
    module.log_router_container.container_definition,
    module.app_container.container_definition,
    module.nginx_container.container_definition
  ]

  task_name = var.service_name
}

module "service" {
  source = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/service?ref=v2.3.0"

  cluster_arn  = var.cluster_arn
  service_name = var.service_name

  service_discovery_namespace_id = var.service_discovery_namespace_id

  task_definition_arn = module.task_definition.arn

  subnets            = var.subnets
  security_group_ids = var.security_group_ids

  desired_task_count = var.desired_task_count
  use_fargate_spot   = var.use_fargate_spot

  target_group_arn = aws_lb_target_group.tcp.arn

  container_name = "nginx"
  container_port = module.nginx_container.container_port
}