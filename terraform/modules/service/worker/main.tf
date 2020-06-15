module "base" {
  source = "../base"

  service_name = var.service_name
  cluster_arn  = var.cluster_arn

  cpu    = var.cpu
  memory = var.memory

  container_definitions = [
    module.app_container.container_definition
  ]

  desired_task_count = var.desired_task_count
  security_group_ids = var.security_group_ids

  service_discovery_namespace_id = var.service_discovery_namespace_id

  subnets = var.subnets

  use_fargate_spot = var.use_fargate_spot
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

module "scaling" {
  source = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/autoscaling?ref=v2.4.1"

  name = var.service_name

  cluster_name = var.cluster_name
  service_name = var.service_name

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity
}
