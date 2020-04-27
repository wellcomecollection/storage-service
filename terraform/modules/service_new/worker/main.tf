module "log_router_container" {
  source    = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/firelens?ref=c4dad2c"
  namespace = var.service_name
}

module "log_router_container_secrets_permissions" {
  source              = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/secrets?ref=c4dad2c"
  secrets             = module.log_router_container.shared_secrets_logging
  execution_role_name = module.task_definition.task_execution_role_name
}

module "app_container" {
  source = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/container_definition?ref=c4dad2c"
  name   = "app"

  image = var.container_image

  environment = var.environment
  secrets     = var.secrets

  log_configuration = module.log_router_container.container_log_configuration
}

module "app_container_secrets_permissions" {
  source              = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/secrets?ref=c4dad2c"
  secrets             = var.secrets
  execution_role_name = module.task_definition.task_execution_role_name
}

module "task_definition" {
  source = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/task_definition?ref=c4dad2c"

  cpu    = var.cpu
  memory = var.memory

  container_definitions = [
    module.log_router_container.container_definition,
    module.app_container.container_definition
  ]

  task_name = var.service_name
}

module "service" {
  source = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/service?ref=c4dad2c"

  cluster_arn  = var.cluster_arn
  service_name = var.service_name

  service_discovery_namespace_id = var.service_discovery_namespace_id

  task_definition_arn = module.task_definition.arn

  subnets            = var.subnets
  security_group_ids = var.security_group_ids

  desired_task_count = var.desired_task_count
  use_fargate_spot   = var.use_fargate_spot
}

module "scaling" {
  source = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/autoscaling?ref=c4dad2c"

  name = var.service_name

  cluster_name = var.cluster_name
  service_name = var.service_name

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity
}