module "log_router_container" {
  source    = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/firelens?ref=v2.4.1"
  namespace = var.service_name
}

module "log_router_container_secrets_permissions" {
  source    = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/secrets?ref=v2.4.1"
  secrets   = module.log_router_container.shared_secrets_logging
  role_name = module.task_definition.task_execution_role_name
}

module "task_definition" {
  source = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/task_definition?ref=v2.4.1"

  cpu    = var.cpu
  memory = var.memory

  container_definitions = concat([
    module.log_router_container.container_definition,
  ], var.container_definitions)

  task_name = var.service_name
}

module "service" {
  source = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/service?ref=v2.4.1"

  cluster_arn  = var.cluster_arn
  service_name = var.service_name

  service_discovery_namespace_id = var.service_discovery_namespace_id

  task_definition_arn = module.task_definition.arn

  subnets            = var.subnets
  security_group_ids = var.security_group_ids

  desired_task_count = var.desired_task_count
  use_fargate_spot   = var.use_fargate_spot

  target_group_arn = var.target_group_arn

  container_name = var.container_name
  container_port = var.container_port
}