module "base" {
  source = "../../service/base"

  service_name = var.service_name
  cluster_arn  = var.cluster_arn

  cpu    = 1024
  memory = 2048

  container_definitions = [
    module.nginx_container.container_definition,
    module.external_api_container.container_definition,
    module.worker_container.container_definition,
    module.internal_api_container.container_definition
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

  # We don't use Fargate Spot for the ingests service, because it includes the
  # external-facing ingests API which has to stay up.
  use_fargate_spot = false

  logging_container = var.logging_container
}

module "nginx_container" {
  source = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/nginx/apigw?ref=v3.9.3"

  forward_port      = var.external_api_container_port
  log_configuration = module.base.log_configuration

  container_registry = var.nginx_container["container_registry"]
  container_name     = var.nginx_container["container_name"]
  container_tag      = var.nginx_container["container_tag"]
}

module "external_api_container" {
  source = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/container_definition?ref=v3.9.3"
  name   = "api"

  image = var.external_api_container_image

  environment = var.external_api_environment
  secrets     = var.external_api_secrets

  log_configuration = module.base.log_configuration
}

module "external_api_container_secrets_permissions" {
  source    = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/secrets?ref=v3.9.3"
  secrets   = var.external_api_secrets
  role_name = module.base.task_execution_role_name
}

module "worker_container" {
  source = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/container_definition?ref=v3.9.3"
  name   = "worker"

  image = var.worker_container_image

  environment = var.worker_environment
  secrets     = var.worker_secrets

  log_configuration = module.base.log_configuration
}

module "worker_container_secrets_permissions" {
  source    = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/secrets?ref=v3.9.3"
  secrets   = var.worker_secrets
  role_name = module.base.task_execution_role_name
}

module "internal_api_container" {
  source = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/container_definition?ref=v3.9.3"
  name   = "tracker"

  image = var.internal_api_container_image

  environment = var.internal_api_environment
  secrets     = var.internal_api_secrets

  log_configuration = module.base.log_configuration
}

module "internal_api_container_secrets_permissions" {
  source    = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//modules/secrets?ref=v3.9.3"
  secrets   = var.internal_api_secrets
  role_name = module.base.task_execution_role_name
}