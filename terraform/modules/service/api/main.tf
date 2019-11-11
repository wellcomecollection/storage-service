module "service" {
  source = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//service?ref=v1.0.0"

  service_name = var.namespace

  cluster_arn = var.cluster_arn

  task_definition_arn = module.task_definition.arn

  subnets = var.subnets

  namespace_id = var.namespace_id

  security_group_ids = var.security_group_ids

  launch_type = var.launch_type

  desired_task_count = var.desired_task_count

  target_group_arn = aws_lb_target_group.tcp.arn
  container_name   = "nginx"
  container_port   = var.nginx_container_port
}

module "task_definition" {
  source = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//task_definition/container_with_sidecar?ref=v1.0.0"

  task_name = var.namespace

  app_container_image = var.container_image
  app_container_port  = var.container_port

  sidecar_container_image = var.nginx_container_image
  sidecar_container_port  = var.nginx_container_port

  app_env_vars = var.env_vars

  sidecar_env_vars = {
    APP_HOST = "localhost"
    APP_PORT = var.container_port
  }

  cpu    = var.cpu
  memory = var.memory

  sidecar_cpu    = var.sidecar_cpu
  sidecar_memory = var.sidecar_memory

  app_cpu    = var.app_cpu
  app_memory = var.app_memory

  aws_region = var.aws_region
}
