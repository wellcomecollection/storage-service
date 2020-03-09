module "worker" {
  source = "../worker"

  env_vars        = var.env_vars
  secret_env_vars = var.secret_env_vars

  subnets = var.subnets

  container_image = var.container_image

  namespace_id = var.namespace_id

  cluster_name = var.cluster_name
  cluster_arn  = var.cluster_arn

  service_name = var.service_name

  desired_task_count = var.desired_task_count

  launch_type = var.launch_type

  security_group_ids = var.security_group_ids

  cpu    = var.cpu
  memory = var.memory

  use_fargate_spot = var.use_fargate_spot
}

module "scaling" {
  source = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//autoscaling?ref=v1.1.0"

  name = var.service_name

  cluster_name = var.cluster_name
  service_name = var.service_name

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity
}
