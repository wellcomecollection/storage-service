module "service" {
  source = "git::https://github.com/wellcometrust/terraform.git//ecs/prebuilt/scaling?ref=7673218"

  service_name    = var.service_name
  container_image = var.container_image

  subnets = var.subnets

  namespace_id = var.namespace_id

  cluster_id   = var.cluster_id
  cluster_name = var.cluster_name

  cpu    = var.cpu
  memory = var.memory

  security_group_ids = [var.security_group_ids]

  env_vars        = var.env_vars
  env_vars_length = var.env_vars_length

  secret_env_vars        = var.secret_env_vars
  secret_env_vars_length = var.secret_env_vars_length

  min_capacity = var.min_capacity
  max_capacity = var.max_capacity

  desired_task_count = var.desired_task_count

  launch_type = var.launch_type
}

