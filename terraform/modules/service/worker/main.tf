module "service" {
  source = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//service?ref=2a6b61585338554146cbd75ddc68243bd19fd87a"

  service_name = var.service_name

  cluster_arn = var.cluster_arn

  task_definition_arn = module.task_definition.arn

  subnets = var.subnets

  namespace_id = var.namespace_id

  security_group_ids = var.security_group_ids

  launch_type = var.launch_type

  desired_task_count = var.desired_task_count
}

module "task_definition" {
  source = "git::github.com/wellcomecollection/terraform-aws-ecs-service.git//task_definition/single_container?ref=2802adab9bb37b17a032f2a830adcb2ee07e51f4"

  task_name = var.service_name

  container_image = var.container_image

  cpu    = var.cpu
  memory = var.memory

  env_vars        = var.env_vars
  secret_env_vars = var.secret_env_vars

  launch_type = var.launch_type

  aws_region = "eu-west-1"
}
