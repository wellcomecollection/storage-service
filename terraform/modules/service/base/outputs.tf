output "task_execution_role_name" {
  value = module.task_definition.task_execution_role_name
}

output "log_configuration" {
  value = module.log_router_container.container_log_configuration
}

output "task_role_name" {
  value = module.task_definition.task_role_name
}

output "task_role_arn" {
  value = module.task_definition.task_role_arn
}