output "target_group_arn" {
  value = aws_lb_target_group.tcp.arn
}

output "name" {
  value = module.service.name
}

output "task_role_name" {
  value = module.task_definition.task_role_name
}

