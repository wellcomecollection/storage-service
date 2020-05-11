output "target_group_arn" {
  value = aws_lb_target_group.tcp.arn
}

output "name" {
  value = var.service_name
}

output "task_role_name" {
  value = module.base.task_role_name
}
