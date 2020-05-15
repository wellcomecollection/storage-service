output "task_role_name" {
  value = module.base.task_role_name
}

output "task_role_arn" {
  value = module.base.task_role_arn
}

output "scale_up_arn" {
  value = module.scaling.scale_up_arn
}

output "scale_down_arn" {
  value = module.scaling.scale_down_arn
}
