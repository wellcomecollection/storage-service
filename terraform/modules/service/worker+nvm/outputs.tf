output "name" {
  value = "${module.service.service_name}"
}

output "task_role_name" {
  value = "${module.service.task_role_name}"
}

output "scale_up_arn" {
  value = "${module.service.scale_up_arn}"
}

output "scale_down_arn" {
  value = "${module.service.scale_down_arn}"
}
