output "prod_domain_name" {
  value = module.stack_prod.api_domain_name
}

output "unpacker_task_role_name" {
  value = module.stack_prod.unpacker_task_role_name
}

output "unpacker_task_role_arn" {
  value = module.stack_prod.unpacker_task_role_arn
}

output "registered_bag_notifications_topic_arn" {
  value = module.stack_prod.registered_bag_notifications_topic_arn
}
