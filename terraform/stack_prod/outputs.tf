output "prod_domain_name" {
  value = module.stack_prod.api_domain_name
}

output "unpacker_task_role_name" {
  value = module.stack_prod.unpacker_task_role_name
}

output "unpacker_task_role_arn" {
  value = module.stack_prod.unpacker_task_role_arn
}

output "bag_register_output_topic_arn" {
  value = module.stack_prod.bag_register_output_topic_arn
}

