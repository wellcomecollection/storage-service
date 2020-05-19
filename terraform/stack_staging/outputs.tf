output "staging_domain_name" {
  value = module.stack_staging.api_domain_name
}

output "unpacker_task_role_name" {
  value = module.stack_staging.unpacker_task_role_name
}

output "unpacker_task_role_arn" {
  value = module.stack_staging.unpacker_task_role_arn
}
