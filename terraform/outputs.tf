# These outputs are provided so Archivematica can give permission for the
# unpacker(s) to read from the bucket where it uploads new bags.

output "unpacker_task_role_arns" {
  value = [
    "${module.stack_staging.unpacker_task_role_arn}",
    "${module.stack_prod.unpacker_task_role_arn}",
  ]
}

output "prod_domain_name" {
  value = "${module.stack_prod.api_domain_name}"
}
