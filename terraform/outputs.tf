# These outputs are provided so Archivematica can give permission for the
# unpacker(s) to read from the bucket where it uploads new bags.

output "unpacker_task_role_arns" {
  value = [
    "${module.stack-colbert.unpacker_task_role_arn}",
    "${module.stack_letterman.unpacker_task_role_arn}",
  ]
}

output "staging_domain_name" {
  value = "${module.stack-colbert.api_domain_name}"
}

output "prod_domain_name" {
  value = "${module.stack_letterman.api_domain_name}"
}
