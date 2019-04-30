# These outputs are provided so Archivematica can give permission for the
# unpacker(s) to read from the bucket where it uploads new bags.

output "unpacker_task_role_arns" {
  value = [
    "${module.stack-colbert.unpacker_task_role_arn}",
  ]

  /*    "${module.stack-stewart.unpacker_task_role_arn}",*/
}
