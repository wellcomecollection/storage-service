# These outputs are provided so Archivematica can give permission for the
# unpacker(s) to read from the bucket where it uploads new bags.

output "unpacker_task_role_arns" {
  value = [
    "${data.aws_iam_role.colbert_bag_unpacker_task_role.arn}",
    "${data.aws_iam_role.stewart_bag_unpacker_task_role.arn}",
  ]
}
