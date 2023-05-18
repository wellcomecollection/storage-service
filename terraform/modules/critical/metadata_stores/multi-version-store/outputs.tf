output "dynamodb_update_policy" {
  value = data.aws_iam_policy_document.dynamodb_update_policy
}

output "read_policy" {
  value = data.aws_iam_policy_document.read_policy.json
}

output "full_access_policy" {
  value = data.aws_iam_policy_document.full_access_policy.json
}

output "table_name" {
  value = aws_dynamodb_table.table.name
}

output "table_arn" {
  value = aws_dynamodb_table.table.arn
}

output "bucket_name" {
  value = aws_s3_bucket.bucket.id
}

output "bucket_arn" {
  value = aws_s3_bucket.bucket.arn
}
