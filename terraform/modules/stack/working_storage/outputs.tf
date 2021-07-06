output "large_response_cache_bucket_name" {
  value = aws_s3_bucket.large_response_cache.bucket
}

output "unpacked_bags_bucket_name" {
  value = aws_s3_bucket.unpacked_bags.bucket
}

output "large_response_cache_bucket_arn" {
  value = aws_s3_bucket.large_response_cache.arn
}

output "unpacked_bags_bucket_arn" {
  value = aws_s3_bucket.unpacked_bags.arn
}

output "replicator_lock_iam_policy" {
  value = module.replicator_lock_table.iam_policy
}

output "replicator_lock_index_name" {
  value = module.replicator_lock_table.index_name
}

output "replicator_lock_table_name" {
  value = module.replicator_lock_table.table_name
}

output "versioner_lock_iam_policy" {
  value = module.versioner_lock_table.iam_policy
}

output "versioner_lock_index_name" {
  value = module.versioner_lock_table.index_name
}

output "versioner_lock_table_name" {
  value = module.versioner_lock_table.table_name
}

output "azure_verifier_cache_table_name" {
  value = aws_dynamodb_table.azure_verifier_tags.*.name
}

output "azure_verifier_cache_table_arn" {
  value = aws_dynamodb_table.azure_verifier_tags.*.arn
}
