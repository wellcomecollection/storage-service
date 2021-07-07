output "elasticsearch_host" {
  value = aws_elasticsearch_domain.elasticsearch.endpoint
}

output "kibana_endpoint" {
  value = aws_elasticsearch_domain.elasticsearch.kibana_endpoint
}

output "token_url" {
  value = local.token_url
}

output "api_url" {
  value = module.stack.api_invoke_url
}

output "replica_primary_bucket_name" {
  value = aws_s3_bucket.replica_primary.bucket
}

output "replica_glacier_bucket_name" {
  value = aws_s3_bucket.replica_glacier.bucket
}

output "uploads_bucket_name" {
  value = aws_s3_bucket.uploads.bucket
}

output "unpacked_bags_bucket_name" {
  value = module.stack.unpacked_bags_bucket_name
}