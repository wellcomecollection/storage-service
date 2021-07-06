resource "local_file" "readme" {
  content  = templatefile(
    "${path.module}/README.html.template",
    {
      primary_bucket  = aws_s3_bucket.replica_primary.bucket,
      glacier_bucket  = aws_s3_bucket.replica_glacier.bucket,
      uploads_bucket  = aws_s3_bucket.uploads.bucket,
      unpacker_bucket = module.stack.unpacked_bags_bucket_name,
      token_url       = local.token_url
      api_url         = module.stack.api_invoke_url
      namespace       = local.namespace
      kibana_url      = aws_elasticsearch_domain.elasticsearch.kibana_endpoint
    }
  )
  filename = "${path.module}/README.html"
}

output "elasticsearch_host" {
  value = aws_elasticsearch_domain.elasticsearch.endpoint
}

output "token_url" {
  value = local.token_url
}

output "api_url" {
  value = module.stack.api_invoke_url
}
