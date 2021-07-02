output "large_response_cache_bucket_name" {
  value = aws_s3_bucket.large_response_cache.bucket
}

output "static_content_bucket_name" {
  value = aws_s3_bucket.static_content.bucket
}

output "unpacked_bags_bucket_name" {
  value = aws_s3_bucket.unpacked_bags.bucket
}

output "large_response_cache_bucket_arn" {
  value = aws_s3_bucket.large_response_cache.arn
}

output "static_content_bucket_arn" {
  value = aws_s3_bucket.static_content.arn
}

output "unpacked_bags_bucket_arn" {
  value = aws_s3_bucket.unpacked_bags.arn
}
