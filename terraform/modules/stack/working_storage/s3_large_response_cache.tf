resource "aws_s3_bucket" "large_response_cache" {
  bucket = "${var.bucket_name_prefix}${var.namespace}-large-response-cache"
}

resource "aws_s3_bucket_acl" "large_response_cache" {
  bucket = aws_s3_bucket.large_response_cache.id
  acl    = "private"
}

resource "aws_s3_bucket_lifecycle_configuration" "large_response_cache" {
  bucket = aws_s3_bucket.large_response_cache.id

  # Pin the pre-provider-5.100 behaviour; the new provider default
  # (all_storage_classes_128K) would stop <128K objects transitioning.
  transition_default_minimum_object_size = "varies_by_storage_class"

  rule {
    id     = "transition-to-standard-id"
    status = "Enabled"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }
}