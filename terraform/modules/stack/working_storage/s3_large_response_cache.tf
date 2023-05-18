resource "aws_s3_bucket" "large_response_cache" {
  bucket = "${var.bucket_name_prefix}${var.namespace}-large-response-cache"
}

resource "aws_s3_bucket_acl" "large_response_cache" {
  bucket = aws_s3_bucket.large_response_cache.id
  acl    = "private"
}

resource "aws_s3_bucket_lifecycle_configuration" "large_response_cache" {
  bucket = aws_s3_bucket.large_response_cache.id

  rule {
    id           = "transition-to-standard-id"
    status = "Enabled"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }
}