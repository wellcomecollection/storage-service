resource "aws_s3_bucket" "large_response_cache" {
  bucket = "${var.bucket_name_prefix}${var.namespace}-large-response-cache"
  acl    = "private"

  lifecycle_rule {
    enabled = true

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }
}

