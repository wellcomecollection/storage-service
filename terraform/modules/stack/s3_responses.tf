resource "aws_s3_bucket" "large_response_cache" {
  bucket = "wellcomecollection-${var.namespace}-large-response-cache"
  acl    = "private"
}

