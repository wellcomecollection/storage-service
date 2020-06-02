resource "aws_s3_bucket" "static_content" {
  bucket = "wellcomecollection-public-${var.namespace}-static"
  acl    = "private"

  tags = var.tags
}
