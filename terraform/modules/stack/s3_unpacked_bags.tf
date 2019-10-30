resource "aws_s3_bucket" "unpacked_bags" {
  bucket = "wellcomecollection-${var.namespace}-unpacked-bags"
  acl    = "private"

  lifecycle_rule {
    expiration {
      days = "30"
    }

    enabled = true
  }
}
