resource "aws_s3_bucket" "unpacked_bags" {
  bucket = "${var.bucket_name_prefix}${var.namespace}-unpacked-bags"
}

resource "aws_s3_bucket_acl" "unpacked_bags" {
  bucket = aws_s3_bucket.unpacked_bags.id
  acl    = "private"
}

resource "aws_s3_bucket_lifecycle_configuration" "unpacked_bags" {
  bucket = aws_s3_bucket.unpacked_bags.id

  rule {
    id     = "expire-after-30-days"
    status = "Enabled"

    expiration {
      days = 30
    }
  }
}