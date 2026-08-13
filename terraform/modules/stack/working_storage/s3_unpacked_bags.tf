resource "aws_s3_bucket" "unpacked_bags" {
  bucket = "${var.bucket_name_prefix}${var.namespace}-unpacked-bags"
}

resource "aws_s3_bucket_acl" "unpacked_bags" {
  bucket = aws_s3_bucket.unpacked_bags.id
  acl    = "private"
}

resource "aws_s3_bucket_lifecycle_configuration" "unpacked_bags" {
  bucket = aws_s3_bucket.unpacked_bags.id

  # Pin the pre-provider-5.100 behaviour; the new provider default
  # (all_storage_classes_128K) would stop <128K objects transitioning.
  transition_default_minimum_object_size = "varies_by_storage_class"

  rule {
    id     = "expire-after-30-days"
    status = "Enabled"

    expiration {
      days = 30
    }
  }
}