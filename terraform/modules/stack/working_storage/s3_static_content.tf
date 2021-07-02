locals {
  prod_bucket_name      = "${var.bucket_name_prefix}public-storage-static"
  namespace_bucket_name = "${var.bucket_name_prefix}public-${var.namespace}-static"

  static_bucket_name = var.namespace == "storage-prod" ? local.prod_bucket_name : local.namespace_bucket_name
}

resource "aws_s3_bucket" "static_content" {
  bucket = local.static_bucket_name
  acl    = "private"
}
