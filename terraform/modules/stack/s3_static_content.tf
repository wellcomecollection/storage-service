locals {
  static_bucket_name = var.namespace == "storage-prod" ? "wellcomecollection-public-storage-static" : "wellcomecollection-public-${var.namespace}-static"
}

resource "aws_s3_bucket" "static_content" {
  bucket = local.static_bucket_name
  acl    = "private"
}
