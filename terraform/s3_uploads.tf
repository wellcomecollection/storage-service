resource "aws_s3_bucket" "uploads" {
  bucket = "${local.namespace}-uploads"
  acl    = "private"
}
