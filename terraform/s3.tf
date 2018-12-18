resource "aws_s3_bucket" "storage_archive" {
  bucket = "${local.storage_archive_bucket_name}"
  acl    = "private"
}

resource "aws_s3_bucket" "storage_access" {
  bucket = "${local.storage_access_bucket_name}"
  acl    = "private"
}
