resource "aws_s3_bucket" "infra" {
  bucket = "wellcomecollection-storage-infra"
  acl    = "private"

  lifecycle {
    prevent_destroy = true
  }

  versioning {
    enabled = true
  }
}

