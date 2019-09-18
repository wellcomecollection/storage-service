resource "aws_s3_bucket" "bagger_drop" {
  bucket = "wellcomecollection-${var.namespace}-bagger-drop"
  acl    = "private"

  lifecycle_rule {
    expiration {
      days = 90
    }

    enabled = true
  }
}

resource "aws_s3_bucket" "bagger_drop_mets_only" {
  bucket = "wellcomecollection-${var.namespace}-bagger-drop-mets-only"
  acl    = "private"

  lifecycle_rule {
    expiration {
      days = 90
    }

    enabled = true
  }
}

resource "aws_s3_bucket" "bagger_errors" {
  bucket = "wellcomecollection-${var.namespace}-bagger-errors"
  acl    = "private"
}

resource "aws_s3_bucket" "bagger_cache" {
  bucket = "wellcomecollection-${var.namespace}-bagger-asset-cache"
  acl    = "private"
}
