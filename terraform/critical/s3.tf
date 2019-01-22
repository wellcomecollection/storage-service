resource "aws_s3_bucket" "static_content" {
  bucket = "wellcomecollection-public-${var.namespace}-static"
  acl    = "private"
}

resource "aws_s3_bucket" "ingests_drop" {
  bucket = "wellcomecollection-${var.namespace}-ingests"
  acl    = "private"
}

resource "aws_s3_bucket" "archive" {
  bucket = "wellcomecollection-${var.namespace}-archive"
  acl    = "private"

  lifecycle_rule {
    enabled = true

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

resource "aws_s3_bucket" "access" {
  bucket = "wellcomecollection-${var.namespace}-access"
  acl    = "private"
}

resource "aws_s3_bucket" "bagger_drop" {
  bucket = "wellcomecollection-${var.namespace}-bagger-drop"
  acl    = "private"
}

resource "aws_s3_bucket" "bagger_drop_mets_only" {
  bucket = "wellcomecollection-${var.namespace}-bagger-drop-mets-only"
  acl    = "private"
}

resource "aws_s3_bucket" "bagger_errors" {
  bucket = "wellcomecollection-${var.namespace}-bagger-errors"
  acl    = "private"
}
