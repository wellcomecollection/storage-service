resource "aws_s3_bucket" "static_content" {
  bucket = "wellcomecollection-public-${var.namespace}-static"
  acl    = "private"
}

resource "aws_s3_bucket" "ingests_drop" {
  bucket = "wellcomecollection-${var.namespace}-ingests"
  acl    = "private"
}

resource "aws_s3_bucket" "access" {
  bucket = "wellcomecollection-${var.namespace}-access"
  acl    = "private"
}

resource "aws_s3_bucket_policy" "access_read" {
  bucket = "${aws_s3_bucket.access.id}"
  policy = "${data.aws_iam_policy_document.access_read.json}"
}

data "aws_iam_policy_document" "access_read" {
  statement {
    actions = [
      "s3:List*",
      "s3:Get*",
    ]

    resources = [
      "${aws_s3_bucket.access.arn}",
      "${aws_s3_bucket.access.arn}/*",
    ]

    principals {
      type = "AWS"

      identifiers = [
        "${var.access_read_principles}",
      ]
    }
  }
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

resource "aws_s3_bucket_policy" "archive_read" {
  bucket = "${aws_s3_bucket.archive.id}"
  policy = "${data.aws_iam_policy_document.archive_read.json}"
}

data "aws_iam_policy_document" "archive_read" {
  statement {
    actions = [
      "s3:List*",
      "s3:Get*",
    ]

    resources = [
      "${aws_s3_bucket.archive.arn}",
      "${aws_s3_bucket.archive.arn}/*",
    ]

    principals {
      type = "AWS"

      identifiers = [
        "${var.archive_read_principles}",
      ]
    }
  }
}

# bagger

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
