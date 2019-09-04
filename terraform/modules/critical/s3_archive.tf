resource "aws_s3_bucket" "archive" {
  bucket = "wellcomecollection-${var.namespace}-replica-ireland"
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
