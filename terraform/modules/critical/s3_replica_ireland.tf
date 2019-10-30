resource "aws_s3_bucket" "replica_ireland" {
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

resource "aws_s3_bucket_policy" "replica_ireland_read" {
  bucket = "${aws_s3_bucket.replica_ireland.id}"
  policy = "${data.aws_iam_policy_document.replica_ireland_read.json}"
}

data "aws_iam_policy_document" "replica_ireland_read" {
  statement {
    actions = [
      "s3:List*",
      "s3:Get*",
    ]

    resources = [
      "${aws_s3_bucket.replica_ireland.arn}",
      "${aws_s3_bucket.replica_ireland.arn}/*",
    ]

    principals {
      type = "AWS"

      identifiers = [
        "${var.replica_ireland_read_principals}",
      ]
    }
  }
}
