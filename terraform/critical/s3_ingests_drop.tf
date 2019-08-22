resource "aws_s3_bucket" "ingests_drop" {
  bucket = "wellcomecollection-${var.namespace}-ingests"
  acl    = "private"

  lifecycle_rule {
    expiration {
      days = "30"
    }

    enabled = true
  }
}

resource "aws_s3_bucket_policy" "ingests_drop" {
  bucket = "${aws_s3_bucket.ingests_drop.id}"
  policy = "${data.aws_iam_policy_document.ingests_drop_read.json}"
}

data "aws_iam_policy_document" "ingests_drop_read" {
  statement {
    actions = [
      "s3:List*",
      "s3:Get*",
    ]

    resources = [
      "${aws_s3_bucket.ingests_drop.arn}",
      "${aws_s3_bucket.ingests_drop.arn}/*",
    ]

    principals {
      type = "AWS"

      identifiers = [
        "${var.ingest_read_principles}",
      ]
    }
  }
}
