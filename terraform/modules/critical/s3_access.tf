resource "aws_s3_bucket" "access" {
  bucket = "wellcomecollection-${var.namespace}"
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

  # Created so that Digirati/DDS can read the bucket.

  statement {
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]

    resources = [
      "${aws_s3_bucket.access.arn}",
      "${aws_s3_bucket.access.arn}/*",
    ]

    condition {
      test     = "StringLike"
      variable = "aws:userId"

      values = [
        "AROAZQI22QHW3LZ4TYY54:*",

        # For the auxiliary ingest engine
        # See https://wellcome.slack.com/archives/CBT40CMKQ/p1569923258424800
        "AROAZQI22QHWUG2I4CBRN:*",

        # For Digirati
        # See https://wellcome.slack.com/archives/CBT40CMKQ/p1570188255112200
        "AROAZQI22QHWYAPBYZG6U:*",

        # For video ingests
        # See https://wellcome.slack.com/archives/CBT40CMKQ/p1571310993345000
        "AROAZQI22QHWV2KHZZHCT:*",
      ]
    }

    principals {
      type = "AWS"

      identifiers = [
        "*",
      ]
    }
  }
}
