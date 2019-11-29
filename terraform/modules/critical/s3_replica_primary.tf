resource "aws_s3_bucket" "replica_primary" {
  bucket = "wellcomecollection-${var.namespace}"
  acl    = "private"
}

resource "aws_s3_bucket_policy" "replica_primary_read" {
  bucket = aws_s3_bucket.replica_primary.id
  policy = data.aws_iam_policy_document.replica_primary_read.json
}

data "aws_iam_policy_document" "replica_primary_read" {
  statement {
    actions = [
      "s3:List*",
      "s3:Get*",
    ]

    resources = [
      "${aws_s3_bucket.replica_primary.arn}",
      "${aws_s3_bucket.replica_primary.arn}/*",
    ]

    principals {
      type = "AWS"

      identifiers = var.replica_primary_read_principals
    }
  }

  # Created so that Digirati/DDS can read both the prod and the staging buckets.

  statement {
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]

    resources = [
      "${aws_s3_bucket.replica_primary.arn}",
      "${aws_s3_bucket.replica_primary.arn}/*",
    ]

    condition {
      test     = "StringLike"
      variable = "aws:userId"

      values = [
        "AROAZQI22QHW3LZ4TYY54:*",

        # For the auxiliary ingest engine
        # See https://wellcome.slack.com/archives/CBT40CMKQ/p1569923258424800
        "AROAZQI22QHWUG2I4CBRN:*",

        # For the Tizer engine
        # See https://wellcome.slack.com/archives/CBT40CMKQ/p1570188255112200
        #     https://wellcome.slack.com/archives/CBT40CMKQ/p1574954471260700
        "AROAZQI22QHWYAPBYZG6U:*",

        # For video ingests
        # See https://wellcome.slack.com/archives/CBT40CMKQ/p1571310993345000
        "AROAZQI22QHWV2KHZZHCT:*",

        # Beta version of the DLCS orchestrator.
        # See https://wellcome.slack.com/archives/CBT40CMKQ/p1573742247457800
        "AROAZQI22QHWTHLN4QHJU:*",
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
