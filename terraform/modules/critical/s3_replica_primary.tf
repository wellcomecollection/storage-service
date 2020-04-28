resource "aws_s3_bucket" "replica_primary" {
  bucket = "wellcomecollection-${var.namespace}"
  acl    = "private"

  versioning {
    enabled = var.enable_s3_versioning
  }

  lifecycle_rule {
    enabled = true

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }
}

resource "aws_s3_bucket_policy" "replica_primary_access" {
  bucket = aws_s3_bucket.replica_primary.id
  policy = data.aws_iam_policy_document.replica_primary_access.json
}

data "aws_iam_policy_document" "replica_primary_access" {
  # By default, we don't allow anybody to write or delete objects in the bucket.
  #
  # We only allow write permissions to certain, pre-approved IAM roles.  This is
  # enforced at the bucket level, so even creating a role with "S3 full access" will
  # not allow you to modify bucket objects.

  statement {
    effect = "Deny"

    actions = [
      "s3:Put*",
      "s3:Delete*",
    ]

    resources = [
      "${aws_s3_bucket.replica_primary.arn}",
      "${aws_s3_bucket.replica_primary.arn}/*",
    ]

    principals {
      type = "AWS"

      identifiers = [
        "*",
      ]
    }
  }

  statement {
    effect = "Allow"

    actions = [
      "s3:PutObject*",
    ]

    resources = [
      "${aws_s3_bucket.replica_primary.arn}",
      "${aws_s3_bucket.replica_primary.arn}/*",
    ]

    principals {
      type = "AWS"

      identifiers = [
        var.replicator_primary_task_role_arn
      ]
    }
  }

  # Give certain read permissions to the bucket

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

      identifiers = sort(var.replica_primary_read_principals)
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
