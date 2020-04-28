resource "aws_s3_bucket" "replica_glacier" {
  bucket = "wellcomecollection-${var.namespace}-replica-ireland"
  acl    = "private"

  versioning {
    enabled = var.enable_s3_versioning
  }

  lifecycle_rule {
    enabled = true

    transition {
      days          = 90
      storage_class = "DEEP_ARCHIVE"
    }
  }
}

resource "aws_s3_bucket_policy" "replica_glacier_access" {
  bucket = aws_s3_bucket.replica_glacier.id
  policy = data.aws_iam_policy_document.replica_glacier_access.json
}

data "aws_iam_policy_document" "replica_glacier_access" {
  # By default, we don't allow anybody to write or delete objects in the bucket.
  #
  # We only allow write permissions to certain, pre-approved IAM roles.  This is
  # enforced at the bucket level, so even creating a role with "S3 full access" will
  # not allow you to modify bucket objects.

  statement {
    effect = "Deny"

    actions = [
      "s3:Delete*",
    ]

    resources = [
      "${aws_s3_bucket.replica_glacier.arn}",
      "${aws_s3_bucket.replica_glacier.arn}/*",
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
      "${aws_s3_bucket.replica_glacier.arn}",
      "${aws_s3_bucket.replica_glacier.arn}/*",
    ]

    principals {
      type = "AWS"

      identifiers = [
        var.replicator_glacier_task_role_arn
      ]
    }
  }
}
