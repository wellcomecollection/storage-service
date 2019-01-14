data "aws_iam_policy_document" "archive_progress_table_read_write_policy" {
  statement {
    actions = [
      "dynamodb:UpdateItem",
      "dynamodb:PutItem",
      "dynamodb:GetItem",
      "dynamodb:DeleteItem",
    ]

    resources = [
      "${var.ingests_table_arn}",
    ]
  }

  statement {
    actions = [
      "dynamodb:Query",
    ]

    resources = [
      "${var.ingests_table_arn}/index/*",
    ]
  }
}

# Bagger

data "aws_iam_policy_document" "bagger_queue_discovery" {
  statement {
    actions = [
      "sqs:ChangeMessageVisibility",
      "sqs:GetQueueUrl",
    ]

    resources = [
      "${module.bagger_queue.arn}",
    ]
  }
}

data "aws_iam_policy_document" "bagger_read" {
  statement {
    actions = [
      "s3:GetObject*",
    ]

    resources = [
      "arn:aws:s3:::${var.bagger_mets_bucket_name}",
      "arn:aws:s3:::${var.bagger_mets_bucket_name}/*",
    ]
  }
}

data "aws_iam_policy_document" "bagger_readwrite" {
  statement {
    actions = [
      "s3:PutObject*",
      "s3:GetObject*",
    ]

    resources = [
      "arn:aws:s3:::${var.ingest_drop_bucket_name}/*",
      "arn:aws:s3:::${var.bagger_drop_bucket_name}/*",
      "arn:aws:s3:::${var.bagger_drop_bucket_name_mets_only}/*",
      "arn:aws:s3:::${var.bagger_drop_bucket_name_errors}/*",
    ]
  }
}

data "aws_iam_policy_document" "bagger_dlcs_read" {
  statement {
    actions = [
      "s3:GetObject*",
    ]

    resources = [
      "arn:aws:s3:::${var.bagger_dlcs_source_bucket}",
      "arn:aws:s3:::${var.bagger_dlcs_source_bucket}/*",
    ]
  }
}

data "aws_iam_policy_document" "bagger_preservica_read" {
  statement {
    actions = [
      "s3:GetObject*",
    ]

    resources = [
      "arn:aws:s3:::${var.bagger_current_preservation_bucket}",
      "arn:aws:s3:::${var.bagger_current_preservation_bucket}/*",
    ]
  }
}

data "aws_iam_policy_document" "storage_dlcs_read" {
  statement {
    effect = "Allow"

    principals {
      type = "AWS"

      identifiers = [
        "arn:aws:iam::653428163053:user/echo-fs",
        "arn:aws:iam::653428163053:user/api",
      ]
    }

    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]

    resources = [
      "arn:aws:s3:::${var.archive_bucket_name}",
      "arn:aws:s3:::${var.archive_bucket_name}/*",
    ]
  }
}

data "aws_iam_policy_document" "storage_archive_read" {
  statement {
    actions = [
      "s3:ListBucket",
      "s3:GetObject*",
    ]

    resources = [
      # Allow archivist to read bagger drop bucket
      "arn:aws:s3:::${var.ingest_drop_bucket_name}/*",

      # Allow archivist to read our archive bucket
      "arn:aws:s3:::${var.archive_bucket_name}",

      "arn:aws:s3:::${var.archive_bucket_name}/*",
    ]
  }
}

data "aws_iam_policy_document" "storage_archive_readwrite" {
  statement {
    actions = [
      "s3:PutObject*",
      "s3:GetObject*",
    ]

    resources = [
      "arn:aws:s3:::${var.archive_bucket_name}/*",
    ]
  }
}

data "aws_iam_policy_document" "storage_access_readwrite" {
  statement {
    actions = [
      "s3:PutObject*",
      "s3:GetObject*",
    ]

    resources = [
      "arn:aws:s3:::${var.access_bucket_name}/*",
    ]
  }
}

data "aws_iam_policy_document" "ingests_read" {
  statement {
    actions = [
      "s3:GetObject*",
      "s3:ListBucket",
    ]

    resources = [
      "arn:aws:s3:::${var.workflow_bucket_name}/*",
      "arn:aws:s3:::${var.bagger_drop_bucket_name}/*",
      "arn:aws:s3:::${var.ingest_drop_bucket_name}/*",
    ]
  }
}

data "aws_iam_policy_document" "cloudwatch_put" {
  statement {
    actions = [
      "cloudwatch:PutMetricData",
    ]

    resources = [
      "*",
    ]
  }
}
