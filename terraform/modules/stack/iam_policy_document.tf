data "aws_iam_policy_document" "archive_ingest_table_read_write_policy" {
  statement {
    actions = [
      "dynamodb:Query",
      "dynamodb:PutItem",
      "dynamodb:GetItem",
    ]

    resources = [
      "${var.ingests_table_arn}",
    ]
  }
}

data "aws_iam_policy_document" "replicas_table_readwrite" {
  statement {
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:Query",
    ]

    resources = [
      "${var.replicas_table_arn}",
    ]
  }
}

# Bagger

data "aws_iam_policy_document" "bagger_ingest_table_readwrite" {
  statement {
    actions = [
      "dynamodb:UpdateItem",
      "dynamodb:PutItem",
      "dynamodb:GetItem",
      "dynamodb:DeleteItem",
    ]

    resources = [
      "${var.bagger_ingest_table_arn}",
    ]
  }

  statement {
    actions = [
      "dynamodb:Query",
    ]

    resources = [
      "${var.bagger_ingest_table_arn}/index/*",
    ]
  }
}

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
      "s3:DeleteObject*",
      "s3:PutObject*",
      "s3:GetObject*",
    ]

    resources = [
      "arn:aws:s3:::${var.ingest_drop_bucket_name}/*",
      "${var.s3_bagger_drop_arn}/*",
      "${var.s3_bagger_drop_mets_only_arn}/*",
      "${var.s3_bagger_errors_arn}/*",
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

data "aws_iam_policy_document" "replica_primary_readonly" {
  statement {
    actions = [
      "s3:ListBucket",
      "s3:GetObject*",
    ]

    resources = [
      "arn:aws:s3:::${var.replica_primary_bucket_name}",
      "arn:aws:s3:::${var.replica_primary_bucket_name}/*",
    ]
  }
}

data "aws_iam_policy_document" "ingests_read" {
  statement {
    actions = [
      "s3:ListBucket",
    ]

    resources = [
      "arn:aws:s3:::${var.workflow_bucket_name}",
      "${var.s3_bagger_drop_arn}",
      "arn:aws:s3:::${var.ingest_drop_bucket_name}",
    ]
  }

  statement {
    actions = [
      "s3:GetObject*",
    ]

    resources = [
      "arn:aws:s3:::${var.workflow_bucket_name}/*",
      "${var.s3_bagger_drop_arn}/*",
      "arn:aws:s3:::${var.ingest_drop_bucket_name}/*",
    ]
  }
}

data "aws_iam_policy_document" "storage_ingests_drop_read_write" {
  statement {
    actions = [
      "s3:ListBucket",
    ]

    resources = [
      "arn:aws:s3:::${var.ingest_drop_bucket_name}",
    ]
  }

  statement {
    actions = [
      "s3:GetObject*",
      "s3:PutObject*",
    ]

    resources = [
      "arn:aws:s3:::${var.ingest_drop_bucket_name}/*",
    ]
  }
}

data "aws_iam_policy_document" "storage_bagger_cache_drop_read_write" {
  statement {
    actions = [
      "s3:ListBucket",
    ]

    resources = [
      "arn:aws:s3:::${var.s3_bagger_cache_name}",
    ]
  }

  statement {
    actions = [
      "s3:GetObject*",
      "s3:PutObject*",
    ]

    resources = [
      "arn:aws:s3:::${var.s3_bagger_cache_name}/*",
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

data "aws_iam_policy_document" "s3_large_response_cache" {
  statement {
    actions = [
      "s3:*"
    ]

    resources = [
      "${aws_s3_bucket.large_response_cache.arn}",
      "${aws_s3_bucket.large_response_cache.arn}/*",
    ]
  }
}

# bagger

data "aws_iam_policy_document" "bagger_s3_readwrite" {
  statement {
    actions = [
      "s3:*",
    ]

    resources = [
      "${var.s3_bagger_drop_arn}/*",
    ]
  }

  statement {
    actions = [
      "s3:*",
    ]

    resources = [
      "${var.s3_bagger_drop_mets_only_arn}/*",
    ]
  }

  statement {
    actions = [
      "s3:*",
    ]

    resources = [
      "${var.s3_bagger_errors_arn}/*",
    ]
  }
}
