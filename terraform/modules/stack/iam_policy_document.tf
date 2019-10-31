data "aws_iam_policy_document" "table_ingests_readwrite" {
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

data "aws_iam_policy_document" "table_replicas_readwrite" {
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

data "aws_iam_policy_document" "unpacked_bags_bucket_readonly" {
  statement {
    actions = [
      "s3:ListBucket",
    ]

    resources = [
      "${aws_s3_bucket.unpacked_bags.arn}",
    ]
  }


  statement {
    actions = [
      "s3:GetObject*",
    ]

    resources = [
      "${aws_s3_bucket.unpacked_bags.arn}/*",
    ]
  }
}

data "aws_iam_policy_document" "unpacked_bags_bucket_readwrite" {
  statement {
    actions = [
      "s3:GetObject*",
      "s3:PutObject*",
    ]

    resources = [
      "${aws_s3_bucket.unpacked_bags.arn}/*",
    ]
  }
}

data "aws_iam_policy_document" "drop_buckets_readonly" {
  statement {
    actions = [
      "s3:ListBucket",
    ]

    resources = [
      "arn:aws:s3:::${var.workflow_bucket_name}",
    ]
  }

  statement {
    actions = [
      "s3:GetObject*",
    ]

    resources = [
      "arn:aws:s3:::${var.workflow_bucket_name}/*",
    ]
  }
}

data "aws_iam_policy_document" "cloudwatch_putmetrics" {
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
