data "aws_caller_identity" "current" {}

data "aws_iam_policy_document" "table_ingests_readwrite" {
  statement {
    actions = [
      "dynamodb:Query",
      "dynamodb:PutItem",
      "dynamodb:GetItem",
    ]

    resources = [
      var.ingests_table_arn,
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
      var.replicas_table_arn,
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
      aws_s3_bucket.unpacked_bags.arn,
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
      "s3:*",
    ]

    resources = [
      aws_s3_bucket.large_response_cache.arn,
      "${aws_s3_bucket.large_response_cache.arn}/*",
    ]
  }
}

data "aws_iam_policy_document" "archivematica_ingests_get" {
  statement {
    actions = [
      "s3:Get*",
    ]

    resources = [
      "${var.archivematica_ingests_bucket}/*",
    ]
  }
}

// This is the default policy that gets added to a topic when no policy is supplied
// plus the statement that allows other principals to subscribe to this topic
data "aws_iam_policy_document" "register_output_subscribe" {
  policy_id = "__default_policy_ID"

  // default permissions copied from https://www.terraform.io/docs/providers/aws/r/sns_topic_policy.html
  statement {
    actions = [
      "SNS:Subscribe",
      "SNS:SetTopicAttributes",
      "SNS:RemovePermission",
      "SNS:Receive",
      "SNS:Publish",
      "SNS:ListSubscriptionsByTopic",
      "SNS:GetTopicAttributes",
      "SNS:DeleteTopic",
      "SNS:AddPermission",
    ]

    condition {
      test     = "StringEquals"
      variable = "AWS:SourceOwner"

      values = [
        data.aws_caller_identity.current.account_id,
      ]
    }

    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = ["*"]
    }

    resources = [
      "${module.bag_register_output_topic.arn}",
    ]

    sid = "__default_statement_ID"
  }
  // allow subscription to other principals
  statement {
    effect = "Allow"
    actions = [
      "sns:Subscribe"
    ]
    resources = ["${module.bag_register_output_topic.arn}"]
    principals {
      identifiers = "${var.register_output_subscribe_principals}"
      type = "AWS"
    }
  }
}
