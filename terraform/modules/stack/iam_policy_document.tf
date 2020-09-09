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

data "aws_iam_policy_document" "unpacked_bags_bucket_put_tags" {
  statement {
    actions = [
      "s3:PutObjectTagging",
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

data "aws_iam_policy_document" "primary_replica_put_tags" {
  statement {
    actions = [
      "s3:PutObjectTagging",
    ]

    resources = [
      "arn:aws:s3:::${var.replica_primary_bucket_name}/*",
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

data "aws_iam_policy_document" "allow_tagging_objects" {
  statement {
    actions = [
      "s3:GetObjectTagging",
      "s3:PutObjectTagging",
    ]

    # The prefix restriction reflects the fact that the current bag tagger
    # should only be applying tags to objects in the digitised prefix.
    # It should never try to tag born-digital objects; if it does something
    # has gone wrong that we should investigate.
    #
    # If we change the rule set, we should expand these permissions.
    resources = [
      "arn:aws:s3:::${var.replica_primary_bucket_name}/digitised/*",
      "arn:aws:s3:::${var.replica_glacier_bucket_name}/digitised/*",
    ]
  }
}

data "aws_iam_policy_document" "azure_verifier_tags_readwrite" {
  statement {
    actions = [
      "dynamodb:DeleteItem",
      "dynamodb:GetItem",
      "dynamodb:PutItem",
    ]

    resources = [
      aws_dynamodb_table.azure_verifier_tags.arn,
    ]
  }
}

# This policy document is specifically to allow subscription across account
# boundaries.  It will only be used if there is a non-empty list of other
# account principals to grant access to.
data "aws_iam_policy_document" "allow_bag_registration_notification_subscription" {
  policy_id = "__default_policy_ID"

  # This is the default policy that gets added to a topic when no policy is supplied
  # plus the statement that allows other principals to subscribe to this topic
  #
  # default permissions copied from https://www.terraform.io/docs/providers/aws/r/sns_topic_policy.html
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
      module.registered_bag_notifications_topic.arn,
    ]

    sid = "__default_statement_ID"
  }

  # Allow subscription by other principals
  statement {
    effect = "Allow"

    actions = [
      "sns:Subscribe"
    ]

    resources = [module.registered_bag_notifications_topic.arn]

    principals {
      identifiers = var.bag_register_output_subscribe_principals
      type        = "AWS"
    }
  }
}
