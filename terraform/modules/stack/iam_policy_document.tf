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
      module.working_storage.unpacked_bags_bucket_arn,
    ]
  }

  statement {
    actions = [
      "s3:GetObject*",
    ]

    resources = [
      "${module.working_storage.unpacked_bags_bucket_arn}/*",
    ]
  }
}

data "aws_iam_policy_document" "unpacked_bags_bucket_put_tags" {
  statement {
    actions = [
      "s3:PutObjectTagging",
    ]

    resources = [
      "${module.working_storage.unpacked_bags_bucket_arn}/*",
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
      "${module.working_storage.unpacked_bags_bucket_arn}/*",
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

data "aws_iam_policy_document" "upload_buckets_readonly" {
  statement {
    actions = [
      "s3:ListBucket",
    ]

    resources = var.upload_bucket_arns
  }

  statement {
    actions = [
      "s3:GetObject*",
    ]

    resources = [
      for arn in var.upload_bucket_arns : "${arn}/*"
    ]
  }
}

data "aws_iam_policy_document" "s3_large_response_cache" {
  statement {
    actions = [
      "s3:*",
    ]

    resources = [
      module.working_storage.large_response_cache_bucket_arn,
      "${module.working_storage.large_response_cache_bucket_arn}/*",
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

    resources = module.working_storage.azure_verifier_cache_table_arn
  }
}

# This policy document allows services to subscribe to the bag register
# output topic.
#
# In particular, they can be notified when a new bag is stored in
# the storage service.
#
# This includes supporting cross-account access to storage service
# notifications, e.g. the METS adapter in the catalogue pipeline that
# feeds our unified collections search.
#
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

  # Allow subscription by users in other accounts.
  #
  # The other account will need a corresponding permission that says which
  # IAM entities in that account can subscribe to that topic.
  #
  # We give blanket permission to everything in that account, because it's
  # easier than specifying individual roles here.
  #
  statement {
    effect = "Allow"

    actions = [
      "sns:Subscribe"
    ]

    resources = [module.registered_bag_notifications_topic.arn]

    principals {
      identifiers = [
        for account_id in var.allow_cross_account_subscription_to_bag_register_output_from:
        "arn:aws:iam::${account_id}:root"
      ]

      type        = "AWS"
    }
  }
}

data "aws_iam_policy_document" "versioner_versions_table_table_readwrite" {
  statement {
    actions = [
      "dynamodb:DeleteItem",
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:Query",
      "dynamodb:UpdateItem",
    ]

    resources = [
      var.versioner_versions_table_arn,
    ]
  }

  statement {
    actions = [
      "dynamodb:Query",
    ]

    resources = [
      "${var.versioner_versions_table_arn}/index/*",
    ]
  }
}
