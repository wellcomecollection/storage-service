module "versioner_lock_table" {
  source = "../modules/lock_table"

  namespace = "${var.namespace}"
  owner     = "versioner"
}

module "replicator_lock_table" {
  source = "../modules/lock_table"

  namespace = "${var.namespace}"
  owner     = "replicator"
}

# Versions

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
      "${var.versioner_versions_table_arn}",
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
