module "versioner_lock_table" {
  source = "../lock_table"

  namespace = var.namespace
  owner     = "versioner"
}

module "replicator_lock_table" {
  source = "../lock_table"

  namespace = var.namespace
  owner     = "replicator"
}

resource "aws_dynamodb_table" "azure_verifier_tags" {
  name     = "${var.namespace}_azure_verifier_tags"
  hash_key = "id"

  billing_mode = "PAY_PER_REQUEST"

  attribute {
    name = "id"
    type = "S"
  }
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

