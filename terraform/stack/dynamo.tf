locals {
  replicator_lock_table_index = "context-ids-index"
}

resource "aws_dynamodb_table" "replicator_lock_table" {
  name     = "${var.namespace}_replicator_lock_table"
  hash_key = "id"

  billing_mode = "PAY_PER_REQUEST"

  attribute {
    name = "id"
    type = "S"
  }

  attribute {
    name = "contextId"
    type = "S"
  }

  global_secondary_index {
    name            = "${local.replicator_lock_table_index}"
    hash_key        = "contextId"
    projection_type = "ALL"
  }

  ttl {
    attribute_name = "expires"
    enabled        = true
  }
}

data "aws_iam_policy_document" "lock_table_readwrite" {
  statement {
    actions = [
      "dynamodb:UpdateItem",
      "dynamodb:PutItem",
      "dynamodb:GetItem",
      "dynamodb:DeleteItem",
    ]

    resources = [
      "${aws_dynamodb_table.replicator_lock_table.arn}",
    ]
  }

  statement {
    actions = [
      "dynamodb:Query",
    ]

    resources = [
      "${aws_dynamodb_table.replicator_lock_table.arn}/index/*",
    ]
  }
}
