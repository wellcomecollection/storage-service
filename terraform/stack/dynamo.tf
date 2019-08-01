module "auditor_lock_table" {
  source = "../modules/lock_table"

  namespace = "${var.namespace}"
  owner     = "auditor"
}

module "replicator_lock_table" {
  source = "../modules/lock_table"

  namespace = "${var.namespace}"
  owner     = "replicator"
}

locals {
  auditor_versions_table_name  = "${aws_dynamodb_table.auditor_versions_table.name}"
  auditor_versions_table_index = "ingestId_index"
}

# TODO: MOve this into the 'critical' part
resource "aws_dynamodb_table" "auditor_versions_table" {
  name      = "${var.namespace}_auditor_versions_table"
  hash_key  = "id"
  range_key = "version"

  billing_mode = "PAY_PER_REQUEST"

  attribute {
    name = "id"
    type = "S"
  }

  attribute {
    name = "version"
    type = "N"
  }

  attribute {
    name = "ingestId"
    type = "S"
  }

  global_secondary_index {
    name            = "${local.auditor_versions_table_index}"
    hash_key        = "ingestId"
    projection_type = "ALL"
  }
}

data "aws_iam_policy_document" "auditor_versions_table_table_readwrite" {
  statement {
    actions = [
      "dynamodb:DeleteItem",
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:Query",
      "dynamodb:UpdateItem",
    ]

    resources = [
      "${aws_dynamodb_table.auditor_versions_table.arn}",
    ]
  }

  statement {
    actions = [
      "dynamodb:Query",
    ]

    resources = [
      "${aws_dynamodb_table.auditor_versions_table.arn}/index/*",
    ]
  }
}
