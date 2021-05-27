locals {
  versioner_versions_table_index = "ingestId_index"
}

resource "aws_dynamodb_table" "versioner_versions_table" {
  name      = "${var.namespace}_versioner_versions_table"
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
    name            = local.versioner_versions_table_index
    hash_key        = "ingestId"
    projection_type = "ALL"
  }
}
