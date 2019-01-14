locals {
  gsi_name = "${var.namespace}-bag-id-index"
}

resource "aws_dynamodb_table" "ingests" {
  name           = "${var.namespace}-ingests"
  read_capacity  = 1
  write_capacity = 1
  hash_key       = "id"

  billing_mode = "PAY_PER_REQUEST"

  attribute {
    name = "id"
    type = "S"
  }

  attribute {
    name = "bagIdIndex"
    type = "S"
  }

  attribute {
    name = "createdDate"
    type = "S"
  }

  global_secondary_index {
    name            = "${local.gsi_name}"
    hash_key        = "bagIdIndex"
    range_key       = "createdDate"
    projection_type = "INCLUDE"

    non_key_attributes = ["bagIdIndex", "id", "createdDate"]
  }

  lifecycle {
    prevent_destroy = true

    ignore_changes = [
      "read_capacity",
      "write_capacity",
    ]
  }
}
