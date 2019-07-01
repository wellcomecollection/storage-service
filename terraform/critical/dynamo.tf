locals {
  gsi_name = "${var.namespace}-bag-id-index"
}

resource "aws_dynamodb_table" "ingests" {
  name           = "${var.namespace}-ingests"
  read_capacity  = 1
  write_capacity = 1
  hash_key       = "id"

  billing_mode = "${var.billing_mode}"

  attribute {
    name = "id"
    type = "S"
  }

  attribute {
    name = "externalIdentifier"
    type = "S"
  }

  attribute {
    name = "space"
    type = "S"
  }

  global_secondary_index {
    name            = "${local.gsi_name}"
    hash_key        = "externalIdentifier"
    range_key       = "space"
    projection_type = "ALL"
  }

  lifecycle {
    prevent_destroy = true

    ignore_changes = [
      "read_capacity",
      "write_capacity",
    ]
  }
}
