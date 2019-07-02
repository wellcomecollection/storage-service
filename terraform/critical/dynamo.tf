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

  lifecycle {
    prevent_destroy = true

    ignore_changes = [
      "read_capacity",
      "write_capacity",
    ]
  }
}

resource "aws_dynamodb_table" "bag_id_lookup" {
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

  lifecycle {
    prevent_destroy = true

    ignore_changes = [
      "read_capacity",
      "write_capacity",
    ]
  }
}