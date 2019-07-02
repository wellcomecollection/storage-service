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
  name           = "${var.namespace}-bag_id_lookup"
  read_capacity  = 1
  write_capacity = 1
  hash_key       = "bagId"
  range_key      = "createdDate"

  billing_mode = "${var.billing_mode}"

  attribute {
    name = "bagId"
    type = "S"
  }

  attribute {
    name = "createdDate"
    type = "N"
  }

  lifecycle {
    prevent_destroy = true

    ignore_changes = [
      "read_capacity",
      "write_capacity",
    ]
  }
}
