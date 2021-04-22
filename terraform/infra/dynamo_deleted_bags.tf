resource "aws_dynamodb_table" "deleted_bags" {
  name           = "deleted_bags"
  read_capacity  = 1
  write_capacity = 1
  hash_key       = "ingest_id"

  billing_mode = "PAY_PER_REQUEST"

  attribute {
    name = "ingest_id"
    type = "S"
  }

  lifecycle {
    prevent_destroy = true

    ignore_changes = [
      read_capacity,
      write_capacity,
    ]
  }
}
