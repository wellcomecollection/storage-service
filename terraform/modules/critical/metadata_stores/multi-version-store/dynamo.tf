resource "aws_dynamodb_table" "table" {
  name             = var.table_name
  hash_key         = "id"
  range_key        = "version"

  billing_mode = "PAY_PER_REQUEST"

  attribute {
    name = "id"
    type = "S"
  }

  attribute {
    name = "version"
    type = "N"
  }
}
