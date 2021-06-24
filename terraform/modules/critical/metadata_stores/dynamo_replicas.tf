resource "aws_dynamodb_table" "replicas_table" {
  name     = "${var.namespace}_replicas_table"
  hash_key = "id"

  billing_mode = "PAY_PER_REQUEST"

  attribute {
    name = "id"
    type = "S"
  }
}
